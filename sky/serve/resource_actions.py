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
import hashlib
import ipaddress
import json
import posixpath
import re
import types
from typing import Any, ClassVar, TypeVar
import unicodedata
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
_AUTHORITY_COHORT_ID_RE = re.compile(
    r'^ra:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-'
    r'[0-9a-f]{12}):([0-9a-f]{64}):'
    r'([a-z0-9](?:[a-z0-9-]{0,40}[a-z0-9])?)$')
_MAX_POSTGRES_BIGINT = 2**63 - 1
_MAX_POSTGRES_INTEGER = 2**31 - 1
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')
_DECIMAL_INTEGER_RE = re.compile(r'^(?:0|[1-9][0-9]{0,18})$')
_DECIMAL_PORT_RE = re.compile(r'^[1-9][0-9]{0,4}$')
_USER_HASH_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9-]*$')
_CANONICAL_POSITIVE_DECIMAL_RE = re.compile(
    r'^(?:[1-9][0-9]*|(?:0|[1-9][0-9]*)\.[0-9]{0,2}[1-9])$')
_MAX_CANONICAL_JSON_CONTAINER_DEPTH = 16
_MAX_CANONICAL_JSON_AGGREGATE_MEMBERS = 4_096
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


def _closed_object_shallow(value: Any, *, name: str,
                           keys: frozenset[str]) -> Mapping[str, Any]:
    """Validate only the outer shape of one closed object.

    Callers with bounded canonical-JSON leaves use this before the generic
    recursive serializer so adversarial cycles and depth overflow reach the
    iterative leaf validator first.
    """

    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    return value


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> JsonObject:
    shallow = _closed_object_shallow(value, name=name, keys=keys)
    encoded = canonical_json_bytes(shallow)
    if len(encoded) > _MAX_OBJECT_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_OBJECT_BYTES} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if shallow != normalized:
        raise ValueError(f'{name} is not canonical.')
    return normalized


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


def _decimal_integer_text(value: Any, *, name: str) -> str:
    """Validate canonical nonnegative decimal text in signed-int64 bounds."""

    decimal_text = _text(value, name=name)
    if (_DECIMAL_INTEGER_RE.fullmatch(decimal_text) is None or
            int(decimal_text) > _MAX_POSTGRES_BIGINT):
        raise ValueError(f'{name} must be canonical decimal integer text in '
                         f'0..{_MAX_POSTGRES_BIGINT}.')
    return decimal_text


def _canonical_positive_decimal_text(value: Any, *, name: str) -> str:
    """Validate one exact positive decimal in the provider numeric domain."""

    decimal_text = _text(value, name=name)
    if _CANONICAL_POSITIVE_DECIMAL_RE.fullmatch(decimal_text) is None:
        raise ValueError(f'{name} must be canonical positive decimal text.')
    integer_part, separator, _ = decimal_text.partition('.')
    maximum_text = str(_MAX_POSTGRES_BIGINT)
    if (len(integer_part) > len(maximum_text) or
        (len(integer_part) == len(maximum_text) and integer_part > maximum_text)
            or (integer_part == maximum_text and separator)):
        raise ValueError(f'{name} must be no greater than '
                         f'{_MAX_POSTGRES_BIGINT}.')
    return decimal_text


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _lower_hex_32_bytes(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(
            f'{name} must be exactly 64 lowercase hexadecimal characters.')
    return value


def _sha256_digest(value: Any, *, name: str) -> str:
    if (type(value) is not str or _SHA256_DIGEST_RE.fullmatch(value) is None):
        raise ValueError(f'{name} must be sha256:<64 lowercase hex>.')
    return value


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
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


def _authority_cohort_id(value: Any, *, name: str) -> str:
    """Validate one installation/release-scoped immutable cohort key."""

    normalized = _text(value, name=name, maximum_bytes=_MAX_TEXT_BYTES)
    match = _AUTHORITY_COHORT_ID_RE.fullmatch(normalized)
    if match is None:
        raise ValueError(
            f'{name} must be ra:<UUID>:<64 lowercase hex>:<DNS label max42>.')
    installation_id, release_digest, cohort_suffix = match.groups()
    _uuid(installation_id, name=f'{name}.installation_id')
    _sha256(release_digest, name=f'{name}.release_digest')
    if len(cohort_suffix) > 42:
        raise ValueError(f'{name} cohort suffix must be at most 42 characters.')
    _dns_label(cohort_suffix, name=f'{name}.cohort_suffix')
    return normalized


def _authority_cohort_id_parts(value: str) -> tuple[uuid.UUID, str, str]:
    normalized = _authority_cohort_id(value, name='authority cohort ID')
    _, installation_id, release_digest, cohort_suffix = normalized.split(':')
    return uuid.UUID(installation_id), release_digest, cohort_suffix


def _dns_subdomain(value: Any, *, name: str) -> str:
    """Validate one canonical Kubernetes DNS-subdomain value."""

    normalized = _text(value, name=name, maximum_bytes=_MAX_SHORT_TEXT_BYTES)
    segments = normalized.split('.')
    if (not normalized.isascii() or any(
            _DNS_LABEL_RE.fullmatch(segment) is None for segment in segments)):
        raise ValueError(f'{name} must be a canonical Kubernetes DNS '
                         'subdomain.')
    return normalized


def _canonical_ip_text(value: Any, *, name: str) -> str:
    """Validate zone-free canonical IPv4 or IPv6 text."""

    normalized = _text(value, name=name, maximum_bytes=_MAX_SHORT_TEXT_BYTES)
    if not normalized.isascii() or '%' in normalized:
        raise ValueError(f'{name} must be canonical zone-free IP text.')
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError as e:
        raise ValueError(f'{name} must be IPv4 or IPv6 text.') from e
    if str(address) != normalized:
        raise ValueError(f'{name} must use canonical IP spelling.')
    return normalized


def _nonnegative_integer(value: Any,
                         *,
                         name: str,
                         maximum: int = _MAX_POSTGRES_BIGINT) -> int:
    if type(value) is not int or value < 0 or value > maximum:
        raise ValueError(
            f'{name} must be a nonnegative integer no greater than {maximum}.')
    return value


def _positive_integer(value: Any,
                      *,
                      name: str,
                      maximum: int = _MAX_POSTGRES_BIGINT) -> int:
    if type(value) is not int or value <= 0 or value > maximum:
        raise ValueError(
            f'{name} must be a positive integer no greater than {maximum}.')
    return value


def _version_one(value: Any, *, name: str) -> int:
    if type(value) is not int or value != 1:
        raise ValueError(f'{name} must be integer 1.')
    return value


def _optional_nonnegative_integer(value: Any, *, name: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_integer(value, name=name)


def _boolean(value: Any, *, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f'{name} must be a Boolean.')
    return value


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


def _action_kind(value: Any, *, name: str) -> kernel_actions.ActionKind:
    if type(value) is kernel_actions.ActionKind:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        return kernel_actions.ActionKind(value)
    except ValueError as e:
        raise ValueError(f'{name} is unsupported.') from e


def _closed_action_kind_object(value: Any, *, name: str, keys: frozenset[str],
                               action_kind_name: str) -> JsonObject:
    """Validate one raw JSON action-kind leaf before canonical reparsing."""

    shallow = _closed_object_shallow(value, name=name, keys=keys)
    raw_action_kind = shallow['action_kind']
    if type(raw_action_kind) is not str:
        raise TypeError(f'{action_kind_name} must be text.')
    _action_kind(raw_action_kind, name=action_kind_name)
    return _closed_object(value, name=name, keys=keys)


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


def _canonical_json_text(value: str, *, name: str) -> None:
    """Validate one string or object key in the bounded canonical domain."""

    try:
        size = len(value.encode('utf-8'))
    except UnicodeEncodeError as e:
        raise ValueError(f'{name} must be valid UTF-8 text.') from e
    if size == 0 or size > _MAX_TEXT_BYTES:
        raise ValueError(f'{name} must be 1..{_MAX_TEXT_BYTES} UTF-8 bytes.')
    if unicodedata.normalize('NFC', value) != value:
        raise ValueError(f'{name} must be NFC-normalized.')


def _bounded_canonical_json_bytes(value: Any,
                                  *,
                                  name: str,
                                  require_object: bool = False,
                                  allow_empty_strings: bool = False) -> bytes:
    """Iteratively validate and encode one bounded canonical JSON value."""

    if require_object and type(value) is not dict:
        raise TypeError(f'{name} must have a JSON object root.')

    # Each item contains the source value plus its destination in a detached
    # built-in graph. Active source-container IDs are removed by exit markers,
    # which rejects cycles while permitting a non-cyclic value to be referenced
    # from more than one sibling position.
    detached_root: list[Any] = [None]
    stack: list[tuple[bool, Any, int, Any,
                      Any]] = [(True, value, 0, detached_root, 0)]
    active_container_ids: set[int] = set()
    aggregate_members = 0
    while stack:
        entering, item, parent_depth, destination, destination_key = stack.pop()
        if not entering:
            active_container_ids.remove(id(item))
            continue
        item_type = type(item)
        if item is None or item_type is bool:
            destination[destination_key] = item
            continue
        if item_type is int:
            if item < -_MAX_POSTGRES_BIGINT - 1 or item > _MAX_POSTGRES_BIGINT:
                raise ValueError(f'{name} integers must fit signed 64-bit.')
            destination[destination_key] = item
            continue
        if item_type is float:
            raise TypeError(f'{name} forbids floating-point values.')
        if item_type is str:
            if item or not allow_empty_strings:
                _canonical_json_text(item, name=f'{name} string')
            destination[destination_key] = item
            continue
        if item_type is tuple:
            raise TypeError(f'{name} JSON arrays must be lists, not tuples.')

        if item_type is not list and item_type is not dict:
            raise TypeError(f'{name} contains a value outside the JSON domain.')

        container_depth = parent_depth + 1
        if container_depth > _MAX_CANONICAL_JSON_CONTAINER_DEPTH:
            raise ValueError(f'{name} container depth exceeds '
                             f'{_MAX_CANONICAL_JSON_CONTAINER_DEPTH}.')
        container_id = id(item)
        if container_id in active_container_ids:
            raise ValueError(f'{name} contains a reference cycle.')

        member_count: int
        child_tasks: list[tuple[Any, Any, Any]]
        if item_type is list:
            member_count = len(item)
            if member_count > _MAX_LIST_ITEMS:
                raise ValueError(f'{name} containers may contain at most '
                                 f'{_MAX_LIST_ITEMS} members.')
            children = item[:_MAX_LIST_ITEMS + 1]
            member_count = len(children)
            if member_count > _MAX_LIST_ITEMS:
                raise ValueError(f'{name} containers may contain at most '
                                 f'{_MAX_LIST_ITEMS} members.')
            detached_container: Any = [None] * member_count
            destination[destination_key] = detached_container
            child_tasks = [(child, detached_container, index)
                           for index, child in enumerate(children)]
        else:
            member_count = len(item)
            if member_count > _MAX_LIST_ITEMS:
                raise ValueError(f'{name} containers may contain at most '
                                 f'{_MAX_LIST_ITEMS} members.')
            entries: list[tuple[Any, Any]] = []
            try:
                for key, child in item.items():
                    if len(entries) == _MAX_LIST_ITEMS:
                        raise ValueError(
                            f'{name} containers may contain at most '
                            f'{_MAX_LIST_ITEMS} members.')
                    entries.append((key, child))
            except RuntimeError as e:
                raise ValueError(f'{name} changed during validation.') from e
            member_count = len(entries)
            normalized_keys: set[str] = set()
            detached_container = {}
            destination[destination_key] = detached_container
            child_tasks = []
            for key, child in entries:
                if type(key) is not str:
                    raise TypeError(f'{name} object keys must be text.')
                normalized_key = unicodedata.normalize('NFC', key)
                if normalized_key in normalized_keys:
                    raise ValueError(f'{name} has duplicate-after-NFC keys.')
                normalized_keys.add(normalized_key)
                _canonical_json_text(key, name=f'{name} object key')
                child_tasks.append((child, detached_container, key))
        aggregate_members += member_count
        if aggregate_members > _MAX_CANONICAL_JSON_AGGREGATE_MEMBERS:
            raise ValueError(f'{name} aggregate container members exceed '
                             f'{_MAX_CANONICAL_JSON_AGGREGATE_MEMBERS}.')
        active_container_ids.add(container_id)
        stack.append((False, item, parent_depth, None, None))
        stack.extend((True, child, container_depth, child_destination,
                      child_destination_key) for child, child_destination,
                     child_destination_key in reversed(child_tasks))

    encoded = canonical_json_bytes(detached_root[0])
    if len(encoded) > _MAX_OBJECT_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_OBJECT_BYTES} bytes.')
    return encoded


@dataclasses.dataclass(frozen=True, init=False)
class CanonicalJsonValue:
    """Immutable bytes for one value in the bounded canonical JSON domain."""

    _canonical_bytes: bytes = dataclasses.field(repr=False)

    def __init__(self, value: Any) -> None:
        object.__setattr__(
            self, '_canonical_bytes',
            _bounded_canonical_json_bytes(value, name='canonical JSON value'))

    @classmethod
    def from_value(cls, value: Any) -> CanonicalJsonValue:
        return cls(value)

    def canonical_value(self) -> Any:
        """Return a detached JSON value that cannot mutate the stored bytes."""

        return json.loads(self._canonical_bytes.decode('utf-8'))

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.canonical_value())


@dataclasses.dataclass(frozen=True, init=False)
class CanonicalJsonObject(CanonicalJsonValue):
    """Immutable bounded canonical JSON value with an object root."""

    def __init__(self, value: Any) -> None:
        if type(value) is not dict:
            raise TypeError('canonical JSON object must have a JSON object '
                            'root.')
        super().__init__(value)

    @classmethod
    def from_value(cls, value: Any) -> CanonicalJsonObject:
        return cls(value)

    def canonical_value(self) -> JsonObject:
        value = super().canonical_value()
        if type(value) is not dict:
            raise ValueError('canonical JSON object lost its object root.')
        return value


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
    def from_value(cls, value: Any) -> _ProviderKubernetesServerOriginV1:
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
        if type(self.server_origin) is not _ProviderKubernetesServerOriginV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesTransportIdentityV1:
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
        if type(self.transport) is not ProviderKubernetesTransportIdentityV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesScopeV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesScopeReadV1:
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
    MAX_FROZEN_USER_HASH_LENGTH: ClassVar[int] = (_MAX_LENGTH -
                                                  _CLUSTER_NAME_HASH_LENGTH - 3)

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
                  maximum_bytes=self.MAX_FROZEN_USER_HASH_LENGTH))
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
    def from_value(cls, value: Any) -> ProviderWorkloadNameBasisV1:
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
        if type(self.repo_path) is not str:
            raise TypeError('artifact.repo_path must be text.')
        if type(self.byte_size) is not int:
            raise TypeError('artifact.byte_size must be an integer.')
        if type(self.sha256) is not str:
            raise TypeError('artifact.sha256 must be text.')
        repo_path = _text(self.repo_path, name='artifact.repo_path')
        if (not repo_path.isascii() or repo_path.startswith('/') or
                repo_path.endswith('/') or '\\' in repo_path or
                posixpath.normpath(repo_path) != repo_path or
                any(component in ('', '.', '..')
                    for component in repo_path.split('/'))):
            raise ValueError('artifact.repo_path must be a normalized relative '
                             'POSIX path.')
        object.__setattr__(self, 'repo_path', repo_path)
        object.__setattr__(
            self, 'byte_size',
            _positive_integer(self.byte_size, name='artifact.byte_size'))
        object.__setattr__(self, 'sha256',
                           _sha256(self.sha256, name='artifact.sha256'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderRepoArtifactRefV1:
        raw = _closed_object_shallow(value,
                                     name='artifact reference',
                                     keys=cls._KEYS)
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
class ProviderProjectedQualificationArtifactRefV1:
    """Chart-packaged post-build qualification evidence reference."""

    source: str
    repo_path: str
    mount_path: str
    byte_size: int
    sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'source', 'repo_path', 'mount_path', 'byte_size', 'sha256'})
    _REPO_PREFIX: ClassVar[
        str] = 'charts/skypilot/files/resource-action-qualifications/'
    _MOUNT_PATH: ClassVar[
        str] = '/etc/skypilot/resource-action-authority/qualification.json'

    def __post_init__(self) -> None:
        if self.source != 'helm_chart_configmap_v1':
            raise ValueError('qualification artifact source is unsupported.')
        repo_path = _text(self.repo_path,
                          name='qualification_artifact.repo_path')
        if (not repo_path.isascii() or
                not repo_path.startswith(self._REPO_PREFIX) or
                repo_path == self._REPO_PREFIX or repo_path.endswith('/') or
                '\\' in repo_path or
                posixpath.normpath(repo_path) != repo_path or
                any(component in ('', '.', '..')
                    for component in repo_path.split('/'))):
            raise ValueError('qualification artifact repository path is '
                             'unsupported.')
        object.__setattr__(self, 'repo_path', repo_path)
        if self.mount_path != self._MOUNT_PATH:
            raise ValueError('qualification artifact mount path is '
                             'unsupported.')
        object.__setattr__(
            self, 'byte_size',
            _positive_integer(self.byte_size,
                              name='qualification_artifact.byte_size'))
        object.__setattr__(
            self, 'sha256',
            _sha256(self.sha256, name='qualification_artifact.sha256'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderProjectedQualificationArtifactRefV1:
        raw = _closed_object(value,
                             name='projected qualification artifact reference',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)

    @property
    def canonical_bytes(self) -> bytes:
        encoded = canonical_json_bytes(self.canonical_value())
        if len(encoded) > _MAX_OBJECT_BYTES:
            raise ValueError('Projected qualification artifact reference '
                             'exceeds 65536 bytes.')
        return encoded


@dataclasses.dataclass(frozen=True)
class ProviderOCIImageQualificationV1(_CanonicalContract):
    """Digest-qualified immutable OCI image identity."""

    requested_reference: str
    oci_manifest_digest: str
    oci_config_digest: str
    qualification_artifact: ProviderProjectedQualificationArtifactRefV1

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
        if type(self.qualification_artifact
               ) is not ProviderProjectedQualificationArtifactRefV1:
            raise TypeError('image qualification artifact has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderOCIImageQualificationV1:
        raw = _closed_object(value,
                             name='OCI image qualification',
                             keys=cls._KEYS)
        return cls(requested_reference=raw['requested_reference'],
                   oci_manifest_digest=raw['oci_manifest_digest'],
                   oci_config_digest=raw['oci_config_digest'],
                   qualification_artifact=(
                       ProviderProjectedQualificationArtifactRefV1.from_value(
                           raw['qualification_artifact'])))

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
        {'containerd', 'cri-o', 'docker-pullable', 'oci-reference'})

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
        if scheme == 'oci-reference':
            raw_body = raw_image_id
        else:
            prefix = f'{scheme}://'
            if not raw_image_id.startswith(prefix):
                raise ValueError(
                    'raw runtime image ID does not match its declared scheme.')
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
        if scheme in ('containerd', 'cri-o'):
            if self.runtime_id_contract != 'qualified_oci_config_digest_v1':
                raise ValueError('runtime image config-ID contract is '
                                 'unsupported.')
            expected_digest = self.qualified_oci_config_digest
        else:
            if self.runtime_id_contract != 'qualified_oci_manifest_digest_v1':
                raise ValueError('runtime image manifest-ID contract is '
                                 'unsupported.')
            expected_digest = self.qualified_oci_manifest_digest
        if self.runtime_image_id_digest != expected_digest:
            if scheme in ('docker-pullable', 'oci-reference'):
                raise ValueError('runtime image ID must equal the qualified '
                                 'OCI manifest digest.')
            raise ValueError('runtime image ID must equal the qualified OCI '
                             'config digest.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderRuntimeImageIdentityV1:
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
        if type(self.qualification) is not ProviderOCIImageQualificationV1:
            raise TypeError('worker image qualification has an invalid type.')
        if type(self.runtime) is not ProviderRuntimeImageIdentityV1:
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
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerImageV1:
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


class CanonicalKubernetesResourceRequirementsV1(CanonicalJsonObject):
    """Canonical Kubernetes container resource requirements."""


class CanonicalKubernetesPodSecurityContextV1(CanonicalJsonObject):
    """Canonical Kubernetes Pod security context."""


class CanonicalKubernetesContainerSecurityContextV1(CanonicalJsonObject):
    """Canonical Kubernetes container security context."""


class CanonicalKubernetesAffinityV1(CanonicalJsonObject):
    """Canonical Kubernetes affinity object."""


class CanonicalKubernetesTolerationV1(CanonicalJsonObject):
    """Canonical Kubernetes toleration object."""


class CanonicalKubernetesTopologySpreadConstraintV1(CanonicalJsonObject):
    """Canonical Kubernetes topology-spread constraint."""


def _environment_name(value: Any, *, name: str) -> str:
    parsed = _text(value, name=name, maximum_bytes=_MAX_SHORT_TEXT_BYTES)
    if (not parsed.isascii() or
            re.fullmatch(r'[A-Z_][A-Z0-9_]*', parsed) is None):
        raise ValueError(f'{name} must be a canonical environment name.')
    return parsed


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerConfigMapFileV1(_CanonicalContract):
    """One immutable ConfigMap key mounted as an individual file."""

    name: str
    key: str
    mount_path: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'name', 'key', 'mount_path'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'name',
                           _dns_subdomain(self.name, name='config_map.name'))
        object.__setattr__(self, 'key', _text(self.key, name='config_map.key'))
        object.__setattr__(self, 'mount_path',
                           _text(self.mount_path, name='config_map.mount_path'))

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerConfigMapFileV1:
        return cls(**_closed_object(
            value, name='authority ConfigMap file', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerAuthSecretV1(_CanonicalContract):
    """Purpose bearer-token Secret projection."""

    name: str
    key: str
    mount_path: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'name', 'key', 'mount_path'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'name',
                           _dns_subdomain(self.name, name='auth_secret.name'))
        object.__setattr__(self, 'key', _text(self.key, name='auth_secret.key'))
        if self.mount_path != (
                '/etc/skypilot/resource-action-authority/auth/tokens'):
            raise ValueError('authority auth Secret mount path is unsupported.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerAuthSecretV1:
        return cls(**_closed_object(
            value, name='authority auth Secret', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerTLSSecretV1(_CanonicalContract):
    """Purpose TLS Secret projection for one authority cohort."""

    name: str
    cert_key: str
    private_key_key: str
    ca_key: str
    cert_path: str
    private_key_path: str
    ca_path: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'name', 'cert_key', 'private_key_key', 'ca_key', 'cert_path',
        'private_key_path', 'ca_path'
    })

    def __post_init__(self) -> None:
        object.__setattr__(self, 'name',
                           _dns_subdomain(self.name, name='tls_secret.name'))
        for field in ('cert_key', 'private_key_key', 'ca_key'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field), name=f'tls_secret.{field}'))
        expected = {
            'cert_path': '/etc/skypilot/resource-action-authority/tls/tls.crt',
            'private_key_path': '/etc/skypilot/resource-action-authority/tls/tls.key',
            'ca_path': '/etc/skypilot/resource-action-authority/tls/ca.crt',
        }
        if any(
                getattr(self, field) != value
                for field, value in expected.items()):
            raise ValueError('authority TLS Secret paths are unsupported.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerTLSSecretV1:
        return cls(**_closed_object(
            value, name='authority TLS Secret', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerDatabaseSecretV1(_CanonicalContract):
    """PostgreSQL connection Secret key used by the frozen Pod template."""

    name: str
    key: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'name', 'key'})

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'name', _dns_subdomain(self.name,
                                         name='database_secret.name'))
        object.__setattr__(self, 'key',
                           _text(self.key, name='database_secret.key'))

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerDatabaseSecretV1:
        return cls(**_closed_object(
            value, name='authority database Secret', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerDownwardAPIFieldV1(_CanonicalContract):
    """One exact downward-API environment binding."""

    env: str
    field_path: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'env', 'field_path'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'env',
                           _environment_name(self.env, name='downward.env'))
        object.__setattr__(self, 'field_path',
                           _text(self.field_path, name='downward.field_path'))

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerDownwardAPIFieldV1:
        return cls(**_closed_object(
            value, name='authority downward API field', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerLiteralEnvV1(_CanonicalContract):
    """One literal environment value in the frozen Pod template."""

    name: str
    value: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'name', 'value'})

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'name', _environment_name(self.name, name='literal_env.name'))
        object.__setattr__(self, 'value',
                           _text(self.value, name='literal_env.value'))

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerLiteralEnvV1:
        return cls(**_closed_object(
            value, name='authority literal environment', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerSecretEnvV1(_CanonicalContract):
    """One Secret-key environment value in the frozen Pod template."""

    name: str
    secret_name: str
    key: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'name', 'secret_name', 'key'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'name',
                           _environment_name(self.name, name='secret_env.name'))
        object.__setattr__(
            self, 'secret_name',
            _dns_subdomain(self.secret_name, name='secret_env.secret_name'))
        object.__setattr__(self, 'key', _text(self.key, name='secret_env.key'))

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerSecretEnvV1:
        return cls(**_closed_object(
            value, name='authority Secret environment', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerPodTemplateReleaseInputsV1(_CanonicalContract):
    """Complete Helm-varying preimage for one authority Pod template."""

    version: int
    namespace: str
    helm_full_name: str
    cohort_suffix: str
    cohort_id: str
    deployment_name: str
    service_account_name: str
    container_name: str
    image: str
    image_pull_policy: str
    command: tuple[str, ...]
    args: tuple[str, ...]
    health_port: str
    preflight_port: str
    manifest_config_map: ProviderAuthorityWorkerConfigMapFileV1
    qualification_config_map: ProviderAuthorityWorkerConfigMapFileV1
    auth_secret: ProviderAuthorityWorkerAuthSecretV1
    tls_secret: ProviderAuthorityWorkerTLSSecretV1
    database_secret: ProviderAuthorityWorkerDatabaseSecretV1
    downward_api_fields: tuple[ProviderAuthorityWorkerDownwardAPIFieldV1, ...]
    literal_env: tuple[ProviderAuthorityWorkerLiteralEnvV1, ...]
    secret_env: tuple[ProviderAuthorityWorkerSecretEnvV1, ...]
    resources: CanonicalKubernetesResourceRequirementsV1
    image_pull_secrets: tuple[str, ...]
    pod_labels: tuple[ProviderLabelV1, ...]
    pod_annotations_without_manifest_hash: tuple[ProviderAnnotationV1, ...]
    pod_security_context: CanonicalKubernetesPodSecurityContextV1
    container_security_context: CanonicalKubernetesContainerSecurityContextV1
    node_selector: tuple[ProviderLabelV1, ...]
    affinity: CanonicalKubernetesAffinityV1 | None
    tolerations: tuple[CanonicalKubernetesTolerationV1, ...]
    topology_spread_constraints: tuple[
        CanonicalKubernetesTopologySpreadConstraintV1, ...]
    priority_class_name: str | None
    runtime_class_name: str | None
    scheduler_name: str | None
    termination_grace_period_seconds: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'namespace', 'helm_full_name', 'cohort_suffix', 'cohort_id',
        'deployment_name', 'service_account_name', 'container_name', 'image',
        'image_pull_policy', 'command', 'args', 'health_port', 'preflight_port',
        'manifest_config_map', 'qualification_config_map', 'auth_secret',
        'tls_secret', 'database_secret', 'downward_api_fields', 'literal_env',
        'secret_env', 'resources', 'image_pull_secrets', 'pod_labels',
        'pod_annotations_without_manifest_hash', 'pod_security_context',
        'container_security_context', 'node_selector', 'affinity',
        'tolerations', 'topology_spread_constraints', 'priority_class_name',
        'runtime_class_name', 'scheduler_name',
        'termination_grace_period_seconds'
    })
    _DOWNWARD_FIELDS: ClassVar[tuple[tuple[str, str], ...]] = (
        ('SKYPILOT_POD_NAME', 'metadata.name'),
        ('SKYPILOT_POD_NAMESPACE', 'metadata.namespace'),
        ('SKYPILOT_POD_UID', 'metadata.uid'),
    )
    _AUTH_TOKENS_PATH: ClassVar[
        str] = '/etc/skypilot/resource-action-authority/auth/tokens'

    def __post_init__(self) -> None:
        _version_one(self.version, name='Pod template release-input version')
        namespace = _dns_subdomain(self.namespace,
                                   name='release_inputs.namespace')
        full_name = _dns_label(self.helm_full_name,
                               name='release_inputs.helm_full_name')
        suffix = _dns_label(self.cohort_suffix,
                            name='release_inputs.cohort_suffix')
        if len(suffix) > 42:
            raise ValueError('release_inputs.cohort_suffix exceeds 42 bytes.')
        cohort_id = _authority_cohort_id(self.cohort_id,
                                         name='release_inputs.cohort_id')
        _, release_digest, cohort_id_suffix = _authority_cohort_id_parts(
            cohort_id)
        expected_digest = hashlib.sha256(
            f'{namespace}\n{full_name}\n{suffix}'.encode()).hexdigest()
        if release_digest != expected_digest or cohort_id_suffix != suffix:
            raise ValueError('release-input cohort ID does not bind its '
                             'namespace/full name/suffix.')
        object.__setattr__(self, 'namespace', namespace)
        object.__setattr__(self, 'helm_full_name', full_name)
        object.__setattr__(self, 'cohort_suffix', suffix)
        object.__setattr__(self, 'cohort_id', cohort_id)
        expected_name = f'{full_name}-authority-{suffix}'
        for field in ('deployment_name', 'service_account_name'):
            parsed = _dns_label(getattr(self, field),
                                name=f'release_inputs.{field}')
            if parsed != expected_name:
                raise ValueError(f'release_inputs.{field} is not derived from '
                                 'the Helm full name and cohort suffix.')
            object.__setattr__(self, field, parsed)
        if self.container_name != 'skypilot-authority-worker':
            raise ValueError('authority container name is unsupported.')
        image = _text(self.image, name='release_inputs.image')
        try:
            canonical_image = container_image_models.validate_oci_reference(
                image, 'release_inputs.image')
            _, digest = container_image_models.split_digest(canonical_image)
        except (TypeError, ValueError) as e:
            raise ValueError('release-input image is not a canonical OCI '
                             'reference.') from e
        if canonical_image != image or digest is None:
            raise ValueError('release-input image must be digest-pinned.')
        object.__setattr__(self, 'image', image)
        if self.image_pull_policy != 'Always':
            raise ValueError('authority image pull policy must be Always.')
        for field in ('command', 'args'):
            values = getattr(self, field)
            if (type(values) is not tuple or not values or
                    len(values) > _MAX_LIST_ITEMS):
                raise ValueError(f'release_inputs.{field} must be a nonempty '
                                 'bounded tuple.')
            object.__setattr__(
                self, field,
                tuple(
                    _text(value, name=f'release_inputs.{field}')
                    for value in values))
        health = _decimal_port_text(self.health_port,
                                    name='release_inputs.health_port')
        preflight = _decimal_port_text(self.preflight_port,
                                       name='release_inputs.preflight_port')
        if health == preflight:
            raise ValueError(
                'authority health and preflight ports must differ.')
        object.__setattr__(self, 'health_port', health)
        object.__setattr__(self, 'preflight_port', preflight)
        expected_types = (
            ('manifest_config_map', ProviderAuthorityWorkerConfigMapFileV1),
            ('qualification_config_map',
             ProviderAuthorityWorkerConfigMapFileV1),
            ('auth_secret', ProviderAuthorityWorkerAuthSecretV1),
            ('tls_secret', ProviderAuthorityWorkerTLSSecretV1),
            ('database_secret', ProviderAuthorityWorkerDatabaseSecretV1),
            ('resources', CanonicalKubernetesResourceRequirementsV1),
            ('pod_security_context', CanonicalKubernetesPodSecurityContextV1),
            ('container_security_context',
             CanonicalKubernetesContainerSecurityContextV1),
        )
        for field, expected_type in expected_types:
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'release_inputs.{field} has an invalid type.')
        if (self.manifest_config_map.key != 'manifest.json' or
                self.manifest_config_map.mount_path
                != '/etc/skypilot/resource-action-authority/manifest.json'):
            raise ValueError('manifest ConfigMap projection is unsupported.')
        if (self.qualification_config_map.key != 'qualification.json' or
                self.qualification_config_map.mount_path !=
                '/etc/skypilot/resource-action-authority/qualification.json'):
            raise ValueError('qualification ConfigMap projection is '
                             'unsupported.')
        downward = tuple(
            (item.env, item.field_path) for item in self.downward_api_fields)
        if (type(self.downward_api_fields) is not tuple or any(
                type(item) is not ProviderAuthorityWorkerDownwardAPIFieldV1
                for item in self.downward_api_fields) or
                downward != self._DOWNWARD_FIELDS):
            raise ValueError('authority downward API fields are not the exact '
                             'ordered v1 inventory.')
        for field, expected_type in (
            ('literal_env', ProviderAuthorityWorkerLiteralEnvV1),
            ('secret_env', ProviderAuthorityWorkerSecretEnvV1),
        ):
            values = getattr(self, field)
            if (type(values) is not tuple or len(values) > _MAX_LIST_ITEMS or
                    any(type(item) is not expected_type for item in values)):
                raise ValueError(f'release_inputs.{field} has invalid entries.')
            names = tuple(item.name for item in values)
            if names != tuple(sorted(set(names))):
                raise ValueError(f'release_inputs.{field} must be sorted by '
                                 'unique name.')
        expected_literal_env = (
            ('SKYPILOT_API_REQUEST_BACKEND', 'postgres'),
            ('SKYPILOT_API_SERVER_ROLE', 'authority-worker'),
            ('SKYPILOT_RELEASE_NAME', full_name),
            ('SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE',
             self._AUTH_TOKENS_PATH),
            ('SKYPILOT_STATE_DB_MIGRATION_MODE', 'verify'),
        )
        if tuple((item.name, item.value)
                 for item in self.literal_env) != expected_literal_env:
            raise ValueError('authority literal environment is not the exact '
                             'ordered v1 inventory.')
        if (len(self.secret_env) != 1 or
                self.secret_env[0].name != 'SKYPILOT_DB_CONNECTION_URI' or
                self.secret_env[0].secret_name != self.database_secret.name or
                self.secret_env[0].key != self.database_secret.key):
            raise ValueError('database Secret environment does not match its '
                             'exact declared v1 inventory.')
        if (type(self.image_pull_secrets) is not tuple or
                len(self.image_pull_secrets) > _MAX_LIST_ITEMS):
            raise ValueError('image_pull_secrets must be a bounded tuple.')
        parsed_pull_secrets = tuple(
            _dns_subdomain(value, name='release_inputs.image_pull_secret')
            for value in self.image_pull_secrets)
        if len(set(parsed_pull_secrets)) != len(parsed_pull_secrets):
            raise ValueError('image_pull_secrets must be unique.')
        object.__setattr__(self, 'image_pull_secrets', parsed_pull_secrets)
        for field, expected_type in (
            ('pod_labels', ProviderLabelV1),
            ('pod_annotations_without_manifest_hash', ProviderAnnotationV1),
            ('node_selector', ProviderLabelV1),
        ):
            values = getattr(self, field)
            if (type(values) is not tuple or len(values) > _MAX_LIST_ITEMS or
                    any(type(item) is not expected_type for item in values)):
                raise ValueError(f'release_inputs.{field} has invalid entries.')
            keys = tuple(item.key for item in values)
            if keys != tuple(sorted(set(keys))):
                raise ValueError(f'release_inputs.{field} must be sorted by '
                                 'unique key.')
        if any(item.key == 'skypilot.co/resource-action-manifest-sha256'
               for item in self.pod_annotations_without_manifest_hash):
            raise ValueError('manifest hash annotation must be excluded from '
                             'release inputs.')
        if self.affinity is not None and type(
                self.affinity) is not CanonicalKubernetesAffinityV1:
            raise TypeError('release_inputs.affinity has an invalid type.')
        for field, expected_type in (
            ('tolerations', CanonicalKubernetesTolerationV1),
            ('topology_spread_constraints',
             CanonicalKubernetesTopologySpreadConstraintV1),
        ):
            values = getattr(self, field)
            if (type(values) is not tuple or len(values) > _MAX_LIST_ITEMS or
                    any(type(item) is not expected_type for item in values)):
                raise ValueError(f'release_inputs.{field} has invalid entries.')
        fixed_toleration_keys = frozenset({
            'node.kubernetes.io/not-ready',
            'node.kubernetes.io/unreachable',
        })
        for toleration in self.tolerations:
            value = toleration.canonical_value()
            if (value.get('key') in fixed_toleration_keys and
                    value.get('effect') in (None, '', 'NoExecute')):
                raise ValueError(
                    'release_inputs.tolerations collides with a fixed '
                    'authority admission-default toleration.')
        for field in ('priority_class_name', 'runtime_class_name',
                      'scheduler_name'):
            object.__setattr__(
                self, field,
                _optional_text(getattr(self, field),
                               name=f'release_inputs.{field}'))
        object.__setattr__(
            self, 'termination_grace_period_seconds',
            _positive_integer(
                self.termination_grace_period_seconds,
                name='release_inputs.termination_grace_period_seconds'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderAuthorityWorkerPodTemplateReleaseInputsV1:
        raw = _closed_object(value,
                             name='authority Pod-template release inputs',
                             keys=cls._KEYS)

        def _tuple(field: str, reader: Any) -> tuple[Any, ...]:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'release_inputs.{field} must be a list.')
            return tuple(reader(item) for item in items)

        affinity = raw['affinity']
        return cls(
            version=raw['version'],
            namespace=raw['namespace'],
            helm_full_name=raw['helm_full_name'],
            cohort_suffix=raw['cohort_suffix'],
            cohort_id=raw['cohort_id'],
            deployment_name=raw['deployment_name'],
            service_account_name=raw['service_account_name'],
            container_name=raw['container_name'],
            image=raw['image'],
            image_pull_policy=raw['image_pull_policy'],
            command=_tuple('command', lambda item: item),
            args=_tuple('args', lambda item: item),
            health_port=raw['health_port'],
            preflight_port=raw['preflight_port'],
            manifest_config_map=ProviderAuthorityWorkerConfigMapFileV1.
            from_value(raw['manifest_config_map']),
            qualification_config_map=(
                ProviderAuthorityWorkerConfigMapFileV1.from_value(
                    raw['qualification_config_map'])),
            auth_secret=ProviderAuthorityWorkerAuthSecretV1.from_value(
                raw['auth_secret']),
            tls_secret=ProviderAuthorityWorkerTLSSecretV1.from_value(
                raw['tls_secret']),
            database_secret=ProviderAuthorityWorkerDatabaseSecretV1.from_value(
                raw['database_secret']),
            downward_api_fields=_tuple(
                'downward_api_fields',
                ProviderAuthorityWorkerDownwardAPIFieldV1.from_value),
            literal_env=_tuple('literal_env',
                               ProviderAuthorityWorkerLiteralEnvV1.from_value),
            secret_env=_tuple('secret_env',
                              ProviderAuthorityWorkerSecretEnvV1.from_value),
            resources=CanonicalKubernetesResourceRequirementsV1(
                raw['resources']),
            image_pull_secrets=_tuple('image_pull_secrets', lambda item: item),
            pod_labels=_tuple('pod_labels', ProviderLabelV1.from_value),
            pod_annotations_without_manifest_hash=_tuple(
                'pod_annotations_without_manifest_hash',
                ProviderAnnotationV1.from_value),
            pod_security_context=(CanonicalKubernetesPodSecurityContextV1(
                raw['pod_security_context'])),
            container_security_context=(
                CanonicalKubernetesContainerSecurityContextV1(
                    raw['container_security_context'])),
            node_selector=_tuple('node_selector', ProviderLabelV1.from_value),
            affinity=(None if affinity is None else
                      CanonicalKubernetesAffinityV1(affinity)),
            tolerations=_tuple('tolerations',
                               CanonicalKubernetesTolerationV1.from_value),
            topology_spread_constraints=_tuple(
                'topology_spread_constraints',
                CanonicalKubernetesTopologySpreadConstraintV1.from_value),
            priority_class_name=raw['priority_class_name'],
            runtime_class_name=raw['runtime_class_name'],
            scheduler_name=raw['scheduler_name'],
            termination_grace_period_seconds=raw[
                'termination_grace_period_seconds'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'namespace': self.namespace,
            'helm_full_name': self.helm_full_name,
            'cohort_suffix': self.cohort_suffix,
            'cohort_id': self.cohort_id,
            'deployment_name': self.deployment_name,
            'service_account_name': self.service_account_name,
            'container_name': 'skypilot-authority-worker',
            'image': self.image,
            'image_pull_policy': 'Always',
            'command': list(self.command),
            'args': list(self.args),
            'health_port': self.health_port,
            'preflight_port': self.preflight_port,
            'manifest_config_map': self.manifest_config_map.canonical_value(),
            'qualification_config_map':
                self.qualification_config_map.canonical_value(),
            'auth_secret': self.auth_secret.canonical_value(),
            'tls_secret': self.tls_secret.canonical_value(),
            'database_secret': self.database_secret.canonical_value(),
            'downward_api_fields': [
                item.canonical_value() for item in self.downward_api_fields
            ],
            'literal_env': [
                item.canonical_value() for item in self.literal_env
            ],
            'secret_env': [item.canonical_value() for item in self.secret_env],
            'resources': self.resources.canonical_value(),
            'image_pull_secrets': list(self.image_pull_secrets),
            'pod_labels': [item.canonical_value() for item in self.pod_labels],
            'pod_annotations_without_manifest_hash': [
                item.canonical_value()
                for item in self.pod_annotations_without_manifest_hash
            ],
            'pod_security_context': self.pod_security_context.canonical_value(),
            'container_security_context':
                self.container_security_context.canonical_value(),
            'node_selector': [
                item.canonical_value() for item in self.node_selector
            ],
            'affinity': (None if self.affinity is None else
                         self.affinity.canonical_value()),
            'tolerations': [
                item.canonical_value() for item in self.tolerations
            ],
            'topology_spread_constraints': [
                item.canonical_value()
                for item in self.topology_spread_constraints
            ],
            'priority_class_name': self.priority_class_name,
            'runtime_class_name': self.runtime_class_name,
            'scheduler_name': self.scheduler_name,
            'termination_grace_period_seconds':
                self.termination_grace_period_seconds,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerPodTemplateBindingV1(_CanonicalContract):
    """Hash binding from release inputs to the exact PodTemplateSpec."""

    version: int
    contract: str
    projector_artifact_sha256: str
    release_inputs: ProviderAuthorityWorkerPodTemplateReleaseInputsV1
    expected_template_sha256: str
    manifest_hash_annotation_json_pointer: str
    manifest_hash_placeholder: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'contract', 'projector_artifact_sha256', 'release_inputs',
        'expected_template_sha256', 'manifest_hash_annotation_json_pointer',
        'manifest_hash_placeholder'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='Pod-template binding version')
        if self.contract != 'authority_worker_pod_template_v1':
            raise ValueError('Pod-template binding contract is unsupported.')
        object.__setattr__(
            self, 'projector_artifact_sha256',
            _sha256(self.projector_artifact_sha256,
                    name='pod_template_binding.projector_artifact_sha256'))
        if type(self.release_inputs
               ) is not ProviderAuthorityWorkerPodTemplateReleaseInputsV1:
            raise TypeError('Pod-template release inputs have an invalid type.')
        object.__setattr__(
            self, 'expected_template_sha256',
            _sha256(self.expected_template_sha256,
                    name='pod_template_binding.expected_template_sha256'))
        if self.manifest_hash_annotation_json_pointer != (
                '/metadata/annotations/'
                'skypilot.co~1resource-action-manifest-sha256'):
            raise ValueError('manifest hash annotation pointer is unsupported.')
        if self.manifest_hash_placeholder != '$MANIFEST_SHA256':
            raise ValueError('manifest hash placeholder is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerPodTemplateBindingV1:
        raw = _closed_object(value,
                             name='authority Pod-template binding',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            projector_artifact_sha256=raw['projector_artifact_sha256'],
            release_inputs=(
                ProviderAuthorityWorkerPodTemplateReleaseInputsV1.from_value(
                    raw['release_inputs'])),
            expected_template_sha256=raw['expected_template_sha256'],
            manifest_hash_annotation_json_pointer=raw[
                'manifest_hash_annotation_json_pointer'],
            manifest_hash_placeholder=raw['manifest_hash_placeholder'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': 'authority_worker_pod_template_v1',
            'projector_artifact_sha256': self.projector_artifact_sha256,
            'release_inputs': self.release_inputs.canonical_value(),
            'expected_template_sha256': self.expected_template_sha256,
            'manifest_hash_annotation_json_pointer':
                self.manifest_hash_annotation_json_pointer,
            'manifest_hash_placeholder': '$MANIFEST_SHA256',
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
    pod_template_binding: ProviderAuthorityWorkerPodTemplateBindingV1
    artifact_inventory: ProviderRepoArtifactRefV1
    callable_inventory: ProviderRepoArtifactRefV1
    claim_contract: str
    handler_allowlist: tuple[str, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cohort_id', 'namespace', 'deployment_name',
        'service_account_name', 'container_name', 'image',
        'pod_template_contract', 'pod_template_binding', 'artifact_inventory',
        'callable_inventory', 'claim_contract', 'handler_allowlist'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='cohort manifest version')
        cohort_id = _authority_cohort_id(self.cohort_id,
                                         name='cohort_manifest.cohort_id')
        object.__setattr__(self, 'cohort_id', cohort_id)
        for field in ('namespace', 'deployment_name', 'service_account_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'cohort_manifest.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if self.container_name != 'skypilot-authority-worker':
            raise ValueError('cohort manifest container name is unsupported.')
        if type(self.image) is not ProviderOCIImageQualificationV1:
            raise TypeError('cohort manifest image has an invalid type.')
        for field in ('pod_template_contract', 'artifact_inventory',
                      'callable_inventory'):
            if type(getattr(self, field)) is not ProviderRepoArtifactRefV1:
                raise TypeError(f'cohort manifest {field} has an invalid type.')
        if type(self.pod_template_binding
               ) is not ProviderAuthorityWorkerPodTemplateBindingV1:
            raise TypeError('cohort manifest Pod-template binding has an '
                            'invalid type.')
        release_inputs = self.pod_template_binding.release_inputs
        if (release_inputs.cohort_id != cohort_id or
                release_inputs.namespace != self.namespace or
                release_inputs.deployment_name != self.deployment_name or
                release_inputs.service_account_name != self.service_account_name
                or release_inputs.container_name != self.container_name or
                release_inputs.image != self.image.requested_reference):
            raise ValueError('cohort manifest and Pod-template release inputs '
                             'are not byte-bound.')
        if (self.pod_template_binding.projector_artifact_sha256
                != self.pod_template_contract.sha256):
            raise ValueError('Pod-template binding does not name the manifest '
                             'projector artifact.')
        if self.claim_contract != 'frozen_action_cohort_join_v1':
            raise ValueError('cohort manifest claim contract is unsupported.')
        if type(self.handler_allowlist) is not tuple:
            raise TypeError('cohort manifest handler allowlist must be a '
                            'tuple.')
        if self.handler_allowlist != (
                PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1):
            raise ValueError('cohort manifest handler allowlist must be the '
                             'ordered v1 allowlist.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerCohortManifestV1:
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
                   pod_template_binding=(
                       ProviderAuthorityWorkerPodTemplateBindingV1.from_value(
                           raw['pod_template_binding'])),
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
            'pod_template_binding': self.pod_template_binding.canonical_value(),
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
        if type(self.manifest) is not ProviderAuthorityWorkerCohortManifestV1:
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
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerCohortV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesControllerOwnerV1:
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
        if type(self.pod_controller_owner
               ) is not ProviderKubernetesControllerOwnerV1:
            raise TypeError('worker Pod controller owner has an invalid type.')
        if type(self.replica_set_controller_owner
               ) is not ProviderKubernetesControllerOwnerV1:
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
        if type(self.image) is not ProviderAuthorityWorkerImageV1:
            raise TypeError('worker image has an invalid type.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='worker.observed_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerIdentityV1:
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
    deployment_status_replicas: int
    deployment_updated_replicas: int
    deployment_ready_replicas: int
    deployment_available_replicas: int
    deployment_unavailable_replicas: int
    registered_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'worker', 'pod_ready', 'deployment_spec_replicas',
        'deployment_status_observed_generation', 'deployment_status_replicas',
        'deployment_updated_replicas', 'deployment_ready_replicas',
        'deployment_available_replicas', 'deployment_unavailable_replicas',
        'registered_at'
    })

    def __post_init__(self) -> None:
        if not isinstance(self.worker, ProviderAuthorityWorkerIdentityV1):
            raise TypeError('worker registration identity has an invalid type.')
        _boolean(self.pod_ready, name='registration.pod_ready')
        if not self.pod_ready:
            raise ValueError('worker registration requires a ready Pod.')
        for field in ('deployment_spec_replicas', 'deployment_status_replicas',
                      'deployment_updated_replicas',
                      'deployment_ready_replicas',
                      'deployment_available_replicas'):
            if _positive_integer(getattr(self, field),
                                 name=f'registration.{field}') != 2:
                raise ValueError(f'registration.{field} must equal 2.')
        if _nonnegative_integer(
                self.deployment_unavailable_replicas,
                name='registration.deployment_unavailable_replicas') != 0:
            raise ValueError(
                'registration.deployment_unavailable_replicas must equal 0.')
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
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerRegistrationV1:
        raw = _closed_object(value,
                             name='authority-worker registration',
                             keys=cls._KEYS)
        return cls(
            worker=ProviderAuthorityWorkerIdentityV1.from_value(raw['worker']),
            pod_ready=raw['pod_ready'],
            deployment_spec_replicas=raw['deployment_spec_replicas'],
            deployment_status_observed_generation=raw[
                'deployment_status_observed_generation'],
            deployment_status_replicas=raw['deployment_status_replicas'],
            deployment_updated_replicas=raw['deployment_updated_replicas'],
            deployment_ready_replicas=raw['deployment_ready_replicas'],
            deployment_available_replicas=raw['deployment_available_replicas'],
            deployment_unavailable_replicas=raw[
                'deployment_unavailable_replicas'],
            registered_at=raw['registered_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'worker': self.worker.canonical_value(),
            'pod_ready': True,
            'deployment_spec_replicas': 2,
            'deployment_status_observed_generation':
                self.deployment_status_observed_generation,
            'deployment_status_replicas': 2,
            'deployment_updated_replicas': 2,
            'deployment_ready_replicas': 2,
            'deployment_available_replicas': 2,
            'deployment_unavailable_replicas': 0,
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
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerRegistrationSetV1:
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
    def from_value(cls, value: Any) -> ProviderResourceIdentityV1:
        raw = _closed_object_shallow(value,
                                     name='resource_identity',
                                     keys=cls._KEYS)
        _text(raw['service_hash'], name='resource_identity.service_hash')
        _uuid(raw['service_incarnation'],
              name='resource_identity.service_incarnation')
        _nonnegative_integer(raw['replica_id'],
                             name='resource_identity.replica_id')
        _uuid(raw['replica_incarnation'],
              name='resource_identity.replica_incarnation')
        _positive_integer(raw['desired_generation'],
                          name='resource_identity.desired_generation')
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
        self, action_kind: kernel_actions.ActionKind | str
    ) -> kernel_actions.ResourceActionIdentity:
        try:
            parsed_action_kind = _action_kind(action_kind, name='action_kind')
        except (TypeError, ValueError) as e:
            raise ValueError('action_kind must be launch or down.') from e
        return kernel_actions.ResourceActionIdentity(
            service_hash=self.service_hash,
            service_incarnation=self.service_incarnation,
            replica_id=self.replica_id,
            replica_incarnation=self.replica_incarnation,
            desired_generation=self.desired_generation,
            action_kind=parsed_action_kind)


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
    def from_value(cls, value: Any) -> CoverageDecisionIdentityV1:
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
    def from_value(cls, value: Any) -> CoverageDecisionV1:
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
    preparation_capability_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'decision_id', 'cohort_id', 'service_hash',
        'replica_incarnation', 'desired_generation', 'action_type',
        'controller_owner_fence', 'lifecycle_epoch',
        'preparation_capability_sha256'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='worker cohort reference version')
        object.__setattr__(
            self, 'decision_id',
            _uuid(self.decision_id, name='cohort_reference.decision_id'))
        object.__setattr__(
            self, 'cohort_id',
            _authority_cohort_id(self.cohort_id,
                                 name='cohort_reference.cohort_id'))
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
        object.__setattr__(
            self, 'preparation_capability_sha256',
            _sha256(self.preparation_capability_sha256,
                    name=('cohort_reference.'
                          'preparation_capability_sha256')))

    @classmethod
    def from_value(cls, value: Any) -> WorkerCohortReferenceInputV1:
        shallow = _closed_object_shallow(value,
                                         name='worker cohort reference input',
                                         keys=cls._KEYS)
        _sha256(shallow['preparation_capability_sha256'],
                name=('cohort_reference.'
                      'preparation_capability_sha256'))
        raw = _closed_object(shallow,
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
            'preparation_capability_sha256': self.preparation_capability_sha256,
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
class ProviderLaunchIdentityCanonicalizationInputV1(_CanonicalContract):
    """Pre-auth launch identity inputs sent to the private canonicalizer."""

    version: int
    contract: str
    service_name: str
    resource_identity: ProviderResourceIdentityV1
    prepared_original_user: str
    prepared_user_hash: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'contract', 'service_name', 'resource_identity',
        'prepared_original_user', 'prepared_user_hash'
    })
    _CONTRACT: ClassVar[str] = 'api_server_effective_launch_identity_v1'

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='launch identity canonicalization input version')
        if type(self.contract) is not str:
            raise TypeError(
                'launch identity canonicalization input contract must be text.')
        if self.contract != self._CONTRACT:
            raise ValueError(
                'launch identity canonicalization input contract is unsupported.'
            )
        object.__setattr__(
            self, 'service_name',
            _text(self.service_name,
                  name='launch identity canonicalization input service_name',
                  maximum_bytes=_MAX_SERVICE_NAME_BYTES))
        if type(self.resource_identity) is not ProviderResourceIdentityV1:
            raise TypeError('launch identity canonicalization input resource '
                            'identity has an invalid type.')
        for field in ('prepared_original_user', 'prepared_user_hash'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'launch identity canonicalization input {field}'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderLaunchIdentityCanonicalizationInputV1:
        raw = _closed_object_shallow(
            value,
            name='launch identity canonicalization input',
            keys=cls._KEYS)
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   service_name=raw['service_name'],
                   resource_identity=ProviderResourceIdentityV1.from_value(
                       raw['resource_identity']),
                   prepared_original_user=raw['prepared_original_user'],
                   prepared_user_hash=raw['prepared_user_hash'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': self._CONTRACT,
            'service_name': self.service_name,
            'resource_identity': self.resource_identity.canonical_value(),
            'prepared_original_user': self.prepared_original_user,
            'prepared_user_hash': self.prepared_user_hash,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchIdentityCanonicalizationContextV1(_CanonicalContract):
    """Exact PREPARING reference context for launch identity resolution."""

    version: int
    decision_id: uuid.UUID
    cohort_id: str
    action_type: kernel_actions.ActionKind
    controller_owner_fence: str
    lifecycle_epoch: int
    preparation_reference_revision: int
    reference_state: WorkerCohortReferenceState
    preparation_capability_sha256: str
    input: ProviderLaunchIdentityCanonicalizationInputV1
    input_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'decision_id', 'cohort_id', 'action_type',
        'controller_owner_fence', 'lifecycle_epoch',
        'preparation_reference_revision', 'reference_state',
        'preparation_capability_sha256', 'input', 'input_sha256'
    })

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='launch identity canonicalization context version')
        decision_id = _uuid(
            self.decision_id,
            name='launch identity canonicalization context decision_id')
        object.__setattr__(self, 'decision_id', decision_id)
        object.__setattr__(
            self, 'cohort_id',
            _authority_cohort_id(
                self.cohort_id,
                name='launch identity canonicalization context cohort_id'))
        action_type = _action_kind(
            self.action_type,
            name='launch identity canonicalization context action_type')
        if action_type is not kernel_actions.ActionKind.LAUNCH:
            raise ValueError(
                'launch identity canonicalization context requires launch.')
        object.__setattr__(self, 'action_type', action_type)
        object.__setattr__(
            self, 'controller_owner_fence',
            _text(self.controller_owner_fence,
                  name=('launch identity canonicalization context '
                        'controller_owner_fence')))
        object.__setattr__(
            self, 'lifecycle_epoch',
            _positive_integer(
                self.lifecycle_epoch,
                name='launch identity canonicalization context lifecycle_epoch')
        )
        _version_one(self.preparation_reference_revision,
                     name=('launch identity canonicalization context '
                           'preparation_reference_revision'))
        reference_state = _enum_value(
            WorkerCohortReferenceState,
            self.reference_state,
            name='launch identity canonicalization context reference_state')
        if reference_state is not WorkerCohortReferenceState.PREPARING:
            raise ValueError(
                'launch identity canonicalization context requires '
                'a PREPARING reference.')
        object.__setattr__(self, 'reference_state', reference_state)
        object.__setattr__(
            self, 'preparation_capability_sha256',
            _sha256(self.preparation_capability_sha256,
                    name=('launch identity canonicalization context '
                          'preparation_capability_sha256')))
        if type(self.input
               ) is not ProviderLaunchIdentityCanonicalizationInputV1:
            raise TypeError(
                'launch identity canonicalization context input has '
                'an invalid type.')
        object.__setattr__(
            self, 'input_sha256',
            _sha256(
                self.input_sha256,
                name='launch identity canonicalization context input_sha256'))
        if self.input_sha256 != self.input.sha256:
            raise ValueError('launch identity canonicalization context input '
                             'hash does not match.')
        expected_decision_id = self.input.resource_identity.action_identity(
            kernel_actions.ActionKind.LAUNCH).action_id
        if decision_id != expected_decision_id:
            raise ValueError(
                'launch identity canonicalization context decision '
                'ID does not match its resource identity.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls, value: Any) -> ProviderLaunchIdentityCanonicalizationContextV1:
        raw = _closed_object_shallow(
            value,
            name='launch identity canonicalization context',
            keys=cls._KEYS)
        return cls(
            version=raw['version'],
            decision_id=raw['decision_id'],
            cohort_id=raw['cohort_id'],
            action_type=raw['action_type'],
            controller_owner_fence=raw['controller_owner_fence'],
            lifecycle_epoch=raw['lifecycle_epoch'],
            preparation_reference_revision=raw[
                'preparation_reference_revision'],
            reference_state=raw['reference_state'],
            preparation_capability_sha256=raw['preparation_capability_sha256'],
            input=ProviderLaunchIdentityCanonicalizationInputV1.from_value(
                raw['input']),
            input_sha256=raw['input_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'decision_id': str(self.decision_id),
            'cohort_id': self.cohort_id,
            'action_type': kernel_actions.ActionKind.LAUNCH.value,
            'controller_owner_fence': self.controller_owner_fence,
            'lifecycle_epoch': self.lifecycle_epoch,
            'preparation_reference_revision': 1,
            'reference_state': WorkerCohortReferenceState.PREPARING.value,
            'preparation_capability_sha256': self.preparation_capability_sha256,
            'input': self.input.canonical_value(),
            'input_sha256': self.input_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchIdentityCanonicalizationRequestV1(_CanonicalContract):
    """Closed no-enqueue launch identity canonicalization request."""

    version: int
    context: ProviderLaunchIdentityCanonicalizationContextV1
    context_sha256: str
    preparation_capability: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'context', 'context_sha256', 'preparation_capability'})

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='launch identity canonicalization request version')
        if type(self.context
               ) is not ProviderLaunchIdentityCanonicalizationContextV1:
            raise TypeError('launch identity canonicalization request context '
                            'has an invalid type.')
        object.__setattr__(
            self, 'context_sha256',
            _sha256(
                self.context_sha256,
                name='launch identity canonicalization request context_sha256'))
        if self.context_sha256 != self.context.sha256:
            raise ValueError('launch identity canonicalization request context '
                             'hash does not match.')
        capability = _lower_hex_32_bytes(
            self.preparation_capability,
            name=('launch identity canonicalization request '
                  'preparation_capability'))
        object.__setattr__(self, 'preparation_capability', capability)
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls, value: Any) -> ProviderLaunchIdentityCanonicalizationRequestV1:
        raw = _closed_object_shallow(
            value,
            name='launch identity canonicalization request',
            keys=cls._KEYS)
        return cls(
            version=raw['version'],
            context=ProviderLaunchIdentityCanonicalizationContextV1.from_value(
                raw['context']),
            context_sha256=raw['context_sha256'],
            preparation_capability=raw['preparation_capability'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'context': self.context.canonical_value(),
            'context_sha256': self.context_sha256,
            'preparation_capability': self.preparation_capability,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchIdentityCanonicalizationProofV1(_CanonicalContract):
    """Retained API-side effective identity proof without the capability."""

    version: int
    boundary: str
    context: ProviderLaunchIdentityCanonicalizationContextV1
    context_sha256: str
    effective_original_user: str
    effective_user_hash: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'boundary', 'context', 'context_sha256',
        'effective_original_user', 'effective_user_hash'
    })
    _BOUNDARY: ClassVar[str] = 'api_server_post_auth_no_enqueue'

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='launch identity canonicalization proof version')
        if type(self.boundary) is not str:
            raise TypeError(
                'launch identity canonicalization proof boundary must be text.')
        if self.boundary != self._BOUNDARY:
            raise ValueError(
                'launch identity canonicalization proof boundary is unsupported.'
            )
        if type(self.context
               ) is not ProviderLaunchIdentityCanonicalizationContextV1:
            raise TypeError(
                'launch identity canonicalization proof context has '
                'an invalid type.')
        object.__setattr__(
            self, 'context_sha256',
            _sha256(
                self.context_sha256,
                name='launch identity canonicalization proof context_sha256'))
        if self.context_sha256 != self.context.sha256:
            raise ValueError('launch identity canonicalization proof context '
                             'hash does not match.')
        effective_original_user = _text(
            self.effective_original_user,
            name=('launch identity canonicalization proof '
                  'effective_original_user'))
        if not effective_original_user.isascii():
            raise ValueError('launch identity canonicalization proof effective '
                             'username must be ASCII.')
        object.__setattr__(self, 'effective_original_user',
                           effective_original_user)
        effective_user_hash = _text(
            self.effective_user_hash,
            name='launch identity canonicalization proof effective_user_hash',
            maximum_bytes=(
                ProviderWorkloadNameBasisV1.MAX_FROZEN_USER_HASH_LENGTH))
        # ``common_utils.is_valid_user_hash()`` intentionally preserves a
        # broad legacy ``re.match(...$)`` contract.  This durable proof must
        # consume the complete value so a trailing newline cannot survive into
        # a name basis that correctly rejects it later.
        if _USER_HASH_RE.fullmatch(effective_user_hash) is None:
            raise ValueError('launch identity canonicalization proof effective '
                             'user hash is invalid.')
        object.__setattr__(self, 'effective_user_hash', effective_user_hash)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderLaunchIdentityCanonicalizationProofV1:
        raw = _closed_object_shallow(
            value,
            name='launch identity canonicalization proof',
            keys=cls._KEYS)
        return cls(
            version=raw['version'],
            boundary=raw['boundary'],
            context=ProviderLaunchIdentityCanonicalizationContextV1.from_value(
                raw['context']),
            context_sha256=raw['context_sha256'],
            effective_original_user=raw['effective_original_user'],
            effective_user_hash=raw['effective_user_hash'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'boundary': self._BOUNDARY,
            'context': self.context.canonical_value(),
            'context_sha256': self.context_sha256,
            'effective_original_user': self.effective_original_user,
            'effective_user_hash': self.effective_user_hash,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchIdentityCanonicalizationResponseV1(_CanonicalContract):
    """Closed response echoing the exact decision, context, and proof hashes."""

    version: int
    decision_id: uuid.UUID
    context_sha256: str
    proof: ProviderLaunchIdentityCanonicalizationProofV1
    proof_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'decision_id', 'context_sha256', 'proof', 'proof_sha256'})

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='launch identity canonicalization response version')
        decision_id = _uuid(
            self.decision_id,
            name='launch identity canonicalization response decision_id')
        object.__setattr__(self, 'decision_id', decision_id)
        object.__setattr__(
            self, 'context_sha256',
            _sha256(
                self.context_sha256,
                name='launch identity canonicalization response context_sha256')
        )
        if type(self.proof
               ) is not ProviderLaunchIdentityCanonicalizationProofV1:
            raise TypeError(
                'launch identity canonicalization response proof has '
                'an invalid type.')
        object.__setattr__(
            self, 'proof_sha256',
            _sha256(
                self.proof_sha256,
                name='launch identity canonicalization response proof_sha256'))
        if decision_id != self.proof.context.decision_id:
            raise ValueError(
                'launch identity canonicalization response decision '
                'ID does not match its proof.')
        if self.context_sha256 != self.proof.context_sha256:
            raise ValueError(
                'launch identity canonicalization response context '
                'hash does not match its proof.')
        if self.proof_sha256 != self.proof.sha256:
            raise ValueError('launch identity canonicalization response proof '
                             'hash does not match.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderLaunchIdentityCanonicalizationResponseV1:
        raw = _closed_object_shallow(
            value,
            name='launch identity canonicalization response',
            keys=cls._KEYS)
        return cls(
            version=raw['version'],
            decision_id=raw['decision_id'],
            context_sha256=raw['context_sha256'],
            proof=ProviderLaunchIdentityCanonicalizationProofV1.from_value(
                raw['proof']),
            proof_sha256=raw['proof_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'decision_id': str(self.decision_id),
            'context_sha256': self.context_sha256,
            'proof': self.proof.canonical_value(),
            'proof_sha256': self.proof_sha256,
        }


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
    def from_value(cls, value: Any) -> CoverageAttemptV1:
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

    scope: ProviderKubernetesScopeV1
    cluster_fingerprint_sha256: str
    namespace: str
    name_basis: ProviderWorkloadNameBasisV1
    provider_cluster_name: str
    workload_kind: str
    workload_name: str
    cluster_record_uuid_label: str
    replica_incarnation_label: str
    topology: ProviderPodTopologyV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'scope', 'cluster_fingerprint_sha256', 'namespace', 'name_basis',
        'provider_cluster_name', 'workload_kind', 'workload_name',
        'cluster_record_uuid_label', 'replica_incarnation_label', 'topology'
    })

    def __post_init__(self) -> None:
        if type(self.scope) is not ProviderKubernetesScopeV1:
            raise TypeError('kubernetes.scope has an invalid type.')
        if type(self.name_basis) is not ProviderWorkloadNameBasisV1:
            raise TypeError('kubernetes.name_basis has an invalid type.')
        if type(self.topology) is not ProviderPodTopologyV1:
            raise TypeError('kubernetes.topology has an invalid type.')
        object.__setattr__(
            self, 'cluster_fingerprint_sha256',
            _sha256(self.cluster_fingerprint_sha256,
                    name='kubernetes.cluster_fingerprint_sha256'))
        for field in ('namespace', 'provider_cluster_name', 'workload_kind',
                      'workload_name'):
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
        if self.namespace != self.scope.namespace:
            raise ValueError('Kubernetes locator namespace does not match its '
                             'scope.')
        if self.cluster_fingerprint_sha256 != self.scope.sha256:
            raise ValueError('Kubernetes locator cluster fingerprint does not '
                             'match its scope.')
        if self.provider_cluster_name != self.name_basis.provider_cluster_name:
            raise ValueError('Kubernetes locator provider cluster name does '
                             'not match its name basis.')
        if self.workload_kind != 'Pod':
            raise ValueError('Kubernetes locator workload kind must be Pod.')
        if self.workload_name != self.name_basis.workload_name:
            raise ValueError('Kubernetes locator workload name does not match '
                             'its name basis.')
        head_pod = self.topology.mutable_objects[2]
        if (head_pod.name != self.workload_name or
                self.topology.mutable_objects[1].name != self.workload_name or
                self.topology.mutable_objects[0].name
                != f'{self.workload_name}-ssh'):
            raise ValueError('Kubernetes locator topology does not match its '
                             'workload name.')
        topology_labels = {
            label.key: label.value
            for label in self.topology.mutable_objects[2].labels
        }
        if (topology_labels.get('skypilot-cluster-name')
                != self.provider_cluster_name or
                topology_labels.get('skypilot.co/cluster-record-uuid')
                != self.cluster_record_uuid_label or
                topology_labels.get('skypilot.co/serve-replica-incarnation')
                != self.replica_incarnation_label):
            raise ValueError('Kubernetes locator topology identity labels do '
                             'not match the locator.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesLocatorV1:
        _bounded_canonical_json_bytes(value,
                                      name='Kubernetes locator',
                                      require_object=True)
        raw = _closed_object_shallow(value, name='kubernetes', keys=cls._KEYS)
        return cls(scope=ProviderKubernetesScopeV1.from_value(raw['scope']),
                   cluster_fingerprint_sha256=raw['cluster_fingerprint_sha256'],
                   namespace=raw['namespace'],
                   name_basis=ProviderWorkloadNameBasisV1.from_value(
                       raw['name_basis']),
                   provider_cluster_name=raw['provider_cluster_name'],
                   workload_kind=raw['workload_kind'],
                   workload_name=raw['workload_name'],
                   cluster_record_uuid_label=raw['cluster_record_uuid_label'],
                   replica_incarnation_label=raw['replica_incarnation_label'],
                   topology=ProviderPodTopologyV1.from_value(raw['topology']))

    def canonical_value(self) -> JsonObject:
        return {
            'scope': self.scope.canonical_value(),
            'cluster_fingerprint_sha256': self.cluster_fingerprint_sha256,
            'namespace': self.namespace,
            'name_basis': self.name_basis.canonical_value(),
            'provider_cluster_name': self.provider_cluster_name,
            'workload_kind': 'Pod',
            'workload_name': self.workload_name,
            'cluster_record_uuid_label': self.cluster_record_uuid_label,
            'replica_incarnation_label': self.replica_incarnation_label,
            'topology': self.topology.canonical_value(),
        }


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
                type(self.kubernetes) is not ProviderKubernetesLocatorV1):
            raise TypeError('locator.kubernetes has an invalid type.')
        if (self.kubernetes is not None and
                self.kubernetes.cluster_record_uuid_label != str(cluster_uuid)):
            raise ValueError('Kubernetes cluster label does not match the '
                             'cluster-record UUID.')
        if (self.kubernetes is not None and self.sky_cluster_name
                != self.kubernetes.name_basis.display_name):
            raise ValueError('Kubernetes locator display name does not match '
                             'the Sky cluster name.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLocatorV1:
        _bounded_canonical_json_bytes(value,
                                      name='provider locator',
                                      require_object=True)
        raw = _closed_object_shallow(value,
                                     name='provider locator',
                                     keys=cls._KEYS)
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
    """Design-complete API006 resolved target retained after launch.

    This is deliberately the same canonical wire as the API006 provider
    progress target.  Keeping the complete three-object identity here avoids
    projecting successful launch evidence through the older lossy
    provider-resource/workload-only shape before a down is admitted.
    """

    version: int
    requested_target_sha256: str
    provider_resource_id: str | None
    workload_uid: str | None
    kubernetes_objects: tuple[ProviderKubernetesResolvedObjectV1, ...]
    provider_operation_id: str | None
    resolved_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'requested_target_sha256', 'provider_resource_id',
        'workload_uid', 'kubernetes_objects', 'provider_operation_id',
        'resolved_at'
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
        if (type(self.kubernetes_objects) is not tuple or
                len(self.kubernetes_objects)
                != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1) or any(
                    type(item) is not ProviderKubernetesResolvedObjectV1
                    for item in self.kubernetes_objects)):
            raise ValueError('resolved target requires exactly three typed '
                             'Kubernetes objects.')
        expected_roles = tuple(
            entry.role for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
        if tuple(item.role
                 for item in self.kubernetes_objects) != expected_roles:
            raise ValueError('resolved target objects are not in canonical '
                             'role order.')
        if not all(item.has_complete_allocations
                   for item in self.kubernetes_objects):
            raise ValueError('resolved target requires every server '
                             'allocation.')
        pod = self.kubernetes_objects[2]
        if self.workload_uid != pod.uid:
            raise ValueError('resolved target workload UID must equal the '
                             'head Pod UID.')
        object.__setattr__(
            self, 'resolved_at',
            _timestamp(self.resolved_at, name='resolved_target.resolved_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ResolvedProviderTargetV1:
        raw = _closed_object(value, name='resolved target', keys=cls._KEYS)
        objects = raw['kubernetes_objects']
        if type(objects) is not list:
            raise TypeError('resolved target kubernetes_objects must be a '
                            'list.')
        if len(objects) != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1):
            raise ValueError('resolved target requires exactly three '
                             'Kubernetes objects.')
        return cls(version=raw['version'],
                   requested_target_sha256=raw['requested_target_sha256'],
                   provider_resource_id=raw['provider_resource_id'],
                   workload_uid=raw['workload_uid'],
                   kubernetes_objects=tuple(
                       ProviderKubernetesResolvedObjectV1.from_value(item)
                       for item in objects),
                   provider_operation_id=raw['provider_operation_id'],
                   resolved_at=raw['resolved_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'requested_target_sha256': self.requested_target_sha256,
            'provider_resource_id': self.provider_resource_id,
            'workload_uid': self.workload_uid,
            'kubernetes_objects': [
                item.canonical_value() for item in self.kubernetes_objects
            ],
            'provider_operation_id': self.provider_operation_id,
            'resolved_at': self.resolved_at,
        }

    def validate_requested_target(self, target: ProviderLocatorV1) -> None:
        if self.requested_target_sha256 != target.sha256:
            raise ValueError('Resolved target does not match requested target.')


# API006 historically used the noun-first spelling.  Keep one implementation
# and one canonical wire so progress can migrate to this shared DTO without a
# conversion or import cycle.
ProviderResolvedTargetV1 = ResolvedProviderTargetV1


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
    def from_value(cls, value: Any) -> ProviderAcceleratorV1:
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
        if type(self.key) is not str or type(self.value) is not str:
            raise TypeError('label key and value must be text.')
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
    def from_value(cls, value: Any) -> ProviderLabelV1:
        raw = _closed_object_shallow(value, name='label', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


def _provider_label_tuple(value: Any, *,
                          name: str) -> tuple[ProviderLabelV1, ...]:
    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    if (len(value) > _MAX_LIST_ITEMS or
            any(type(label) is not ProviderLabelV1 for label in value)):
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
class _ProviderKubernetesObjectRoleMapEntryV1:
    """One immutable entry in the direct-Pod object-role protocol map."""

    plan_sequence: int
    role: ProviderObjectRoleV1
    kind: ProviderPodTopologyMutableObjectKindV1
    name_rule: str
    create_sequence: int
    delete_sequence: int


PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1 = (
    _ProviderKubernetesObjectRoleMapEntryV1(
        plan_sequence=0,
        role=ProviderObjectRoleV1.HEAD_SSH_SERVICE,
        kind=ProviderPodTopologyMutableObjectKindV1.SERVICE,
        name_rule='workload_name_plus_-ssh',
        create_sequence=0,
        delete_sequence=1),
    _ProviderKubernetesObjectRoleMapEntryV1(
        plan_sequence=1,
        role=ProviderObjectRoleV1.HEAD_SERVICE,
        kind=ProviderPodTopologyMutableObjectKindV1.SERVICE,
        name_rule='workload_name',
        create_sequence=1,
        delete_sequence=0),
    _ProviderKubernetesObjectRoleMapEntryV1(
        plan_sequence=2,
        role=ProviderObjectRoleV1.HEAD_POD,
        kind=ProviderPodTopologyMutableObjectKindV1.POD,
        name_rule='workload_name',
        create_sequence=2,
        delete_sequence=2),
)
_PROVIDER_KUBERNETES_OBJECT_ROLE_BY_SEQUENCE_V1 = types.MappingProxyType({
    entry.plan_sequence: entry
    for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
})


@dataclasses.dataclass(frozen=True)
class ProviderPodTopologyMutableObjectV1(_CanonicalContract):
    """One role-specific mutable object in a direct-Pod topology."""

    kind: ProviderPodTopologyMutableObjectKindV1
    role: ProviderObjectRoleV1
    name: str
    labels: tuple[ProviderLabelV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'kind', 'role', 'name', 'labels'})
    _ROLE_KINDS: ClassVar[Mapping[
        ProviderObjectRoleV1,
        ProviderPodTopologyMutableObjectKindV1]] = types.MappingProxyType({
            entry.role: entry.kind
            for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
        })

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
    def from_value(cls, value: Any) -> ProviderPodTopologyMutableObjectV1:
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
    _EXPECTED_ROLES: ClassVar[tuple[ProviderObjectRoleV1, ...]] = tuple(
        entry.role for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
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
        if (type(self.mutable_objects) is not tuple or
                len(self.mutable_objects) != len(self._EXPECTED_ROLES) or any(
                    type(item) is not ProviderPodTopologyMutableObjectV1
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
    def from_value(cls, value: Any) -> ProviderPodTopologyV1:
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
class ProviderKubernetesServerAllocationV1(_CanonicalContract):
    """One independently validated Kubernetes server allocation value."""

    json_pointer: str
    allocator: str
    value: CanonicalJsonValue

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'json_pointer', 'allocator', 'value'})
    _SERVICE_POINTERS: ClassVar[frozenset[str]] = frozenset({
        '/spec/clusterIP', '/spec/clusterIPs', '/spec/ipFamilies',
        '/spec/ipFamilyPolicy'
    })
    _NODE_NAME_POINTER: ClassVar[str] = '/spec/nodeName'

    def __post_init__(self) -> None:
        pointer = _text(self.json_pointer,
                        name='server_allocation.json_pointer')
        allocator = _text(self.allocator, name='server_allocation.allocator')
        if type(self.value) is not CanonicalJsonValue:
            raise TypeError('server allocation value has an invalid type.')
        raw_value = self.value.canonical_value()
        if pointer in self._SERVICE_POINTERS:
            if allocator != 'api_server':
                raise ValueError('Service allocations require the api_server '
                                 'allocator.')
            self._validate_service_value(pointer, raw_value)
        elif pointer == self._NODE_NAME_POINTER:
            if allocator != 'scheduler':
                raise ValueError('Pod nodeName allocation requires the '
                                 'scheduler allocator.')
            _dns_subdomain(raw_value, name='server_allocation.value.nodeName')
        else:
            raise ValueError('server allocation JSON pointer is unsupported.')
        object.__setattr__(self, 'json_pointer', pointer)
        object.__setattr__(self, 'allocator', allocator)
        _ = self.canonical_bytes

    @staticmethod
    def _validate_service_value(pointer: str, value: Any) -> None:
        if pointer == '/spec/clusterIP':
            if value != 'None':
                _canonical_ip_text(value,
                                   name='server_allocation.value.clusterIP')
            return
        if pointer == '/spec/clusterIPs':
            if not isinstance(value, list) or len(value) != 1:
                raise ValueError('clusterIPs allocation must be a one-element '
                                 'JSON array.')
            if value[0] != 'None':
                _canonical_ip_text(value[0],
                                   name='server_allocation.value.clusterIPs')
            return
        if pointer == '/spec/ipFamilies':
            if (not isinstance(value, list) or len(value) != 1 or
                    value[0] not in ('IPv4', 'IPv6')):
                raise ValueError('ipFamilies allocation must contain exactly '
                                 'IPv4 or IPv6.')
            return
        if value != 'SingleStack':
            raise ValueError('ipFamilyPolicy allocation must be SingleStack.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesServerAllocationV1:
        shallow = _closed_object_shallow(value,
                                         name='server allocation',
                                         keys=cls._KEYS)
        allocation_value = CanonicalJsonValue.from_value(shallow['value'])
        raw = _closed_object(shallow, name='server allocation', keys=cls._KEYS)
        return cls(json_pointer=raw['json_pointer'],
                   allocator=raw['allocator'],
                   value=allocation_value)

    def canonical_value(self) -> JsonObject:
        return {
            'json_pointer': self.json_pointer,
            'allocator': self.allocator,
            'value': self.value.canonical_value(),
        }


_PROVIDER_KUBERNETES_SERVICE_ALLOCATION_POINTERS_V1 = (
    '/spec/clusterIP',
    '/spec/clusterIPs',
    '/spec/ipFamilies',
    '/spec/ipFamilyPolicy',
)
_PROVIDER_KUBERNETES_POD_ALLOCATION_POINTERS_V1 = ('/spec/nodeName',)


def _validate_provider_kubernetes_role_allocations_v1(
    role: ProviderObjectRoleV1,
    allocations: Any,
    *,
    name: str,
    require_pod_node_name: bool = False,
) -> tuple[ProviderKubernetesServerAllocationV1, ...]:
    """Validate one role's atomic, canonically ordered server allocations."""

    if type(allocations) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    if role in (ProviderObjectRoleV1.HEAD_SSH_SERVICE,
                ProviderObjectRoleV1.HEAD_SERVICE):
        if len(allocations) != len(
                _PROVIDER_KUBERNETES_SERVICE_ALLOCATION_POINTERS_V1):
            raise ValueError(f'{name} must contain the complete Service '
                             'allocation quartet in canonical order.')
    elif role is ProviderObjectRoleV1.HEAD_POD:
        allowed_lengths = (1,) if require_pod_node_name else (0, 1)
        if len(allocations) not in allowed_lengths:
            requirement = ('the scheduler nodeName allocation'
                           if require_pod_node_name else
                           'no allocation or the scheduler nodeName allocation')
            raise ValueError(f'{name} Pod must contain {requirement}.')
    else:
        raise ValueError(f'{name} has an unsupported object role.')
    if any(
            type(allocation) is not ProviderKubernetesServerAllocationV1
            for allocation in allocations):
        raise TypeError(f'{name} must contain typed server allocations.')
    pointers = tuple(allocation.json_pointer for allocation in allocations)
    if role in (ProviderObjectRoleV1.HEAD_SSH_SERVICE,
                ProviderObjectRoleV1.HEAD_SERVICE):
        if pointers != _PROVIDER_KUBERNETES_SERVICE_ALLOCATION_POINTERS_V1:
            raise ValueError(f'{name} must contain the complete Service '
                             'allocation quartet in canonical order.')
        cluster_ip = allocations[0].value.canonical_value()
        cluster_ips = allocations[1].value.canonical_value()
        ip_families = allocations[2].value.canonical_value()
        ip_family_policy = allocations[3].value.canonical_value()
        if ip_family_policy != 'SingleStack':
            raise ValueError(f'{name} must use SingleStack.')
        if role is ProviderObjectRoleV1.HEAD_SSH_SERVICE:
            if cluster_ip == 'None':
                raise ValueError(f'{name} SSH Service must have a cluster IP.')
            canonical_ip = _canonical_ip_text(
                cluster_ip, name=f'{name} SSH Service cluster IP')
            expected_family = ('IPv4'
                               if ipaddress.ip_address(canonical_ip).version
                               == 4 else 'IPv6')
            if cluster_ips != [canonical_ip
                              ] or ip_families != [expected_family]:
                raise ValueError(f'{name} SSH Service allocations disagree on '
                                 'IP value or address family.')
        elif (cluster_ip != 'None' or cluster_ips != ['None'] or
              ip_families not in (['IPv4'], ['IPv6'])):
            raise ValueError(f'{name} headless Service allocations are '
                             'inconsistent.')
        return allocations

    if require_pod_node_name:
        valid_pointers = (
            pointers == _PROVIDER_KUBERNETES_POD_ALLOCATION_POINTERS_V1)
    else:
        valid_pointers = pointers in (
            (), _PROVIDER_KUBERNETES_POD_ALLOCATION_POINTERS_V1)
    if not valid_pointers:
        requirement = ('the scheduler nodeName allocation'
                       if require_pod_node_name else
                       'no allocation or the scheduler nodeName allocation')
        raise ValueError(f'{name} Pod must contain {requirement}.')
    return allocations


_PROVIDER_KUBERNETES_RESOLVED_BINDING_ROWS_V1 = (
    ('head_labels', 'object'),
    ('head_name', 'string'),
    ('head_pod_labels', 'object'),
    ('head_pod_name', 'string'),
    ('head_service_selector', 'object'),
    ('head_ssh_labels', 'object'),
    ('head_ssh_name', 'string'),
    ('image_pull_policy', 'string'),
    ('original_user', 'string'),
    ('pod_cpu_limit', 'string'),
    ('pod_cpu_request', 'string'),
    ('pod_memory_limit', 'string'),
    ('pod_memory_request', 'string'),
    ('replica_id_text', 'string'),
    ('target_namespace', 'string'),
    ('workload_image', 'string'),
    ('workload_service_account', 'string'),
)


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesBindingV1(_CanonicalContract):
    """One position-bound result from the frozen renderer binding table."""

    sequence: int
    name: str
    json_type: str
    value: CanonicalJsonValue

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'sequence', 'name', 'json_type', 'value'})

    def __post_init__(self) -> None:
        sequence = _nonnegative_integer(
            self.sequence,
            name='resolved Kubernetes binding sequence',
            maximum=len(_PROVIDER_KUBERNETES_RESOLVED_BINDING_ROWS_V1) - 1)
        if type(self.name) is not str or type(self.json_type) is not str:
            raise TypeError('resolved Kubernetes binding name and json_type '
                            'must be text.')
        expected_name, expected_json_type = (
            _PROVIDER_KUBERNETES_RESOLVED_BINDING_ROWS_V1[sequence])
        if (self.name, self.json_type) != (expected_name, expected_json_type):
            raise ValueError('resolved Kubernetes binding does not match its '
                             'frozen table row.')
        if type(self.value) is not CanonicalJsonValue:
            raise TypeError('resolved Kubernetes binding value must be an '
                            'exact CanonicalJsonValue.')
        raw_value = self.value.canonical_value()
        expected_value_type = {
            'string': str,
            'object': dict,
            'array': list,
        }[expected_json_type]
        if type(raw_value) is not expected_value_type:
            raise TypeError('resolved Kubernetes binding value does not match '
                            'its declared JSON type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ResolvedProviderKubernetesBindingV1:
        raw = _closed_object_shallow(value,
                                     name='resolved Kubernetes binding',
                                     keys=cls._KEYS)
        return cls(sequence=raw['sequence'],
                   name=raw['name'],
                   json_type=raw['json_type'],
                   value=CanonicalJsonValue.from_value(raw['value']))

    def canonical_value(self) -> JsonObject:
        return {
            'sequence': self.sequence,
            'name': self.name,
            'json_type': self.json_type,
            'value': self.value.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesBindingSetV1(_CanonicalContract):
    """Exact complete result of renderer-input binding resolution."""

    version: int
    contract: str
    bindings: tuple[ResolvedProviderKubernetesBindingV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'bindings'})
    _CONTRACT: ClassVar[
        str] = 'skypilot.serve.prebooted-direct-pod.resolved-bindings.v1'

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='resolved Kubernetes binding set version')
        if type(self.contract) is not str:
            raise TypeError('resolved Kubernetes binding set contract must be '
                            'text.')
        if self.contract != self._CONTRACT:
            raise ValueError('resolved Kubernetes binding set contract is '
                             'unsupported.')
        if type(self.bindings) is not tuple:
            raise TypeError('resolved Kubernetes binding set bindings must be '
                            'a tuple.')
        if (len(self.bindings)
                != len(_PROVIDER_KUBERNETES_RESOLVED_BINDING_ROWS_V1) or any(
                    type(binding) is not ResolvedProviderKubernetesBindingV1
                    for binding in self.bindings)):
            raise ValueError('resolved Kubernetes binding set must contain '
                             'exactly 17 typed bindings.')
        if tuple(binding.sequence for binding in self.bindings) != tuple(
                range(17)):
            raise ValueError('resolved Kubernetes bindings are not in exact '
                             'table order.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ResolvedProviderKubernetesBindingSetV1:
        raw = _closed_object_shallow(value,
                                     name='resolved Kubernetes binding set',
                                     keys=cls._KEYS)
        raw_bindings = raw['bindings']
        if type(raw_bindings) is not list:
            raise TypeError('resolved Kubernetes binding set bindings must be '
                            'a list.')
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   bindings=tuple(
                       ResolvedProviderKubernetesBindingV1.from_value(binding)
                       for binding in raw_bindings))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': self._CONTRACT,
            'bindings': [
                binding.canonical_value() for binding in self.bindings
            ],
        }


_PROVIDER_KUBERNETES_IDENTITY_LABEL_KEYS_V1 = frozenset({
    'skypilot-cluster-name',
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
})
_PROVIDER_KUBERNETES_SELECTOR_KEYS_V1 = frozenset({
    'component',
    'skypilot-cluster-name',
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
})


def _validate_provider_kubernetes_body_metadata_v1(
        role: ProviderObjectRoleV1,
        body: JsonObject) -> tuple[JsonObject, JsonObject]:
    """Validate exact common metadata and return metadata plus labels."""

    if set(body) != {'apiVersion', 'kind', 'metadata', 'spec'}:
        raise ValueError('validated Kubernetes body top-level shape is not '
                         'exact.')
    expected_kind = ('Pod'
                     if role is ProviderObjectRoleV1.HEAD_POD else 'Service')
    if body['apiVersion'] != 'v1' or body['kind'] != expected_kind:
        raise ValueError('validated Kubernetes body apiVersion or kind does '
                         'not match its role.')
    expected_metadata_keys = (frozenset({
        'annotations', 'labels', 'name', 'namespace'
    }) if role is ProviderObjectRoleV1.HEAD_POD else frozenset(
        {'labels', 'name', 'namespace'}))
    if (type(body['metadata']) is not dict or
            set(body['metadata']) != expected_metadata_keys):
        raise ValueError('validated Kubernetes body metadata is not exact.')
    metadata = body['metadata']
    labels = metadata['labels']
    expected_role_label = ('component' if role is ProviderObjectRoleV1.HEAD_POD
                           else 'service-role')
    expected_label_keys = (_PROVIDER_KUBERNETES_IDENTITY_LABEL_KEYS_V1 |
                           {'skypilot-user', expected_role_label})
    labels = _closed_object_shallow(labels,
                                    name='validated Kubernetes body labels',
                                    keys=expected_label_keys)
    name = _dns_label(metadata['name'],
                      name='validated Kubernetes body metadata.name')
    _dns_label(metadata['namespace'],
               name='validated Kubernetes body metadata.namespace')
    provider_cluster_name = _dns_label(
        labels['skypilot-cluster-name'],
        name='validated Kubernetes body cluster-name label')
    cleaned_user = _text(labels['skypilot-user'],
                         name='validated Kubernetes body user label')
    del cleaned_user
    for key in ('skypilot.co/cluster-record-uuid',
                'skypilot.co/serve-replica-incarnation'):
        _uuid(labels[key], name=f'validated Kubernetes body label {key}')
    workload_name = f'{provider_cluster_name}-head'
    if role is ProviderObjectRoleV1.HEAD_POD:
        if labels['component'] != workload_name or name != workload_name:
            raise ValueError('validated Kubernetes Pod name and component '
                             'label do not match its cluster label.')
        annotations = _closed_object_shallow(
            metadata['annotations'],
            name='validated Kubernetes Pod annotations',
            keys=frozenset({'skypilot-user'}))
        _text(annotations['skypilot-user'],
              name='validated Kubernetes Pod user annotation')
    else:
        if labels['service-role'] != role.value:
            raise ValueError('validated Kubernetes Service role label does '
                             'not match its role.')
        expected_name = (f'{workload_name}-ssh'
                         if role is ProviderObjectRoleV1.HEAD_SSH_SERVICE else
                         workload_name)
        if name != expected_name:
            raise ValueError('validated Kubernetes Service name does not '
                             'match its cluster label and role.')
    spec = body['spec']
    if type(spec) is not dict:
        raise TypeError('validated Kubernetes body spec must be an object.')
    return dict(metadata), dict(labels)


def _validate_provider_kubernetes_service_body_v1(role: ProviderObjectRoleV1,
                                                  body: JsonObject,
                                                  labels: JsonObject) -> None:
    """Validate one exact renderer-owned Service request body."""

    base_keys = {'type', 'sessionAffinity', 'internalTrafficPolicy', 'selector'}
    expected_keys = (base_keys | {'ports'} if role
                     is ProviderObjectRoleV1.HEAD_SSH_SERVICE else base_keys |
                     {'clusterIP'})
    spec = _closed_object_shallow(body['spec'],
                                  name='validated Kubernetes Service spec',
                                  keys=frozenset(expected_keys))
    if (spec['type'] != 'ClusterIP' or spec['sessionAffinity'] != 'None' or
            spec['internalTrafficPolicy'] != 'Cluster'):
        raise ValueError('validated Kubernetes Service defaults are not '
                         'exact.')
    selector = _closed_object_shallow(
        spec['selector'],
        name='validated Kubernetes Service selector',
        keys=_PROVIDER_KUBERNETES_SELECTOR_KEYS_V1)
    expected_selector = {
        key: (body['metadata']['name'] if key == 'component' and
              role is ProviderObjectRoleV1.HEAD_SERVICE else
              body['metadata']['name'][:-len('-ssh')] if key == 'component' else
              labels[key]) for key in _PROVIDER_KUBERNETES_SELECTOR_KEYS_V1
    }
    if dict(selector) != expected_selector:
        raise ValueError('validated Kubernetes Service selector does not '
                         'match its identity labels and workload name.')
    if role is ProviderObjectRoleV1.HEAD_SSH_SERVICE:
        if spec['ports'] != [{
                'protocol': 'TCP',
                'port': 22,
                'targetPort': 22,
        }]:
            raise ValueError('validated Kubernetes SSH Service port is not '
                             'exact.')
    elif spec['clusterIP'] != 'None':
        raise ValueError('validated Kubernetes head Service clusterIP intent '
                         'must be None.')


def _validate_provider_kubernetes_pod_body_v1(body: JsonObject) -> None:
    """Validate one exact renderer-owned Pod request body."""

    spec = _closed_object_shallow(
        body['spec'],
        name='validated Kubernetes Pod spec',
        keys=frozenset({
            'automountServiceAccountToken', 'containers', 'dnsPolicy',
            'enableServiceLinks', 'preemptionPolicy', 'priority',
            'restartPolicy', 'schedulerName', 'securityContext',
            'serviceAccount', 'serviceAccountName',
            'terminationGracePeriodSeconds', 'tolerations'
        }))
    if (spec['automountServiceAccountToken'] is not False or
            spec['dnsPolicy'] != 'ClusterFirst' or
            spec['enableServiceLinks'] is not True or
            spec['preemptionPolicy'] != 'PreemptLowerPriority' or
            type(spec['priority']) is not int or spec['priority'] != 0 or
            spec['restartPolicy'] != 'Always' or
            spec['schedulerName'] != 'default-scheduler' or
            spec['securityContext'] != {} or
            type(spec['terminationGracePeriodSeconds']) is not int or
            spec['terminationGracePeriodSeconds'] != 30):
        raise ValueError('validated Kubernetes Pod defaults are not exact.')
    service_account = _dns_label(spec['serviceAccount'],
                                 name='validated Kubernetes Pod serviceAccount')
    if spec['serviceAccountName'] != service_account:
        raise ValueError('validated Kubernetes Pod service-account fields '
                         'must be byte-equal.')
    expected_tolerations = [{
        'effect': 'NoExecute',
        'key': 'node.kubernetes.io/not-ready',
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }, {
        'effect': 'NoExecute',
        'key': 'node.kubernetes.io/unreachable',
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }]
    if spec['tolerations'] != expected_tolerations:
        raise ValueError('validated Kubernetes Pod tolerations are not exact.')
    containers = spec['containers']
    if type(containers) is not list or len(containers) != 1:
        raise ValueError('validated Kubernetes Pod must contain exactly one '
                         'container.')
    container = _closed_object_shallow(
        containers[0],
        name='validated Kubernetes Pod container',
        keys=frozenset({
            'env', 'image', 'imagePullPolicy', 'name', 'ports', 'resources',
            'terminationMessagePath', 'terminationMessagePolicy'
        }))
    if (container['name'] != 'ray-node' or
            container['imagePullPolicy'] != 'Always' or
            container['terminationMessagePath'] != '/dev/termination-log' or
            container['terminationMessagePolicy'] != 'File'):
        raise ValueError('validated Kubernetes Pod container literals are '
                         'not exact.')
    image = _text(container['image'],
                  name='validated Kubernetes Pod container image')
    try:
        canonical_image = container_image_models.validate_oci_reference(
            image, 'validated Kubernetes Pod container image')
        _, image_digest = container_image_models.split_digest(canonical_image)
    except (TypeError, ValueError) as e:
        raise ValueError('validated Kubernetes Pod container image is not a '
                         'canonical OCI reference.') from e
    if canonical_image != image or image_digest is None:
        raise ValueError('validated Kubernetes Pod container image must be '
                         'digest-pinned.')
    environment = container['env']
    if type(environment) is not list:
        raise ValueError('validated Kubernetes Pod replica environment entry '
                         'is not exact.')
    replica_environment = [
        entry for entry in environment if type(entry) is dict and
        entry.get('name') == 'SKYPILOT_SERVE_REPLICA_ID'
    ]
    if len(environment) != 1 or len(replica_environment) != 1:
        raise ValueError('validated Kubernetes Pod replica environment entry '
                         'is not exact.')
    replica_entry, = replica_environment
    if set(replica_entry) != {'name', 'value'}:
        raise ValueError('validated Kubernetes Pod replica environment entry '
                         'is not exact.')
    _decimal_integer_text(
        replica_entry['value'],
        name='validated Kubernetes Pod replica environment value')
    expected_ports = [{
        'containerPort': port,
        'protocol': 'TCP'
    } for port in (10001, 10002, 10003, 10004, 46590)]
    if container['ports'] != expected_ports:
        raise ValueError('validated Kubernetes Pod management and application '
                         'ports are invalid.')
    resources = _closed_object_shallow(
        container['resources'],
        name='validated Kubernetes Pod resources',
        keys=frozenset({'limits', 'requests'}))
    limits = _closed_object_shallow(
        resources['limits'],
        name='validated Kubernetes Pod resource limits',
        keys=frozenset({'cpu', 'memory'}))
    requests = _closed_object_shallow(
        resources['requests'],
        name='validated Kubernetes Pod resource requests',
        keys=frozenset({'cpu', 'memory'}))
    if dict(limits) != dict(requests):
        raise ValueError('validated Kubernetes Pod resource requests and '
                         'limits must be byte-equal.')
    _canonical_positive_decimal_text(
        requests['cpu'], name='validated Kubernetes Pod CPU request')
    memory = _text(requests['memory'],
                   name='validated Kubernetes Pod memory request')
    if not memory.endswith('G'):
        raise ValueError('validated Kubernetes Pod memory must use a G '
                         'suffix.')
    _canonical_positive_decimal_text(
        memory[:-1], name='validated Kubernetes Pod memory request')


@dataclasses.dataclass(frozen=True)
class ValidatedKubernetesServeThreeObjectBodyV1(_CanonicalContract):
    """One schema-valid request body tagged with its direct-Pod role."""

    role: ProviderObjectRoleV1
    body: CanonicalJsonObject

    _KEYS: ClassVar[frozenset[str]] = frozenset({'role', 'body'})

    def __post_init__(self) -> None:
        if type(self.role) is not ProviderObjectRoleV1:
            raise TypeError('validated Kubernetes body role must be an exact '
                            'ProviderObjectRoleV1.')
        if type(self.body) is not CanonicalJsonObject:
            raise TypeError('validated Kubernetes body must be an exact '
                            'CanonicalJsonObject.')
        raw_body = self.body.canonical_value()
        _, labels = _validate_provider_kubernetes_body_metadata_v1(
            self.role, raw_body)
        if self.role is ProviderObjectRoleV1.HEAD_POD:
            _validate_provider_kubernetes_pod_body_v1(raw_body)
        else:
            _validate_provider_kubernetes_service_body_v1(
                self.role, raw_body, labels)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ValidatedKubernetesServeThreeObjectBodyV1:
        raw = _closed_object_shallow(value,
                                     name='validated Kubernetes body',
                                     keys=cls._KEYS)
        return cls(role=_enum_value(ProviderObjectRoleV1,
                                    raw['role'],
                                    name='validated Kubernetes body role'),
                   body=CanonicalJsonObject.from_value(raw['body']))

    def canonical_value(self) -> JsonObject:
        return {
            'role': self.role.value,
            'body': self.body.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRequestNormalizationV1(_CanonicalContract):
    """Projected semantic request plus its role-specific allocation intent."""

    requested_semantic: CanonicalJsonObject
    requested_allocation_intent: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'requested_semantic', 'requested_allocation_intent'})
    _INTENTS: ClassVar[frozenset[str]] = frozenset({
        'allocate_single_stack_cluster_ip', 'headless_single_stack',
        'schedule_one_node'
    })

    def __post_init__(self) -> None:
        if type(self.requested_semantic) is not CanonicalJsonObject:
            raise TypeError('request normalization semantic must be an exact '
                            'CanonicalJsonObject.')
        if type(self.requested_allocation_intent) is not str:
            raise TypeError('request normalization allocation intent must be '
                            'text.')
        if self.requested_allocation_intent not in self._INTENTS:
            raise ValueError('request normalization allocation intent is '
                             'unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRequestNormalizationV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes request normalization',
                                     keys=cls._KEYS)
        return cls(
            requested_semantic=CanonicalJsonObject.from_value(
                raw['requested_semantic']),
            requested_allocation_intent=raw['requested_allocation_intent'])

    def canonical_value(self) -> JsonObject:
        return {
            'requested_semantic': self.requested_semantic.canonical_value(),
            'requested_allocation_intent': self.requested_allocation_intent,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesAdmittedNormalizationV1(_CanonicalContract):
    """Projected admitted semantic plus separated server allocations."""

    admitted_semantic: CanonicalJsonObject
    server_allocations: tuple[ProviderKubernetesServerAllocationV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'admitted_semantic', 'server_allocations'})

    def __post_init__(self) -> None:
        if type(self.admitted_semantic) is not CanonicalJsonObject:
            raise TypeError('admitted normalization semantic must be an exact '
                            'CanonicalJsonObject.')
        if type(self.server_allocations) is not tuple:
            raise TypeError('admitted normalization server_allocations must '
                            'be a tuple.')
        allocations = self.server_allocations
        if any(
                type(allocation) is not ProviderKubernetesServerAllocationV1
                for allocation in allocations):
            raise TypeError('admitted normalization must contain exact typed '
                            'server allocations.')
        pointers = tuple(allocation.json_pointer for allocation in allocations)
        if pointers == _PROVIDER_KUBERNETES_SERVICE_ALLOCATION_POINTERS_V1:
            cluster_ip = allocations[0].value.canonical_value()
            role = (ProviderObjectRoleV1.HEAD_SERVICE if cluster_ip == 'None'
                    else ProviderObjectRoleV1.HEAD_SSH_SERVICE)
            _validate_provider_kubernetes_role_allocations_v1(
                role,
                allocations,
                name='admitted normalization server_allocations')
        elif pointers in ((), _PROVIDER_KUBERNETES_POD_ALLOCATION_POINTERS_V1):
            _validate_provider_kubernetes_role_allocations_v1(
                ProviderObjectRoleV1.HEAD_POD,
                allocations,
                name='admitted normalization server_allocations')
        else:
            raise ValueError('admitted normalization server allocations have '
                             'an invalid atomic shape.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderKubernetesAdmittedNormalizationV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes admitted normalization',
                                     keys=cls._KEYS)
        raw_allocations = raw['server_allocations']
        if type(raw_allocations) is not list:
            raise TypeError('admitted normalization server_allocations must '
                            'be a list.')
        return cls(
            admitted_semantic=CanonicalJsonObject.from_value(
                raw['admitted_semantic']),
            server_allocations=tuple(
                ProviderKubernetesServerAllocationV1.from_value(allocation)
                for allocation in raw_allocations))

    def canonical_value(self) -> JsonObject:
        return {
            'admitted_semantic': self.admitted_semantic.canonical_value(),
            'server_allocations': [
                allocation.canonical_value()
                for allocation in self.server_allocations
            ],
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesResolvedObjectV1(_CanonicalContract):
    """One write-once admitted Kubernetes object commitment."""

    role: ProviderObjectRoleV1
    kind: ProviderPodTopologyMutableObjectKindV1
    namespace: str
    name: str
    uid: str
    observed_semantic_sha256: str
    server_allocations: tuple[ProviderKubernetesServerAllocationV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'kind', 'namespace', 'name', 'uid', 'observed_semantic_sha256',
        'server_allocations'
    })

    def __post_init__(self) -> None:
        role = _enum_value(ProviderObjectRoleV1,
                           self.role,
                           name='resolved_object.role')
        kind = _enum_value(ProviderPodTopologyMutableObjectKindV1,
                           self.kind,
                           name='resolved_object.kind')
        role_entry = next(
            entry for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
            if entry.role is role)
        if kind is not role_entry.kind:
            raise ValueError('resolved object role and kind do not match.')
        object.__setattr__(self, 'role', role)
        object.__setattr__(self, 'kind', kind)
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='resolved_object.namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(self, 'name',
                           _dns_label(self.name, name='resolved_object.name'))
        object.__setattr__(self, 'uid',
                           _text(self.uid, name='resolved_object.uid'))
        object.__setattr__(
            self, 'observed_semantic_sha256',
            _sha256(self.observed_semantic_sha256,
                    name='resolved_object.observed_semantic_sha256'))
        object.__setattr__(
            self, 'server_allocations',
            _validate_provider_kubernetes_role_allocations_v1(
                role,
                self.server_allocations,
                name='resolved_object.server_allocations'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesResolvedObjectV1:
        shallow = _closed_object_shallow(value,
                                         name='Kubernetes resolved object',
                                         keys=cls._KEYS)
        raw_allocations = shallow['server_allocations']
        if not isinstance(raw_allocations, list):
            raise TypeError('resolved object server_allocations must be a '
                            'list.')
        role = _enum_value(ProviderObjectRoleV1,
                           shallow['role'],
                           name='resolved_object.role')
        if role in (ProviderObjectRoleV1.HEAD_SSH_SERVICE,
                    ProviderObjectRoleV1.HEAD_SERVICE):
            valid_length = len(raw_allocations) == len(
                _PROVIDER_KUBERNETES_SERVICE_ALLOCATION_POINTERS_V1)
        else:
            valid_length = len(raw_allocations) in (0, 1)
        if not valid_length:
            if role in (ProviderObjectRoleV1.HEAD_SSH_SERVICE,
                        ProviderObjectRoleV1.HEAD_SERVICE):
                raise ValueError('resolved object server_allocations must '
                                 'contain the complete Service allocation '
                                 'quartet in canonical order.')
            raise ValueError('resolved object Pod server_allocations has '
                             'invalid role-specific cardinality.')
        allocations = tuple(
            ProviderKubernetesServerAllocationV1.from_value(allocation)
            for allocation in raw_allocations)
        return cls(role=role,
                   kind=shallow['kind'],
                   namespace=shallow['namespace'],
                   name=shallow['name'],
                   uid=shallow['uid'],
                   observed_semantic_sha256=shallow['observed_semantic_sha256'],
                   server_allocations=allocations)

    def canonical_value(self) -> JsonObject:
        return {
            'role': self.role.value,
            'kind': self.kind.value,
            'namespace': self.namespace,
            'name': self.name,
            'uid': self.uid,
            'observed_semantic_sha256': self.observed_semantic_sha256,
            'server_allocations': [
                allocation.canonical_value()
                for allocation in self.server_allocations
            ],
        }

    @property
    def has_complete_allocations(self) -> bool:
        """Whether this role has every allocation required at launch success."""

        return (self.role is not ProviderObjectRoleV1.HEAD_POD or
                bool(self.server_allocations))


class ProviderKubernetesResolvedObjectSlotDispositionV1(str, enum.Enum):
    """Whether one canonical object-role slot has a write-once commitment."""

    UNKNOWN = 'unknown'
    COMMITTED = 'committed'


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesResolvedObjectSlotV1(_CanonicalContract):
    """One explicit slot in the canonical launch create order."""

    sequence: int
    role: ProviderObjectRoleV1
    disposition: ProviderKubernetesResolvedObjectSlotDispositionV1
    object: ProviderKubernetesResolvedObjectV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'sequence', 'role', 'disposition', 'object'})

    def __post_init__(self) -> None:
        sequence = _nonnegative_integer(self.sequence,
                                        name='resolved_object_slot.sequence')
        role_entry = _PROVIDER_KUBERNETES_OBJECT_ROLE_BY_SEQUENCE_V1.get(
            sequence)
        if role_entry is None:
            raise ValueError('resolved object slot sequence is unsupported.')
        role = _enum_value(ProviderObjectRoleV1,
                           self.role,
                           name='resolved_object_slot.role')
        disposition = _enum_value(
            ProviderKubernetesResolvedObjectSlotDispositionV1,
            self.disposition,
            name='resolved_object_slot.disposition')
        if role is not role_entry.role:
            raise ValueError('resolved object slot sequence and role do not '
                             'match.')
        if (self.object is not None and
                type(self.object) is not ProviderKubernetesResolvedObjectV1):
            raise TypeError('resolved object slot object has an invalid type.')
        if ((disposition
             is ProviderKubernetesResolvedObjectSlotDispositionV1.COMMITTED)
                != (self.object is not None)):
            raise ValueError('committed resolved object slots require an '
                             'object; unknown slots require null.')
        if self.object is not None and self.object.role is not role:
            raise ValueError('resolved object slot and object roles do not '
                             'match.')
        object.__setattr__(self, 'sequence', sequence)
        object.__setattr__(self, 'role', role)
        object.__setattr__(self, 'disposition', disposition)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesResolvedObjectSlotV1:
        shallow = _closed_object_shallow(value,
                                         name='Kubernetes resolved object slot',
                                         keys=cls._KEYS)
        return cls(sequence=shallow['sequence'],
                   role=shallow['role'],
                   disposition=shallow['disposition'],
                   object=(None if shallow['object'] is None else
                           ProviderKubernetesResolvedObjectV1.from_value(
                               shallow['object'])))

    def canonical_value(self) -> JsonObject:
        return {
            'sequence': self.sequence,
            'role': self.role.value,
            'disposition': self.disposition.value,
            'object':
                (None if self.object is None else self.object.canonical_value()
                ),
        }


@dataclasses.dataclass(frozen=True)
class PartialResolvedProviderTargetV1(_CanonicalContract):
    """Canonical explicit-prefix Kubernetes launch progress."""

    version: int
    requested_target_sha256: str
    kubernetes_objects: tuple[ProviderKubernetesResolvedObjectSlotV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'requested_target_sha256', 'kubernetes_objects'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='partial resolved target version')
        object.__setattr__(
            self, 'requested_target_sha256',
            _sha256(self.requested_target_sha256,
                    name='partial_target.requested_target_sha256'))
        if (type(self.kubernetes_objects) is not tuple or
                len(self.kubernetes_objects)
                != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1) or any(
                    type(slot) is not ProviderKubernetesResolvedObjectSlotV1
                    for slot in self.kubernetes_objects)):
            raise ValueError('partial target requires exactly three typed '
                             'Kubernetes object slots.')
        expected = tuple((entry.plan_sequence, entry.role)
                         for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
        actual = tuple(
            (slot.sequence, slot.role) for slot in self.kubernetes_objects)
        if actual != expected:
            raise ValueError('partial target object slots have invalid order.')
        dispositions = tuple(
            slot.disposition for slot in self.kubernetes_objects)
        seen_unknown = False
        for disposition in dispositions:
            if disposition is (
                    ProviderKubernetesResolvedObjectSlotDispositionV1.UNKNOWN):
                seen_unknown = True
            elif seen_unknown:
                raise ValueError('partial target committed slots must form a '
                                 'prefix of create order.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> PartialResolvedProviderTargetV1:
        shallow = _closed_object_shallow(value,
                                         name='partial resolved target',
                                         keys=cls._KEYS)
        raw_objects = shallow['kubernetes_objects']
        if not isinstance(raw_objects, list):
            raise TypeError('partial target kubernetes_objects must be a list.')
        if len(raw_objects) != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1):
            raise ValueError('partial target requires exactly three '
                             'Kubernetes object slots.')
        objects = tuple(
            ProviderKubernetesResolvedObjectSlotV1.from_value(slot)
            for slot in raw_objects)
        return cls(version=shallow['version'],
                   requested_target_sha256=shallow['requested_target_sha256'],
                   kubernetes_objects=objects)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'requested_target_sha256': self.requested_target_sha256,
            'kubernetes_objects': [
                slot.canonical_value() for slot in self.kubernetes_objects
            ],
        }

    def validate_requested_target(self, target: ProviderLocatorV1) -> None:
        if self.requested_target_sha256 != target.sha256:
            raise ValueError('Partial target does not match requested target.')


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesHandleProviderConfigV1(_CanonicalContract):
    """Closed provider block persisted in one Kubernetes cluster handle."""

    context_mode: str
    scope_sha256: str
    namespace: str
    port_mode: str
    use_internal_ips: bool
    application_port: str
    pod_name: str
    pod_uid: str
    node_name: str
    pod_ip: str
    head_service_uid: str
    head_ssh_service_uid: str
    ambient_fallback: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'context_mode', 'scope_sha256', 'namespace', 'port_mode',
        'use_internal_ips', 'application_port', 'pod_name', 'pod_uid',
        'node_name', 'pod_ip', 'head_service_uid', 'head_ssh_service_uid',
        'ambient_fallback'
    })

    def __post_init__(self) -> None:
        if (type(self.context_mode) is not str or
                self.context_mode != 'in_cluster'):
            raise ValueError('handle provider_config context_mode must be '
                             'in_cluster.')
        object.__setattr__(
            self, 'scope_sha256',
            _sha256(self.scope_sha256,
                    name='handle.provider_config.scope_sha256'))
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='handle.provider_config.namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if type(self.port_mode) is not str or self.port_mode != 'podip':
            raise ValueError('handle provider_config port_mode must be podip.')
        if not _boolean(self.use_internal_ips,
                        name='handle.provider_config.use_internal_ips'):
            raise ValueError('handle provider_config use_internal_ips must be '
                             'true.')
        object.__setattr__(
            self, 'application_port',
            _decimal_port_text(self.application_port,
                               name='handle.provider_config.application_port'))
        object.__setattr__(
            self, 'pod_name',
            _dns_label(self.pod_name, name='handle.provider_config.pod_name'))
        object.__setattr__(
            self, 'node_name',
            _dns_subdomain(self.node_name,
                           name='handle.provider_config.node_name'))
        object.__setattr__(
            self, 'pod_ip',
            _canonical_ip_text(self.pod_ip,
                               name='handle.provider_config.pod_ip'))
        for field in ('pod_uid', 'head_service_uid', 'head_ssh_service_uid'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'handle.provider_config.{field}'))
        if _boolean(self.ambient_fallback,
                    name='handle.provider_config.ambient_fallback'):
            raise ValueError('handle provider_config ambient_fallback must be '
                             'false.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesHandleProviderConfigV1:
        shallow = _closed_object_shallow(
            value, name='Kubernetes handle provider config', keys=cls._KEYS)
        return cls(**shallow)

    def canonical_value(self) -> JsonObject:
        return {
            'context_mode': 'in_cluster',
            'scope_sha256': self.scope_sha256,
            'namespace': self.namespace,
            'port_mode': 'podip',
            'use_internal_ips': True,
            'application_port': self.application_port,
            'pod_name': self.pod_name,
            'pod_uid': self.pod_uid,
            'node_name': self.node_name,
            'pod_ip': self.pod_ip,
            'head_service_uid': self.head_service_uid,
            'head_ssh_service_uid': self.head_ssh_service_uid,
            'ambient_fallback': False,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesHandleV1(_CanonicalContract):
    """Exact nonambient Kubernetes provider handle."""

    version: int
    cluster_record_uuid: uuid.UUID
    cluster_name: str
    cluster_name_on_cloud: str
    requested_target_sha256: str
    launched_resources_sha256: str
    provider_config: ProviderKubernetesHandleProviderConfigV1
    provider_config_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cluster_record_uuid', 'cluster_name',
        'cluster_name_on_cloud', 'requested_target_sha256',
        'launched_resources_sha256', 'provider_config', 'provider_config_sha256'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='Kubernetes handle version')
        object.__setattr__(
            self, 'cluster_record_uuid',
            _uuid(self.cluster_record_uuid, name='handle.cluster_record_uuid'))
        object.__setattr__(
            self, 'cluster_name',
            _text(self.cluster_name,
                  name='handle.cluster_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'cluster_name_on_cloud',
            _dns_label(self.cluster_name_on_cloud,
                       name='handle.cluster_name_on_cloud'))
        object.__setattr__(
            self, 'requested_target_sha256',
            _sha256(self.requested_target_sha256,
                    name='handle.requested_target_sha256'))
        object.__setattr__(
            self, 'launched_resources_sha256',
            _sha256(self.launched_resources_sha256,
                    name='handle.launched_resources_sha256'))
        if type(self.provider_config) is not (
                ProviderKubernetesHandleProviderConfigV1):
            raise TypeError('handle provider_config has an invalid type.')
        if self.provider_config.pod_name != f'{self.cluster_name_on_cloud}-head':
            raise ValueError('handle Pod name does not match '
                             'cluster_name_on_cloud.')
        provider_config_sha256 = _sha256(self.provider_config_sha256,
                                         name='handle.provider_config_sha256')
        if provider_config_sha256 != self.provider_config.sha256:
            raise ValueError('handle provider_config hash does not match.')
        object.__setattr__(self, 'provider_config_sha256',
                           provider_config_sha256)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesHandleV1:
        shallow = _closed_object_shallow(value,
                                         name='Kubernetes handle',
                                         keys=cls._KEYS)
        return cls(
            version=shallow['version'],
            cluster_record_uuid=_uuid(shallow['cluster_record_uuid'],
                                      name='handle.cluster_record_uuid'),
            cluster_name=shallow['cluster_name'],
            cluster_name_on_cloud=shallow['cluster_name_on_cloud'],
            requested_target_sha256=shallow['requested_target_sha256'],
            launched_resources_sha256=shallow['launched_resources_sha256'],
            provider_config=ProviderKubernetesHandleProviderConfigV1.from_value(
                shallow['provider_config']),
            provider_config_sha256=shallow['provider_config_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'cluster_record_uuid': str(self.cluster_record_uuid),
            'cluster_name': self.cluster_name,
            'cluster_name_on_cloud': self.cluster_name_on_cloud,
            'requested_target_sha256': self.requested_target_sha256,
            'launched_resources_sha256': self.launched_resources_sha256,
            'provider_config': self.provider_config.canonical_value(),
            'provider_config_sha256': self.provider_config_sha256,
        }

    def validate_requested_target(self, target: ProviderLocatorV1) -> None:
        """Bind the handle to the locator fields available in this scaffold."""

        if (self.requested_target_sha256 != target.sha256 or
                self.cluster_record_uuid != target.sky_cluster_record_uuid or
                self.cluster_name != target.sky_cluster_name or
                target.kubernetes is None or
                self.provider_config.namespace != target.kubernetes.namespace or
                self.provider_config.pod_name
                != target.kubernetes.workload_name):
            raise ValueError('Kubernetes handle does not match the requested '
                             'target.')

    def validate_launched_resources(
            self, resources: ProviderPodResourceSnapshotV1) -> None:
        if self.launched_resources_sha256 != resources.sha256:
            raise ValueError('Kubernetes handle does not match launched '
                             'resources.')

    def validate_workspace_identity(
            self, workspace: ProviderWorkspaceIdentityV1) -> None:
        if (self.provider_config.scope_sha256
                != workspace.kubernetes_scope.sha256 or
                self.provider_config.namespace
                != workspace.kubernetes_scope.namespace):
            raise ValueError('Kubernetes handle does not match workspace '
                             'scope.')


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
        if type(self.qualification) is not ProviderOCIImageQualificationV1:
            raise TypeError('pod image qualification has an invalid type.')
        if self.auth_strategy != 'anonymous':
            raise ValueError('pod image auth strategy must be anonymous.')
        if (self.implementation_contract
                != 'kubernetes_serve_prebooted_runtime_v1'):
            raise ValueError('pod image implementation contract is '
                             'unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderPodImageV1:
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
class ProviderKubernetesResourceContractV1(_CanonicalContract):
    """Exact Kubernetes Pod resource translation for the first provider."""

    source_cpus: str
    source_memory_gb: str
    pod_cpu_request: str
    pod_cpu_limit: str
    pod_memory_request: str
    pod_memory_limit: str
    translation_contract: str
    set_pod_resource_limits: bool
    resource_limit_multiplier: int
    live_allocatable_clamp: bool
    accelerator: None
    ephemeral_storage: None
    image: ProviderPodImageV1
    image_pull_policy: str
    application_port: str
    resources_ports: tuple[str, ...]
    port_mode: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'source_cpus', 'source_memory_gb', 'pod_cpu_request', 'pod_cpu_limit',
        'pod_memory_request', 'pod_memory_limit', 'translation_contract',
        'set_pod_resource_limits', 'resource_limit_multiplier',
        'live_allocatable_clamp', 'accelerator', 'ephemeral_storage', 'image',
        'image_pull_policy', 'application_port', 'resources_ports', 'port_mode'
    })
    _RESERVED_RENDERER_PORTS: ClassVar[frozenset[str]] = frozenset(
        {'22', '10001', '10002', '10003', '10004', '46590'})

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'source_cpus',
            _canonical_positive_decimal_text(self.source_cpus,
                                             name='resources.source_cpus'))
        object.__setattr__(
            self, 'source_memory_gb',
            _canonical_positive_decimal_text(self.source_memory_gb,
                                             name='resources.source_memory_gb'))
        for field in ('pod_cpu_request', 'pod_cpu_limit', 'pod_memory_request',
                      'pod_memory_limit'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field), name=f'resources.{field}'))
        if (self.pod_cpu_request != self.source_cpus or
                self.pod_cpu_limit != self.source_cpus):
            raise ValueError(
                'Pod CPU request and limit must equal source_cpus.')
        expected_memory = f'{self.source_memory_gb}G'
        if (self.pod_memory_request != expected_memory or
                self.pod_memory_limit != expected_memory):
            raise ValueError('Pod memory request and limit must equal '
                             'source_memory_gb with a G suffix.')
        if self.translation_contract != 'sky_to_k8s_exact_resources_v1':
            raise ValueError('resource translation contract is unsupported.')
        if not _boolean(self.set_pod_resource_limits,
                        name='resources.set_pod_resource_limits'):
            raise ValueError('set_pod_resource_limits must be true.')
        if (not isinstance(self.resource_limit_multiplier, int) or
                isinstance(self.resource_limit_multiplier, bool) or
                self.resource_limit_multiplier != 1):
            raise ValueError('resource_limit_multiplier must be integer 1.')
        if _boolean(self.live_allocatable_clamp,
                    name='resources.live_allocatable_clamp'):
            raise ValueError('live_allocatable_clamp must be false.')
        if self.accelerator is not None or self.ephemeral_storage is not None:
            raise ValueError('accelerator and ephemeral_storage must be null.')
        if type(self.image) is not ProviderPodImageV1:
            raise TypeError('resource image has an invalid type.')
        if self.image_pull_policy != 'Always':
            raise ValueError('resource image_pull_policy must be Always.')
        object.__setattr__(
            self, 'application_port',
            _decimal_port_text(self.application_port,
                               name='resources.application_port'))
        if self.application_port in self._RESERVED_RENDERER_PORTS:
            raise ValueError('resource application_port collides with a '
                             'renderer-owned port.')
        if not isinstance(self.resources_ports, tuple):
            raise TypeError('resource resources_ports must be a tuple.')
        ports = tuple(
            _decimal_port_text(port, name='resources.resources_ports')
            for port in self.resources_ports)
        if ports != (self.application_port,):
            raise ValueError('resource resources_ports must contain exactly '
                             'the application port.')
        object.__setattr__(self, 'resources_ports', ports)
        if self.port_mode != 'podip':
            raise ValueError('resource port_mode must be podip.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesResourceContractV1:
        raw = _closed_object(value,
                             name='Kubernetes resource contract',
                             keys=cls._KEYS)
        resources_ports = raw['resources_ports']
        if not isinstance(resources_ports, list):
            raise TypeError('resource resources_ports must be a list.')
        return cls(source_cpus=raw['source_cpus'],
                   source_memory_gb=raw['source_memory_gb'],
                   pod_cpu_request=raw['pod_cpu_request'],
                   pod_cpu_limit=raw['pod_cpu_limit'],
                   pod_memory_request=raw['pod_memory_request'],
                   pod_memory_limit=raw['pod_memory_limit'],
                   translation_contract=raw['translation_contract'],
                   set_pod_resource_limits=raw['set_pod_resource_limits'],
                   resource_limit_multiplier=raw['resource_limit_multiplier'],
                   live_allocatable_clamp=raw['live_allocatable_clamp'],
                   accelerator=raw['accelerator'],
                   ephemeral_storage=raw['ephemeral_storage'],
                   image=ProviderPodImageV1.from_value(raw['image']),
                   image_pull_policy=raw['image_pull_policy'],
                   application_port=raw['application_port'],
                   resources_ports=tuple(resources_ports),
                   port_mode=raw['port_mode'])

    def canonical_value(self) -> JsonObject:
        return {
            'source_cpus': self.source_cpus,
            'source_memory_gb': self.source_memory_gb,
            'pod_cpu_request': self.pod_cpu_request,
            'pod_cpu_limit': self.pod_cpu_limit,
            'pod_memory_request': self.pod_memory_request,
            'pod_memory_limit': self.pod_memory_limit,
            'translation_contract': 'sky_to_k8s_exact_resources_v1',
            'set_pod_resource_limits': True,
            'resource_limit_multiplier': 1,
            'live_allocatable_clamp': False,
            'accelerator': None,
            'ephemeral_storage': None,
            'image': self.image.canonical_value(),
            'image_pull_policy': 'Always',
            'application_port': self.application_port,
            'resources_ports': list(self.resources_ports),
            'port_mode': 'podip',
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
            if type(value) is not tuple:
                raise TypeError(f'config projection {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'config projection {field} must be empty.')
        for field in self._FALSE_FIELDS:
            value = getattr(self, field)
            _boolean(value, name=f'config_projection.{field}')
            if value:
                raise ValueError(f'config projection {field} must be false.')
        if self.detected_network_type != 'default':
            raise ValueError('config projection detected_network_type must be '
                             'default.')
        if type(self.config_access_inventory) is not ProviderRepoArtifactRefV1:
            raise TypeError('config access inventory has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesConfigProjectionV1:
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
    def from_value(cls, value: Any) -> ProviderPolicyModeEvidenceV1:
        raw = _closed_object(value, name='policy modes', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderPolicyBoundaryProofV1(_CanonicalContract):
    """Context-free proof that one policy boundary preserved its projection."""

    version: int
    boundary: str
    config_projection_sha256: str
    modes: ProviderPolicyModeEvidenceV1
    policy_subject_sha256: str
    projection_before_sha256: str
    projection_after_sha256: str
    projections_equal: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'boundary', 'config_projection_sha256', 'modes',
        'policy_subject_sha256', 'projection_before_sha256',
        'projection_after_sha256', 'projections_equal'
    })
    _BOUNDARIES: ClassVar[frozenset[str]] = frozenset(
        {'serve_controller_prepare', 'api_executor_pre_io'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='policy boundary proof version')
        if self.boundary not in self._BOUNDARIES:
            raise ValueError('policy boundary proof boundary is unsupported.')
        for field in ('config_projection_sha256', 'policy_subject_sha256',
                      'projection_before_sha256', 'projection_after_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field),
                        name=f'policy_boundary_proof.{field}'))
        if type(self.modes) is not ProviderPolicyModeEvidenceV1:
            raise TypeError('policy boundary proof modes has an invalid type.')
        if self.projection_before_sha256 != self.projection_after_sha256:
            raise ValueError('policy boundary proof projections must have '
                             'equal hashes.')
        _boolean(self.projections_equal,
                 name='policy_boundary_proof.projections_equal')
        if not self.projections_equal:
            raise ValueError('policy boundary proof projections_equal must be '
                             'true.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderPolicyBoundaryProofV1:
        raw = _closed_object(value,
                             name='policy boundary proof',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   boundary=raw['boundary'],
                   config_projection_sha256=raw['config_projection_sha256'],
                   modes=ProviderPolicyModeEvidenceV1.from_value(raw['modes']),
                   policy_subject_sha256=raw['policy_subject_sha256'],
                   projection_before_sha256=raw['projection_before_sha256'],
                   projection_after_sha256=raw['projection_after_sha256'],
                   projections_equal=raw['projections_equal'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'boundary': self.boundary,
            'config_projection_sha256': self.config_projection_sha256,
            'modes': self.modes.canonical_value(),
            'policy_subject_sha256': self.policy_subject_sha256,
            'projection_before_sha256': self.projection_before_sha256,
            'projection_after_sha256': self.projection_after_sha256,
            'projections_equal': True,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAnnotationV1(_CanonicalContract):
    """One sorted, nonsecret provider annotation with generic text bounds."""

    key: str
    value: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'key', 'value'})

    def __post_init__(self) -> None:
        if type(self.key) is not str or type(self.value) is not str:
            raise TypeError('annotation key and value must be text.')
        object.__setattr__(self, 'key', _text(self.key, name='annotation.key'))
        object.__setattr__(self, 'value',
                           _text(self.value, name='annotation.value'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAnnotationV1:
        raw = _closed_object_shallow(value, name='annotation', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


def _provider_annotation_tuple(value: Any, *,
                               name: str) -> tuple[ProviderAnnotationV1, ...]:
    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    if (len(value) > _MAX_LIST_ITEMS or any(
            type(annotation) is not ProviderAnnotationV1
            for annotation in value)):
        raise ValueError(f'{name} must contain at most 256 typed annotations.')
    keys = tuple(annotation.key for annotation in value)
    if keys != tuple(sorted(set(keys))):
        raise ValueError(f'{name} must be sorted by unique key.')
    return value


def _provider_bounded_raw_list(value: Any, *, name: str) -> list[Any]:
    """Validate a provider wire list before copying or parsing its children."""

    if type(value) is not list:
        raise TypeError(f'{name} must be a list.')
    if len(value) > _MAX_LIST_ITEMS:
        raise ValueError(
            f'{name} must contain at most {_MAX_LIST_ITEMS} items.')
    return value


def _sorted_text_tuple(value: Any,
                       *,
                       name: str,
                       minimum_items: int = 0,
                       maximum_bytes: int = _MAX_TEXT_BYTES) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    if not minimum_items <= len(value) <= _MAX_LIST_ITEMS:
        raise ValueError(f'{name} must contain {minimum_items}..256 values.')
    if any(type(item) is not str for item in value):
        raise TypeError(f'{name} must contain exact text values.')
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
        for field in ('namespace', 'name', 'uid', 'resource_version'):
            if type(getattr(self, field)) is not str:
                raise TypeError(f'service_account.{field} must be text.')
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
    def from_value(cls,
                   value: Any) -> ProviderKubernetesServiceAccountProjectionV1:
        raw = _closed_object_shallow(value,
                                     name='service-account projection',
                                     keys=cls._KEYS)
        for field in ('labels', 'annotations', 'image_pull_secrets',
                      'legacy_secret_refs'):
            _provider_bounded_raw_list(raw[field],
                                       name=f'service-account {field}')
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


class ProviderKubernetesPrerequisiteKindV1(str, enum.Enum):
    """Closed Kubernetes prerequisite kinds for the first provider cohort."""

    NAMESPACE = 'Namespace'
    SERVICE_ACCOUNT = 'ServiceAccount'
    NETWORK_POLICY = 'NetworkPolicy'
    VALIDATING_ADMISSION_POLICY = 'ValidatingAdmissionPolicy'
    VALIDATING_ADMISSION_POLICY_BINDING = 'ValidatingAdmissionPolicyBinding'


class ProviderKubernetesPrerequisiteRoleV1(str, enum.Enum):
    """Exact semantic roles in the first Kubernetes prerequisite inventory."""

    AUTHORITY_RELEASE_NAMESPACE = 'authority_release_namespace'
    TARGET_NAMESPACE = 'target_namespace'
    KUBE_SYSTEM_NAMESPACE = 'kube_system_namespace'
    SERVE_LB_SLOT_0_NAMESPACE = 'serve_lb_slot_0_namespace'
    SERVE_LB_SLOT_1_NAMESPACE = 'serve_lb_slot_1_namespace'
    CALLER_SERVICE_ACCOUNT = 'caller_service_account'
    WORKLOAD_SERVICE_ACCOUNT = 'workload_service_account'
    SERVE_LB_SLOT_0_SERVICE_ACCOUNT = 'serve_lb_slot_0_service_account'
    SERVE_LB_SLOT_1_SERVICE_ACCOUNT = 'serve_lb_slot_1_service_account'
    ENDPOINT_NETWORK_POLICY = 'endpoint_network_policy'
    VALIDATING_ADMISSION_POLICY = 'validating_admission_policy'
    VALIDATING_ADMISSION_POLICY_BINDING = (
        'validating_admission_policy_binding')


@dataclasses.dataclass(frozen=True)
class _ProviderKubernetesPrerequisiteKindMapEntryV1:
    """One immutable API-version and scope dispatch entry."""

    api_version: str
    scope: str


PROVIDER_KUBERNETES_PREREQUISITE_KIND_MAP_V1 = types.MappingProxyType({
    ProviderKubernetesPrerequisiteKindV1.NAMESPACE:
        _ProviderKubernetesPrerequisiteKindMapEntryV1(api_version='v1',
                                                      scope='cluster'),
    ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT:
        _ProviderKubernetesPrerequisiteKindMapEntryV1(api_version='v1',
                                                      scope='namespaced'),
    ProviderKubernetesPrerequisiteKindV1.NETWORK_POLICY:
        _ProviderKubernetesPrerequisiteKindMapEntryV1(
            api_version='networking.k8s.io/v1', scope='namespaced'),
    ProviderKubernetesPrerequisiteKindV1.VALIDATING_ADMISSION_POLICY:
        _ProviderKubernetesPrerequisiteKindMapEntryV1(
            api_version='admissionregistration.k8s.io/v1', scope='cluster'),
    ProviderKubernetesPrerequisiteKindV1.VALIDATING_ADMISSION_POLICY_BINDING:
        _ProviderKubernetesPrerequisiteKindMapEntryV1(
            api_version='admissionregistration.k8s.io/v1', scope='cluster'),
})


@dataclasses.dataclass(frozen=True)
class _ProviderKubernetesPrerequisiteRoleMapEntryV1:
    """One immutable role, position, and kind dispatch entry."""

    sequence: int
    role: ProviderKubernetesPrerequisiteRoleV1
    kind: ProviderKubernetesPrerequisiteKindV1


PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1 = (
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=0,
        role=ProviderKubernetesPrerequisiteRoleV1.AUTHORITY_RELEASE_NAMESPACE,
        kind=ProviderKubernetesPrerequisiteKindV1.NAMESPACE),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=1,
        role=ProviderKubernetesPrerequisiteRoleV1.TARGET_NAMESPACE,
        kind=ProviderKubernetesPrerequisiteKindV1.NAMESPACE),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=2,
        role=ProviderKubernetesPrerequisiteRoleV1.KUBE_SYSTEM_NAMESPACE,
        kind=ProviderKubernetesPrerequisiteKindV1.NAMESPACE),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=3,
        role=ProviderKubernetesPrerequisiteRoleV1.SERVE_LB_SLOT_0_NAMESPACE,
        kind=ProviderKubernetesPrerequisiteKindV1.NAMESPACE),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=4,
        role=ProviderKubernetesPrerequisiteRoleV1.SERVE_LB_SLOT_1_NAMESPACE,
        kind=ProviderKubernetesPrerequisiteKindV1.NAMESPACE),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=5,
        role=ProviderKubernetesPrerequisiteRoleV1.CALLER_SERVICE_ACCOUNT,
        kind=ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=6,
        role=ProviderKubernetesPrerequisiteRoleV1.WORKLOAD_SERVICE_ACCOUNT,
        kind=ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=7,
        role=(ProviderKubernetesPrerequisiteRoleV1.
              SERVE_LB_SLOT_0_SERVICE_ACCOUNT),
        kind=ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=8,
        role=(ProviderKubernetesPrerequisiteRoleV1.
              SERVE_LB_SLOT_1_SERVICE_ACCOUNT),
        kind=ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=9,
        role=ProviderKubernetesPrerequisiteRoleV1.ENDPOINT_NETWORK_POLICY,
        kind=ProviderKubernetesPrerequisiteKindV1.NETWORK_POLICY),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=10,
        role=ProviderKubernetesPrerequisiteRoleV1.VALIDATING_ADMISSION_POLICY,
        kind=(
            ProviderKubernetesPrerequisiteKindV1.VALIDATING_ADMISSION_POLICY)),
    _ProviderKubernetesPrerequisiteRoleMapEntryV1(
        sequence=11,
        role=(ProviderKubernetesPrerequisiteRoleV1.
              VALIDATING_ADMISSION_POLICY_BINDING),
        kind=(ProviderKubernetesPrerequisiteKindV1.
              VALIDATING_ADMISSION_POLICY_BINDING)),
)

_PROVIDER_KUBERNETES_PREREQUISITE_ROLE_DISPATCH_V1 = types.MappingProxyType({
    entry.role: entry for entry in PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1
})


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesNamespacePrerequisiteSpecV1(_CanonicalContract):
    """Sorted live Namespace metadata retained as prerequisite evidence."""

    kind: ProviderKubernetesPrerequisiteKindV1
    labels: tuple[ProviderLabelV1, ...]
    annotations: tuple[ProviderAnnotationV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'kind', 'labels', 'annotations'})

    def __post_init__(self) -> None:
        if type(self.kind) not in (str, ProviderKubernetesPrerequisiteKindV1):
            raise TypeError('Namespace prerequisite kind must be text.')
        kind = _enum_value(ProviderKubernetesPrerequisiteKindV1,
                           self.kind,
                           name='namespace prerequisite kind')
        if kind is not ProviderKubernetesPrerequisiteKindV1.NAMESPACE:
            raise ValueError('Namespace prerequisite spec kind is invalid.')
        object.__setattr__(self, 'kind', kind)
        object.__setattr__(
            self, 'labels',
            _provider_label_tuple(self.labels, name='Namespace labels'))
        object.__setattr__(
            self, 'annotations',
            _provider_annotation_tuple(self.annotations,
                                       name='Namespace annotations'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderKubernetesNamespacePrerequisiteSpecV1:
        raw = _closed_object_shallow(value,
                                     name='Namespace prerequisite spec',
                                     keys=cls._KEYS)
        _provider_bounded_raw_list(raw['labels'],
                                   name='Namespace prerequisite labels')
        _provider_bounded_raw_list(raw['annotations'],
                                   name='Namespace prerequisite annotations')
        return cls(
            kind=raw['kind'],
            labels=tuple(
                ProviderLabelV1.from_value(item) for item in raw['labels']),
            annotations=tuple(
                ProviderAnnotationV1.from_value(item)
                for item in raw['annotations']))

    def canonical_value(self) -> JsonObject:
        return {
            'kind': self.kind.value,
            'labels': [label.canonical_value() for label in self.labels],
            'annotations': [
                annotation.canonical_value() for annotation in self.annotations
            ],
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesServiceAccountPrerequisiteSpecV1(_CanonicalContract):
    """One typed ServiceAccount projection used as prerequisite evidence."""

    kind: ProviderKubernetesPrerequisiteKindV1
    projection: ProviderKubernetesServiceAccountProjectionV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({'kind', 'projection'})

    def __post_init__(self) -> None:
        if type(self.kind) not in (str, ProviderKubernetesPrerequisiteKindV1):
            raise TypeError('ServiceAccount prerequisite kind must be text.')
        kind = _enum_value(ProviderKubernetesPrerequisiteKindV1,
                           self.kind,
                           name='ServiceAccount prerequisite kind')
        if kind is not ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT:
            raise ValueError(
                'ServiceAccount prerequisite spec kind is invalid.')
        object.__setattr__(self, 'kind', kind)
        if type(self.projection) is not (
                ProviderKubernetesServiceAccountProjectionV1):
            raise TypeError('ServiceAccount prerequisite projection has an '
                            'invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderKubernetesServiceAccountPrerequisiteSpecV1:
        raw = _closed_object_shallow(value,
                                     name='ServiceAccount prerequisite spec',
                                     keys=cls._KEYS)
        return cls(
            kind=raw['kind'],
            projection=ProviderKubernetesServiceAccountProjectionV1.from_value(
                raw['projection']))

    def canonical_value(self) -> JsonObject:
        return {
            'kind': self.kind.value,
            'projection': self.projection.canonical_value(),
        }


_ProviderKubernetesManifestPrerequisiteSpecT = TypeVar(
    '_ProviderKubernetesManifestPrerequisiteSpecT',
    bound='_ProviderKubernetesManifestPrerequisiteSpecV1')


@dataclasses.dataclass(frozen=True)
class _ProviderKubernetesManifestPrerequisiteSpecV1(_CanonicalContract):
    """Shared closed representation for one manifest-backed prerequisite."""

    kind: ProviderKubernetesPrerequisiteKindV1
    contract: str
    manifest: ProviderRepoArtifactRefV1

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'kind', 'contract', 'manifest'})
    _EXPECTED_KIND: ClassVar[ProviderKubernetesPrerequisiteKindV1]
    _EXPECTED_CONTRACT: ClassVar[str]

    def __post_init__(self) -> None:
        if type(self.kind) not in (str, ProviderKubernetesPrerequisiteKindV1):
            raise TypeError('manifest prerequisite kind must be text.')
        kind = _enum_value(ProviderKubernetesPrerequisiteKindV1,
                           self.kind,
                           name='manifest prerequisite kind')
        if kind is not self._EXPECTED_KIND:
            raise ValueError('manifest prerequisite spec kind is invalid.')
        object.__setattr__(self, 'kind', kind)
        if type(self.contract) is not str:
            raise TypeError('manifest prerequisite contract must be text.')
        contract = _text(self.contract, name='manifest prerequisite contract')
        if contract != self._EXPECTED_CONTRACT:
            raise ValueError('manifest prerequisite contract is invalid.')
        object.__setattr__(self, 'contract', contract)
        if type(self.manifest) is not ProviderRepoArtifactRefV1:
            raise TypeError('prerequisite manifest has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls: type[_ProviderKubernetesManifestPrerequisiteSpecT],
                   value: Any) -> _ProviderKubernetesManifestPrerequisiteSpecT:
        raw = _closed_object_shallow(
            value,
            name=f'{cls._EXPECTED_KIND.value} prerequisite spec',
            keys=cls._KEYS)
        return cls(kind=raw['kind'],
                   contract=raw['contract'],
                   manifest=ProviderRepoArtifactRefV1.from_value(
                       raw['manifest']))

    def canonical_value(self) -> JsonObject:
        return {
            'kind': self.kind.value,
            'contract': self.contract,
            'manifest': self.manifest.canonical_value(),
        }


class ProviderKubernetesNetworkPolicyPrerequisiteSpecV1(
        _ProviderKubernetesManifestPrerequisiteSpecV1):
    """Content-addressed NetworkPolicy prerequisite."""

    _EXPECTED_KIND = ProviderKubernetesPrerequisiteKindV1.NETWORK_POLICY
    _EXPECTED_CONTRACT = 'serve_action_network_policy_v1'


class ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1(
        _ProviderKubernetesManifestPrerequisiteSpecV1):
    """Content-addressed ValidatingAdmissionPolicy prerequisite."""

    _EXPECTED_KIND = (
        ProviderKubernetesPrerequisiteKindV1.VALIDATING_ADMISSION_POLICY)
    _EXPECTED_CONTRACT = 'serve_action_validating_policy_v1'


class ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1(
        _ProviderKubernetesManifestPrerequisiteSpecV1):
    """Content-addressed ValidatingAdmissionPolicyBinding prerequisite."""

    _EXPECTED_KIND = (ProviderKubernetesPrerequisiteKindV1.
                      VALIDATING_ADMISSION_POLICY_BINDING)
    _EXPECTED_CONTRACT = 'serve_action_validating_binding_v1'


ProviderKubernetesPrerequisiteSpecV1 = (
    ProviderKubernetesNamespacePrerequisiteSpecV1 |
    ProviderKubernetesServiceAccountPrerequisiteSpecV1 |
    ProviderKubernetesNetworkPolicyPrerequisiteSpecV1 |
    ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1 |
    ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1)


def _provider_kubernetes_prerequisite_spec_from_value(
        value: Any) -> ProviderKubernetesPrerequisiteSpecV1:
    if type(value) is not dict:
        raise TypeError('Kubernetes prerequisite spec must be an object.')
    if 'kind' not in value:
        raise ValueError('Kubernetes prerequisite spec is missing kind.')
    if type(value['kind']) is not str:
        raise TypeError('Kubernetes prerequisite spec kind must be text.')
    kind = _enum_value(ProviderKubernetesPrerequisiteKindV1,
                       value['kind'],
                       name='Kubernetes prerequisite spec kind')
    if kind is ProviderKubernetesPrerequisiteKindV1.NAMESPACE:
        return ProviderKubernetesNamespacePrerequisiteSpecV1.from_value(value)
    if kind is ProviderKubernetesPrerequisiteKindV1.SERVICE_ACCOUNT:
        return ProviderKubernetesServiceAccountPrerequisiteSpecV1.from_value(
            value)
    if kind is ProviderKubernetesPrerequisiteKindV1.NETWORK_POLICY:
        return ProviderKubernetesNetworkPolicyPrerequisiteSpecV1.from_value(
            value)
    if kind is ProviderKubernetesPrerequisiteKindV1.VALIDATING_ADMISSION_POLICY:
        return ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1.from_value(
            value)
    if kind is (ProviderKubernetesPrerequisiteKindV1.
                VALIDATING_ADMISSION_POLICY_BINDING):
        return ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1.from_value(
            value)
    raise AssertionError(f'unhandled Kubernetes prerequisite kind: {kind!r}')


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesPrerequisiteV1(_CanonicalContract):
    """Pure typed identity and content commitment for one prerequisite."""

    role: ProviderKubernetesPrerequisiteRoleV1
    api_version: str
    kind: ProviderKubernetesPrerequisiteKindV1
    namespace: str | None
    name: str
    uid: str
    resource_version: str
    deletion_timestamp: None
    spec: ProviderKubernetesPrerequisiteSpecV1
    spec_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'api_version', 'kind', 'namespace', 'name', 'uid',
        'resource_version', 'deletion_timestamp', 'spec', 'spec_sha256'
    })
    _SPEC_TYPES: ClassVar[tuple[type[Any], ...]] = (
        ProviderKubernetesNamespacePrerequisiteSpecV1,
        ProviderKubernetesServiceAccountPrerequisiteSpecV1,
        ProviderKubernetesNetworkPolicyPrerequisiteSpecV1,
        ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1,
        ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1,
    )

    def __post_init__(self) -> None:
        if type(self.role) not in (str, ProviderKubernetesPrerequisiteRoleV1):
            raise TypeError('Kubernetes prerequisite role must be text.')
        role = _enum_value(ProviderKubernetesPrerequisiteRoleV1,
                           self.role,
                           name='Kubernetes prerequisite role')
        object.__setattr__(self, 'role', role)
        if type(self.kind) not in (str, ProviderKubernetesPrerequisiteKindV1):
            raise TypeError('Kubernetes prerequisite kind must be text.')
        kind = _enum_value(ProviderKubernetesPrerequisiteKindV1,
                           self.kind,
                           name='Kubernetes prerequisite kind')
        object.__setattr__(self, 'kind', kind)
        if kind is not _PROVIDER_KUBERNETES_PREREQUISITE_ROLE_DISPATCH_V1[
                role].kind:
            raise ValueError('Kubernetes prerequisite kind does not match its '
                             'semantic role.')
        dispatch = PROVIDER_KUBERNETES_PREREQUISITE_KIND_MAP_V1[kind]
        if type(self.api_version) is not str:
            raise TypeError('Kubernetes prerequisite api_version must be text.')
        api_version = _text(self.api_version,
                            name='Kubernetes prerequisite api_version')
        if api_version != dispatch.api_version:
            raise ValueError('Kubernetes prerequisite API version does not '
                             'match its kind.')
        object.__setattr__(self, 'api_version', api_version)
        if self.namespace is not None and type(self.namespace) is not str:
            raise TypeError('Kubernetes prerequisite namespace must be text '
                            'or null.')
        namespace = _optional_text(self.namespace,
                                   name='Kubernetes prerequisite namespace',
                                   maximum_bytes=_MAX_SHORT_TEXT_BYTES)
        if ((dispatch.scope == 'cluster' and namespace is not None) or
            (dispatch.scope == 'namespaced' and namespace is None)):
            raise ValueError('Kubernetes prerequisite namespace does not match '
                             'its kind scope.')
        object.__setattr__(self, 'namespace', namespace)
        for field in ('name', 'uid', 'resource_version'):
            if type(getattr(self, field)) is not str:
                raise TypeError(f'Kubernetes prerequisite {field} must be '
                                'text.')
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'Kubernetes prerequisite {field}'))
        if self.deletion_timestamp is not None:
            raise ValueError('Kubernetes prerequisite deletion_timestamp must '
                             'be null.')
        if type(self.spec) not in self._SPEC_TYPES:
            raise TypeError('Kubernetes prerequisite spec has an invalid type.')
        if self.spec.kind is not kind:
            raise ValueError('Kubernetes prerequisite outer and spec kinds do '
                             'not match.')
        if type(self.spec_sha256) is not str:
            raise TypeError('Kubernetes prerequisite spec_sha256 must be text.')
        spec_sha256 = _sha256(self.spec_sha256,
                              name='Kubernetes prerequisite spec_sha256')
        if spec_sha256 != self.spec.sha256:
            raise ValueError(
                'Kubernetes prerequisite spec hash does not match.')
        object.__setattr__(self, 'spec_sha256', spec_sha256)
        if isinstance(self.spec,
                      ProviderKubernetesServiceAccountPrerequisiteSpecV1):
            projection = self.spec.projection
            if (namespace != projection.namespace or
                    self.name != projection.name or
                    self.uid != projection.uid or
                    self.resource_version != projection.resource_version):
                raise ValueError('ServiceAccount prerequisite outer identity '
                                 'does not match its projection.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesPrerequisiteV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes prerequisite',
                                     keys=cls._KEYS)
        return cls(role=raw['role'],
                   api_version=raw['api_version'],
                   kind=raw['kind'],
                   namespace=raw['namespace'],
                   name=raw['name'],
                   uid=raw['uid'],
                   resource_version=raw['resource_version'],
                   deletion_timestamp=raw['deletion_timestamp'],
                   spec=_provider_kubernetes_prerequisite_spec_from_value(
                       raw['spec']),
                   spec_sha256=raw['spec_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'role': self.role.value,
            'api_version': self.api_version,
            'kind': self.kind.value,
            'namespace': self.namespace,
            'name': self.name,
            'uid': self.uid,
            'resource_version': self.resource_version,
            'deletion_timestamp': None,
            'spec': self.spec.canonical_value(),
            'spec_sha256': self.spec_sha256,
        }


def _provider_kubernetes_prerequisite_inventory_tuple(
        value: Any, *,
        name: str) -> tuple[ProviderKubernetesPrerequisiteV1, ...]:
    """Validate the exact bare 12-role prerequisite inventory."""

    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    expected_count = len(PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1)
    if len(value) != expected_count:
        raise ValueError(f'{name} must contain exactly {expected_count} '
                         'prerequisites.')
    if any(
            type(item) is not ProviderKubernetesPrerequisiteV1
            for item in value):
        raise ValueError(f'{name} must contain exact typed prerequisites.')
    expected_roles = tuple(
        entry.role for entry in PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1)
    if tuple(item.role for item in value) != expected_roles:
        raise ValueError(f'{name} does not match the exact role-map order.')

    authority_release = value[0]
    for alias in (value[3], value[4]):
        authority_value = authority_release.canonical_value()
        alias_value = alias.canonical_value()
        del authority_value['role']
        del alias_value['role']
        if canonical_json_bytes(authority_value) != canonical_json_bytes(
                alias_value):
            raise ValueError(f'{name} required Namespace aliases must be '
                             'byte-equal after omitting only role.')

    # The two LB Namespace roles above are the only aliases.  Collapse those
    # two duplicate semantic roles and require every remaining live key and UID
    # to be unique, including across prerequisite kinds.
    nonaliased = (value[0], value[1], value[2], *value[5:])
    live_keys = tuple((item.api_version, item.kind, item.namespace, item.name)
                      for item in nonaliased)
    live_uids = tuple(item.uid for item in nonaliased)
    if (len(set(live_keys)) != len(live_keys) or
            len(set(live_uids)) != len(live_uids)):
        raise ValueError(f'{name} nonaliased prerequisites must have distinct '
                         'live keys and UIDs.')
    return value


def _provider_kubernetes_prerequisite_inventory_from_value(
        value: Any, *,
        name: str) -> tuple[ProviderKubernetesPrerequisiteV1, ...]:
    """Parse the bare inventory after a bounded raw-list cardinality check."""

    if type(value) is not list:
        raise TypeError(f'{name} must be a list.')
    expected_count = len(PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1)
    if len(value) != expected_count:
        raise ValueError(f'{name} must contain exactly {expected_count} '
                         'prerequisites.')
    return _provider_kubernetes_prerequisite_inventory_tuple(tuple(
        ProviderKubernetesPrerequisiteV1.from_value(item) for item in value),
                                                             name=name)


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
        if type(self.extra_keys) is not tuple:
            raise TypeError('self identity extra_keys must be a tuple.')
        if len(self.extra_keys) != 0:
            raise ValueError('self identity extra_keys must be empty.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesSelfIdentityV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesResourceRuleV1:
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
        if type(self.urls) is not tuple or type(self.verbs) is not tuple:
            raise TypeError('nonresource rule fields must be tuples.')
        if self.urls != ('/version',) or self.verbs != ('get',):
            raise ValueError('nonresource rule must be exactly GET /version.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesNonResourceRuleV1:
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
        if (type(self.resource_rules) is not tuple or
                not 1 <= len(self.resource_rules) <= _MAX_LIST_ITEMS or any(
                    type(rule) is not ProviderKubernetesResourceRuleV1
                    for rule in self.resource_rules)):
            raise ValueError('rules review resource_rules must contain 1..256 '
                             'typed rules.')
        rule_bytes = tuple(rule.canonical_bytes for rule in self.resource_rules)
        if rule_bytes != tuple(sorted(set(rule_bytes))):
            raise ValueError('rules review resource_rules must be sorted and '
                             'duplicate-free by canonical bytes.')
        if (type(self.non_resource_rules) is not tuple or
                len(self.non_resource_rules) != 1 or
                type(self.non_resource_rules[0])
                is not ProviderKubernetesNonResourceRuleV1):
            raise ValueError('rules review requires exactly one typed '
                             'nonresource rule.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRulesReviewV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesResourceAccessV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesNonResourceAccessV1:
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
             type(self.resource) is not ProviderKubernetesResourceAccessV1) or
            (self.non_resource is not None and type(self.non_resource)
             is not ProviderKubernetesNonResourceAccessV1)):
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
    def from_value(cls, value: Any) -> ProviderKubernetesAccessDecisionV1:
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
        if type(self.identity) is not ProviderKubernetesSelfIdentityV1:
            raise TypeError('authorization identity has an invalid type.')
        if type(self.rules) is not ProviderKubernetesRulesReviewV1:
            raise TypeError('authorization rules have an invalid type.')
        object.__setattr__(
            self, 'rules_sha256',
            _sha256(self.rules_sha256, name='authorization.rules_sha256'))
        if self.rules_sha256 != self.rules.sha256:
            raise ValueError('authorization rules hash does not match its '
                             'embedded preimage.')
        if type(self.access_matrix_contract) is not ProviderRepoArtifactRefV1:
            raise TypeError('authorization access-matrix contract has an '
                            'invalid type.')
        if (type(self.access_decisions) is not tuple or
                not 1 <= len(self.access_decisions) <= _MAX_LIST_ITEMS or any(
                    type(decision) is not ProviderKubernetesAccessDecisionV1
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
                   value: Any) -> ProviderKubernetesAuthorizationEvidenceV1:
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
        if type(self.caller
               ) is not ProviderKubernetesServiceAccountProjectionV1:
            raise TypeError('caller service account has an invalid type.')
        if type(self.workload
               ) is not ProviderKubernetesServiceAccountProjectionV1:
            raise TypeError('workload service account has an invalid type.')
        if type(self.caller_authorization
               ) is not ProviderKubernetesAuthorizationEvidenceV1:
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
    def from_value(cls, value: Any) -> ProviderKubernetesPrincipalsV1:
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
        if (self.accelerator is not None and
                type(self.accelerator) is not ProviderAcceleratorV1):
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
        if (type(self.labels) is not tuple or
                len(self.labels) > _MAX_LIST_ITEMS or any(
                    type(label) is not ProviderLabelV1
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
    def from_value(cls, value: Any) -> ProviderPodResourceSnapshotV1:
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
class ProviderLaunchContentSourceV1(_CanonicalContract):
    """Content-addressed nonsecret source of a prepared Serve launch."""

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
        if type(self.store) is not str:
            raise TypeError('launch.source.store must be text.')
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
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchContentSourceV1:
        _bounded_canonical_json_bytes(value,
                                      name='launch content source',
                                      require_object=True)
        raw = _closed_object_shallow(value,
                                     name='launch content source',
                                     keys=cls._KEYS)
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
class ProviderLaunchSourceV1(_CanonicalContract):
    """Content source bound to one server-effective identity proof."""

    content: ProviderLaunchContentSourceV1
    identity_canonicalization: ProviderLaunchIdentityCanonicalizationProofV1
    identity_canonicalization_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'content', 'identity_canonicalization',
        'identity_canonicalization_sha256'
    })

    def __post_init__(self) -> None:
        if type(self.content) is not ProviderLaunchContentSourceV1:
            raise TypeError('launch source content has an invalid type.')
        if type(self.identity_canonicalization
               ) is not ProviderLaunchIdentityCanonicalizationProofV1:
            raise TypeError('launch source identity canonicalization has an '
                            'invalid type.')
        proof_sha256 = _sha256(
            self.identity_canonicalization_sha256,
            name='launch.source.identity_canonicalization_sha256')
        object.__setattr__(self, 'identity_canonicalization_sha256',
                           proof_sha256)
        if proof_sha256 != self.identity_canonicalization.sha256:
            raise ValueError('launch source identity canonicalization hash '
                             'does not match.')
        proof_input = self.identity_canonicalization.context.input
        if proof_input.service_name != self.content.service_name:
            raise ValueError('launch source service name does not match its '
                             'identity proof.')
        if (proof_input.resource_identity.service_incarnation
                != self.content.service_incarnation):
            raise ValueError('launch source service incarnation does not '
                             'match its identity proof.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchSourceV1:
        _bounded_canonical_json_bytes(value,
                                      name='launch source',
                                      require_object=True)
        raw = _closed_object_shallow(value,
                                     name='launch source',
                                     keys=cls._KEYS)
        return cls(content=ProviderLaunchContentSourceV1.from_value(
            raw['content']),
                   identity_canonicalization=(
                       ProviderLaunchIdentityCanonicalizationProofV1.from_value(
                           raw['identity_canonicalization'])),
                   identity_canonicalization_sha256=raw[
                       'identity_canonicalization_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'content': self.content.canonical_value(),
            'identity_canonicalization':
                self.identity_canonicalization.canonical_value(),
            'identity_canonicalization_sha256':
                self.identity_canonicalization_sha256,
        }


def project_provider_launch_source_v1(
    content: ProviderLaunchContentSourceV1,
    identity_canonicalization: ProviderLaunchIdentityCanonicalizationProofV1,
) -> ProviderLaunchSourceV1:
    """Bind retained content to its sole server-effective identity proof."""

    if type(content) is not ProviderLaunchContentSourceV1:
        raise TypeError('launch source content has an invalid type.')
    if type(identity_canonicalization
           ) is not ProviderLaunchIdentityCanonicalizationProofV1:
        raise TypeError('launch source identity canonicalization has an '
                        'invalid type.')
    return ProviderLaunchSourceV1(
        content=content,
        identity_canonicalization=identity_canonicalization,
        identity_canonicalization_sha256=identity_canonicalization.sha256)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesObjectPlanV1(_CanonicalContract):
    """One exact bounded CoreV1 object plan without artifact execution."""

    sequence: int
    role: ProviderObjectRoleV1
    api_version: str
    kind: ProviderPodTopologyMutableObjectKindV1
    namespace: str
    name: str
    required_identity_labels: tuple[ProviderLabelV1, ...]
    request_body: CanonicalJsonObject
    request_body_sha256: str
    requested_semantic: CanonicalJsonObject
    requested_semantic_sha256: str
    comparison_contract: str
    normalization_profile: ProviderRepoArtifactRefV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'sequence', 'role', 'api_version', 'kind', 'namespace', 'name',
        'required_identity_labels', 'request_body', 'request_body_sha256',
        'requested_semantic', 'requested_semantic_sha256',
        'comparison_contract', 'normalization_profile'
    })
    _DISPLAY_LABEL: ClassVar[str] = 'skypilot-cluster-name'
    _CLUSTER_UUID_LABEL: ClassVar[str] = 'skypilot.co/cluster-record-uuid'
    _REPLICA_UUID_LABEL: ClassVar[str] = (
        'skypilot.co/serve-replica-incarnation')
    _REQUIRED_LABEL_KEYS: ClassVar[tuple[str, ...]] = (
        _DISPLAY_LABEL,
        _CLUSTER_UUID_LABEL,
        _REPLICA_UUID_LABEL,
    )

    def __post_init__(self) -> None:
        sequence = _nonnegative_integer(self.sequence,
                                        name='object_plan.sequence')
        role_entry = _PROVIDER_KUBERNETES_OBJECT_ROLE_BY_SEQUENCE_V1.get(
            sequence)
        if role_entry is None:
            raise ValueError('object plan sequence is unsupported.')
        role = _enum_value(ProviderObjectRoleV1,
                           self.role,
                           name='object_plan.role')
        kind = _enum_value(ProviderPodTopologyMutableObjectKindV1,
                           self.kind,
                           name='object_plan.kind')
        if role is not role_entry.role or kind is not role_entry.kind:
            raise ValueError(
                'object plan sequence, role, and kind do not match.')
        object.__setattr__(self, 'sequence', sequence)
        object.__setattr__(self, 'role', role)
        object.__setattr__(self, 'kind', kind)
        if self.api_version != 'v1':
            raise ValueError('object plan api_version must be v1.')
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='object_plan.namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(self, 'name',
                           _dns_label(self.name, name='object_plan.name'))
        labels = _provider_label_tuple(
            self.required_identity_labels,
            name='object plan required identity labels')
        label_keys = tuple(label.key for label in labels)
        if label_keys != self._REQUIRED_LABEL_KEYS:
            raise ValueError('object plan requires the exact three sorted '
                             'identity label keys.')
        label_values = {label.key: label.value for label in labels}
        for key in (self._CLUSTER_UUID_LABEL, self._REPLICA_UUID_LABEL):
            _uuid(label_values[key], name=f'object plan label {key}')
        name_suffix = ('-head-ssh' if role
                       is ProviderObjectRoleV1.HEAD_SSH_SERVICE else '-head')
        if (not self.name.endswith(name_suffix) or
                len(self.name) == len(name_suffix)):
            raise ValueError('object plan name does not match its role suffix.')
        provider_cluster_name = self.name[:-len(name_suffix)]
        if label_values[self._DISPLAY_LABEL] != provider_cluster_name:
            raise ValueError(
                'object plan display identity label does not match '
                'its generated name.')
        object.__setattr__(self, 'required_identity_labels', labels)
        if type(self.request_body) is not CanonicalJsonObject:
            raise TypeError('object plan request_body has an invalid type.')
        if type(self.requested_semantic) is not CanonicalJsonObject:
            raise TypeError(
                'object plan requested_semantic has an invalid type.')
        request_body_sha256 = _sha256(self.request_body_sha256,
                                      name='object_plan.request_body_sha256')
        if request_body_sha256 != self.request_body.sha256:
            raise ValueError('object plan request body hash does not match.')
        object.__setattr__(self, 'request_body_sha256', request_body_sha256)
        requested_semantic_sha256 = _sha256(
            self.requested_semantic_sha256,
            name='object_plan.requested_semantic_sha256')
        if requested_semantic_sha256 != self.requested_semantic.sha256:
            raise ValueError('object plan requested semantic hash does not '
                             'match.')
        object.__setattr__(self, 'requested_semantic_sha256',
                           requested_semantic_sha256)
        if self.comparison_contract != 'kubernetes_admitted_object_v1':
            raise ValueError('object plan comparison contract is unsupported.')
        if type(self.normalization_profile) is not ProviderRepoArtifactRefV1:
            raise TypeError('object plan normalization profile has an invalid '
                            'type.')
        self._validate_request_body(label_values)
        _ = self.canonical_bytes

    def _validate_request_body(self, required_labels: Mapping[str,
                                                              str]) -> None:
        body = self.request_body.canonical_value()
        try:
            metadata, body_labels = (
                _validate_provider_kubernetes_body_metadata_v1(self.role, body))
        except (TypeError, ValueError) as error:
            raise ValueError(f'object plan request body: {error}') from error
        if (metadata.get('namespace') != self.namespace or
                metadata.get('name') != self.name):
            raise ValueError('object plan request body metadata identity does '
                             'not match.')
        if any(
                body_labels.get(key) != value
                for key, value in required_labels.items()):
            raise ValueError('object plan request body is missing a required '
                             'identity label.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesObjectPlanV1:
        shallow = _closed_object_shallow(value,
                                         name='Kubernetes object plan',
                                         keys=cls._KEYS)
        request_body = CanonicalJsonObject.from_value(shallow['request_body'])
        requested_semantic = CanonicalJsonObject.from_value(
            shallow['requested_semantic'])
        raw = _closed_object(shallow,
                             name='Kubernetes object plan',
                             keys=cls._KEYS)
        labels = raw['required_identity_labels']
        if not isinstance(labels, list):
            raise TypeError('object plan required_identity_labels must be a '
                            'list.')
        return cls(sequence=raw['sequence'],
                   role=raw['role'],
                   api_version=raw['api_version'],
                   kind=raw['kind'],
                   namespace=raw['namespace'],
                   name=raw['name'],
                   required_identity_labels=tuple(
                       ProviderLabelV1.from_value(label) for label in labels),
                   request_body=request_body,
                   request_body_sha256=raw['request_body_sha256'],
                   requested_semantic=requested_semantic,
                   requested_semantic_sha256=raw['requested_semantic_sha256'],
                   comparison_contract=raw['comparison_contract'],
                   normalization_profile=ProviderRepoArtifactRefV1.from_value(
                       raw['normalization_profile']))

    def canonical_value(self) -> JsonObject:
        return {
            'sequence': self.sequence,
            'role': self.role.value,
            'api_version': 'v1',
            'kind': self.kind.value,
            'namespace': self.namespace,
            'name': self.name,
            'required_identity_labels': [
                label.canonical_value()
                for label in self.required_identity_labels
            ],
            'request_body': self.request_body.canonical_value(),
            'request_body_sha256': self.request_body_sha256,
            'requested_semantic': self.requested_semantic.canonical_value(),
            'requested_semantic_sha256': self.requested_semantic_sha256,
            'comparison_contract': 'kubernetes_admitted_object_v1',
            'normalization_profile':
                self.normalization_profile.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderWorkspaceIdentityV1(_CanonicalContract):
    """Complete bounded workspace and Kubernetes scope identity."""

    version: int
    workspace: str
    kubernetes_scope: ProviderKubernetesScopeV1

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'workspace', 'kubernetes_scope'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='workspace identity version')
        object.__setattr__(
            self, 'workspace',
            _text(self.workspace, name='workspace_identity.workspace'))
        if type(self.kubernetes_scope) is not ProviderKubernetesScopeV1:
            raise TypeError('workspace Kubernetes scope has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderWorkspaceIdentityV1:
        shallow = _closed_object_shallow(value,
                                         name='workspace identity',
                                         keys=cls._KEYS)
        return cls(version=shallow['version'],
                   workspace=shallow['workspace'],
                   kubernetes_scope=ProviderKubernetesScopeV1.from_value(
                       shallow['kubernetes_scope']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'workspace': self.workspace,
            'kubernetes_scope': self.kubernetes_scope.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesCleanupObjectV1(_CanonicalContract):
    """One immutable plan plus its optional write-once launch commitment."""

    sequence: int
    role: ProviderObjectRoleV1
    plan: ProviderKubernetesObjectPlanV1
    committed_uid: str | None
    committed_server_allocations: tuple[ProviderKubernetesServerAllocationV1,
                                        ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'sequence', 'role', 'plan', 'committed_uid',
        'committed_server_allocations'
    })

    def __post_init__(self) -> None:
        sequence = _nonnegative_integer(self.sequence,
                                        name='cleanup_object.sequence')
        role_entry = _PROVIDER_KUBERNETES_OBJECT_ROLE_BY_SEQUENCE_V1.get(
            sequence)
        if role_entry is None:
            raise ValueError('cleanup object sequence is unsupported.')
        role = _enum_value(ProviderObjectRoleV1,
                           self.role,
                           name='cleanup_object.role')
        if role is not role_entry.role:
            raise ValueError('cleanup object sequence and role do not match.')
        if type(self.plan) is not ProviderKubernetesObjectPlanV1:
            raise TypeError('cleanup object plan has an invalid type.')
        if type(self.committed_server_allocations) is not tuple:
            raise TypeError('cleanup object committed allocations must be '
                            'a tuple.')
        if (self.plan.sequence != sequence or self.plan.role is not role or
                self.plan.kind is not role_entry.kind):
            raise ValueError('cleanup object does not match its embedded plan.')
        committed_uid = _optional_text(self.committed_uid,
                                       name='cleanup_object.committed_uid')
        allocations: tuple[ProviderKubernetesServerAllocationV1, ...]
        if committed_uid is None:
            if len(self.committed_server_allocations) != 0:
                raise ValueError('cleanup object without a committed UID '
                                 'cannot retain server allocations.')
            allocations = self.committed_server_allocations
        else:
            allocations = _validate_provider_kubernetes_role_allocations_v1(
                role,
                self.committed_server_allocations,
                name='cleanup_object.committed_server_allocations')
        object.__setattr__(self, 'sequence', sequence)
        object.__setattr__(self, 'role', role)
        object.__setattr__(self, 'committed_uid', committed_uid)
        object.__setattr__(self, 'committed_server_allocations', allocations)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesCleanupObjectV1:
        shallow = _closed_object_shallow(value,
                                         name='Kubernetes cleanup object',
                                         keys=cls._KEYS)
        plan = ProviderKubernetesObjectPlanV1.from_value(shallow['plan'])
        raw_allocations = shallow['committed_server_allocations']
        if not isinstance(raw_allocations, list):
            raise TypeError('cleanup object committed_server_allocations must '
                            'be a list.')
        role = _enum_value(ProviderObjectRoleV1,
                           shallow['role'],
                           name='cleanup_object.role')
        committed_uid = _optional_text(shallow['committed_uid'],
                                       name='cleanup_object.committed_uid')
        if committed_uid is None:
            valid_length = len(raw_allocations) == 0
        elif role in (ProviderObjectRoleV1.HEAD_SSH_SERVICE,
                      ProviderObjectRoleV1.HEAD_SERVICE):
            valid_length = len(raw_allocations) == len(
                _PROVIDER_KUBERNETES_SERVICE_ALLOCATION_POINTERS_V1)
        else:
            valid_length = len(raw_allocations) in (0, 1)
        if not valid_length:
            if committed_uid is None:
                raise ValueError('cleanup object without a committed UID '
                                 'cannot retain server allocations.')
            raise ValueError('cleanup object committed allocations has '
                             'invalid role-specific cardinality.')
        allocations = tuple(
            ProviderKubernetesServerAllocationV1.from_value(allocation)
            for allocation in raw_allocations)
        return cls(sequence=shallow['sequence'],
                   role=role,
                   plan=plan,
                   committed_uid=committed_uid,
                   committed_server_allocations=allocations)

    def canonical_value(self) -> JsonObject:
        return {
            'sequence': self.sequence,
            'role': self.role.value,
            'plan': self.plan.canonical_value(),
            'committed_uid': self.committed_uid,
            'committed_server_allocations': [
                allocation.canonical_value()
                for allocation in self.committed_server_allocations
            ],
        }

    @property
    def delete_sequence(self) -> int:
        """Return emission order without changing canonical plan order."""

        return _PROVIDER_KUBERNETES_OBJECT_ROLE_BY_SEQUENCE_V1[
            self.sequence].delete_sequence

    @property
    def has_complete_allocations(self) -> bool:
        return (self.committed_uid is not None and
                (self.role is not ProviderObjectRoleV1.HEAD_POD or
                 bool(self.committed_server_allocations)))


class ProviderKubernetesCleanupBasisKindV1(str, enum.Enum):
    """Closed cleanup-address source variants."""

    COMPLETED_LAUNCH = 'completed_launch'
    PARTIAL_LAUNCH_CLEANUP = 'partial_launch_cleanup'


class ProviderKubernetesClusterRowDispositionV1(str, enum.Enum):
    """Exact same-UUID cluster-row read disposition."""

    EXACT_HANDLE = 'exact_handle'
    NOT_FOUND = 'not_found'


@dataclasses.dataclass(frozen=True)
class ProviderPartialLaunchCleanupLegalShapeV1(_CanonicalContract):
    """One mechanically enumerable partial-launch cleanup admission shape."""

    case_id: str
    launch_phase: str
    committed_object_count: int
    pod_node_allocation: bool
    cluster_row_disposition: ProviderKubernetesClusterRowDispositionV1

    _POST_HANDLE_PHASES: ClassVar[frozenset[str]] = frozenset({
        'HANDLE_COMMITTED', 'RUNTIME_READY', 'JOB_INTENT', 'JOB_COMMITTED',
        'JOB_RUNNING', 'ENDPOINT_RESOLVED'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'case_id',
            _text(self.case_id,
                  name='partial cleanup legal shape case_id',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if type(self.launch_phase) is not str:
            raise TypeError('partial cleanup legal shape launch_phase must be '
                            'text.')
        count = _nonnegative_integer(
            self.committed_object_count,
            name='partial cleanup legal shape committed_object_count')
        if count > len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1):
            raise ValueError('partial cleanup legal shape has too many '
                             'committed objects.')
        object.__setattr__(self, 'committed_object_count', count)
        node = _boolean(self.pod_node_allocation,
                        name='partial cleanup legal shape pod_node_allocation')
        disposition = _enum_value(
            ProviderKubernetesClusterRowDispositionV1,
            self.cluster_row_disposition,
            name='partial cleanup legal shape cluster_row_disposition')
        object.__setattr__(self, 'cluster_row_disposition', disposition)

        phase = self.launch_phase
        if phase == 'CREATE_INTENT':
            legal = count in (0, 1, 2) and not node and disposition is (
                ProviderKubernetesClusterRowDispositionV1.NOT_FOUND)
        elif phase == 'OBJECTS_PARTIAL':
            legal = count in (1, 2, 3) and not node and disposition is (
                ProviderKubernetesClusterRowDispositionV1.NOT_FOUND)
        elif phase in ('OBJECTS_EXACT', 'HANDLE_INTENT'):
            legal = count == 3 and node and disposition is (
                ProviderKubernetesClusterRowDispositionV1.NOT_FOUND)
        elif phase in self._POST_HANDLE_PHASES:
            legal = count == 3 and node
        else:
            legal = False
        if not legal:
            raise ValueError('partial cleanup legal shape is not in the '
                             'literal v1 phase/disposition graph.')
        _ = self.canonical_bytes

    def canonical_value(self) -> JsonObject:
        return {
            'case_id': self.case_id,
            'launch_phase': self.launch_phase,
            'committed_object_count': self.committed_object_count,
            'pod_node_allocation': self.pod_node_allocation,
            'cluster_row_disposition': self.cluster_row_disposition.value,
        }


PROVIDER_PARTIAL_LAUNCH_CLEANUP_LEGAL_SHAPE_MANIFEST_V1 = (
    ProviderPartialLaunchCleanupLegalShapeV1(
        'create_intent_0_not_found', 'CREATE_INTENT', 0, False,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'objects_partial_1_not_found', 'OBJECTS_PARTIAL', 1, False,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'create_intent_1_not_found', 'CREATE_INTENT', 1, False,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'objects_partial_2_not_found', 'OBJECTS_PARTIAL', 2, False,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'create_intent_2_not_found', 'CREATE_INTENT', 2, False,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'objects_partial_3_unscheduled_not_found', 'OBJECTS_PARTIAL', 3, False,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'objects_exact_not_found', 'OBJECTS_EXACT', 3, True,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    ProviderPartialLaunchCleanupLegalShapeV1(
        'handle_intent_not_found', 'HANDLE_INTENT', 3, True,
        ProviderKubernetesClusterRowDispositionV1.NOT_FOUND),
    *tuple(
        ProviderPartialLaunchCleanupLegalShapeV1(
            f'{phase.lower()}_{disposition.value}', phase, 3, True, disposition)
        for phase in (
            'HANDLE_COMMITTED',
            'RUNTIME_READY',
            'JOB_INTENT',
            'JOB_COMMITTED',
            'JOB_RUNNING',
            'ENDPOINT_RESOLVED',
        )
        for disposition in (
            ProviderKubernetesClusterRowDispositionV1.NOT_FOUND,
            ProviderKubernetesClusterRowDispositionV1.EXACT_HANDLE,
        )),
)


def enumerate_provider_partial_launch_cleanup_legal_shapes_v1(
) -> tuple[ProviderPartialLaunchCleanupLegalShapeV1, ...]:
    """Return the immutable, exhaustive v1 partial-down shape manifest."""

    return PROVIDER_PARTIAL_LAUNCH_CLEANUP_LEGAL_SHAPE_MANIFEST_V1


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesCleanupTargetV1(_CanonicalContract):
    """Complete immutable down-addressing preimage for one launch basis."""

    version: int
    basis_kind: ProviderKubernetesCleanupBasisKindV1
    requested_target_sha256: str
    cluster_name: str
    cluster_record_uuid: uuid.UUID
    objects: tuple[ProviderKubernetesCleanupObjectV1, ...]
    cluster_row_disposition: ProviderKubernetesClusterRowDispositionV1
    handle: ProviderKubernetesHandleV1 | None
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis_kind', 'requested_target_sha256', 'cluster_name',
        'cluster_record_uuid', 'objects', 'cluster_row_disposition', 'handle',
        'observed_at'
    })
    _DISPLAY_LABEL: ClassVar[str] = 'skypilot-cluster-name'
    _CLUSTER_UUID_LABEL: ClassVar[str] = 'skypilot.co/cluster-record-uuid'

    def __post_init__(self) -> None:
        _version_one(self.version, name='Kubernetes cleanup target version')
        basis_kind = _enum_value(ProviderKubernetesCleanupBasisKindV1,
                                 self.basis_kind,
                                 name='cleanup_target.basis_kind')
        disposition = _enum_value(ProviderKubernetesClusterRowDispositionV1,
                                  self.cluster_row_disposition,
                                  name='cleanup_target.cluster_row_disposition')
        object.__setattr__(self, 'basis_kind', basis_kind)
        object.__setattr__(self, 'cluster_row_disposition', disposition)
        object.__setattr__(
            self, 'requested_target_sha256',
            _sha256(self.requested_target_sha256,
                    name='cleanup_target.requested_target_sha256'))
        object.__setattr__(
            self, 'cluster_name',
            _text(self.cluster_name,
                  name='cleanup_target.cluster_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'cluster_record_uuid',
            _uuid(self.cluster_record_uuid,
                  name='cleanup_target.cluster_record_uuid'))
        self._validate_objects()
        if (self.handle is not None and
                type(self.handle) is not ProviderKubernetesHandleV1):
            raise TypeError('cleanup target handle has an invalid type.')
        if ((disposition
             is ProviderKubernetesClusterRowDispositionV1.EXACT_HANDLE)
                != (self.handle is not None)):
            raise ValueError('exact_handle requires a handle; not_found '
                             'requires null.')
        if (basis_kind is ProviderKubernetesCleanupBasisKindV1.COMPLETED_LAUNCH
                and disposition
                is not ProviderKubernetesClusterRowDispositionV1.EXACT_HANDLE):
            raise ValueError('completed cleanup target requires an exact '
                             'handle.')
        if (basis_kind is ProviderKubernetesCleanupBasisKindV1.COMPLETED_LAUNCH
                or self.handle is not None):
            self._validate_complete_commitments()
        if self.handle is not None:
            self._validate_handle()
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='cleanup_target.observed_at'))
        _ = self.canonical_bytes

    def _validate_objects(self) -> None:
        if (type(self.objects) is not tuple or len(self.objects)
                != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1) or any(
                    type(item) is not ProviderKubernetesCleanupObjectV1
                    for item in self.objects)):
            raise ValueError('cleanup target requires exactly three typed '
                             'objects.')
        expected = tuple((entry.plan_sequence, entry.role)
                         for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
        actual = tuple((item.sequence, item.role) for item in self.objects)
        if actual != expected:
            raise ValueError('cleanup target objects have invalid plan order.')
        seen_unknown = False
        for item in self.objects:
            if item.committed_uid is None:
                seen_unknown = True
            elif seen_unknown:
                raise ValueError('cleanup target UID commitments must form a '
                                 'prefix of create order.')
        plans = tuple(item.plan for item in self.objects)
        namespaces = {plan.namespace for plan in plans}
        label_maps = tuple(plan.required_identity_labels for plan in plans)
        if len(namespaces) != 1 or len(set(label_maps)) != 1:
            raise ValueError('cleanup target plans must share namespace and '
                             'identity labels.')
        ssh_plan, service_plan, pod_plan = plans
        if (service_plan.name != pod_plan.name or
                ssh_plan.name != f'{pod_plan.name}-ssh'):
            raise ValueError('cleanup target object names do not form the '
                             'canonical Pod/two-Service group.')
        identity_labels = {
            label.key: label.value
            for label in pod_plan.required_identity_labels
        }
        if identity_labels[self._CLUSTER_UUID_LABEL] != str(
                self.cluster_record_uuid):
            raise ValueError('cleanup target cluster UUID does not match its '
                             'object plans.')

    def _validate_complete_commitments(self) -> None:
        if any(item.committed_uid is None for item in self.objects):
            raise ValueError('complete cleanup target requires all three '
                             'committed UIDs.')
        for item in self.objects:
            _validate_provider_kubernetes_role_allocations_v1(
                item.role,
                item.committed_server_allocations,
                name='cleanup target committed allocations',
                require_pod_node_name=True)

    def _validate_handle(self) -> None:
        assert self.handle is not None
        handle = self.handle
        ssh_object, service_object, pod_object = self.objects
        config = handle.provider_config
        display_label = {
            label.key: label.value
            for label in pod_object.plan.required_identity_labels
        }[self._DISPLAY_LABEL]
        pod_allocations = pod_object.committed_server_allocations
        node_name = pod_allocations[0].value.canonical_value()
        if (handle.requested_target_sha256 != self.requested_target_sha256 or
                handle.cluster_name != self.cluster_name or
                handle.cluster_record_uuid != self.cluster_record_uuid or
                handle.cluster_name_on_cloud != display_label or
                config.namespace != pod_object.plan.namespace or
                config.pod_name != pod_object.plan.name or
                config.pod_uid != pod_object.committed_uid or
                config.node_name != node_name or
                config.head_service_uid != service_object.committed_uid or
                config.head_ssh_service_uid != ssh_object.committed_uid):
            raise ValueError('cleanup target exact handle conflicts with its '
                             'object commitments.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesCleanupTargetV1:
        shallow = _closed_object_shallow(value,
                                         name='Kubernetes cleanup target',
                                         keys=cls._KEYS)
        raw_objects = shallow['objects']
        if not isinstance(raw_objects, list):
            raise TypeError('cleanup target objects must be a list.')
        if len(raw_objects) != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1):
            raise ValueError('cleanup target requires exactly three objects.')
        objects = tuple(
            ProviderKubernetesCleanupObjectV1.from_value(item)
            for item in raw_objects)
        handle = (None if shallow['handle'] is None else
                  ProviderKubernetesHandleV1.from_value(shallow['handle']))
        return cls(version=shallow['version'],
                   basis_kind=shallow['basis_kind'],
                   requested_target_sha256=shallow['requested_target_sha256'],
                   cluster_name=shallow['cluster_name'],
                   cluster_record_uuid=_uuid(
                       shallow['cluster_record_uuid'],
                       name='cleanup_target.cluster_record_uuid'),
                   objects=objects,
                   cluster_row_disposition=shallow['cluster_row_disposition'],
                   handle=handle,
                   observed_at=shallow['observed_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'basis_kind': self.basis_kind.value,
            'requested_target_sha256': self.requested_target_sha256,
            'cluster_name': self.cluster_name,
            'cluster_record_uuid': str(self.cluster_record_uuid),
            'objects': [item.canonical_value() for item in self.objects],
            'cluster_row_disposition': self.cluster_row_disposition.value,
            'handle': None
                      if self.handle is None else self.handle.canonical_value(),
            'observed_at': self.observed_at,
        }

    @property
    def objects_in_delete_order(
            self) -> tuple[ProviderKubernetesCleanupObjectV1, ...]:
        """Project mutation order without changing stored plan order."""

        return tuple(sorted(self.objects,
                            key=lambda item: item.delete_sequence))

    def validate_requested_target(self, target: ProviderLocatorV1) -> None:
        if type(target) is not ProviderLocatorV1:
            raise TypeError('cleanup target requested target has an invalid '
                            'type.')
        if (self.requested_target_sha256 != target.sha256 or
                self.cluster_name != target.sky_cluster_name or
                self.cluster_record_uuid != target.sky_cluster_record_uuid):
            raise ValueError('Cleanup target does not match requested target.')
        if self.handle is not None:
            self.handle.validate_requested_target(target)


class ProviderPriorLaunchSourceStoreV1(str, enum.Enum):
    """Closed retained-row stores from which a launch basis may originate."""

    API_RESOURCE_ACTIONS = 'api_resource_actions'
    SERVE_RESOURCE_ACTION_SHADOW_SAMPLES = (
        'serve_resource_action_shadow_samples')


def _validate_prior_launch_basis_common_v1(
    *,
    source_store: ProviderPriorLaunchSourceStoreV1 | str,
    allowed_source_stores: frozenset[ProviderPriorLaunchSourceStoreV1],
    launch_action_id: uuid.UUID,
    launch_resource_identity: ProviderResourceIdentityV1,
    launch_requested_target: ProviderLocatorV1,
    launch_resources: ProviderPodResourceSnapshotV1,
    launch_workspace_identity: ProviderWorkspaceIdentityV1,
    launch_cleanup_target_sha256: str,
    launch_immutable_spec_sha256: str,
    exact_resources_override: bool,
) -> ProviderPriorLaunchSourceStoreV1:
    """Validate one standalone retained-source basis reference."""

    source = _enum_value(ProviderPriorLaunchSourceStoreV1,
                         source_store,
                         name='prior_launch_basis.source_store')
    if source not in allowed_source_stores:
        raise ValueError('prior launch basis source store is unsupported for '
                         'its basis kind.')
    if type(launch_action_id) is not uuid.UUID:
        raise TypeError('prior launch basis action ID has an invalid type.')
    for field, value, expected_type in (
        ('launch_resource_identity', launch_resource_identity,
         ProviderResourceIdentityV1),
        ('launch_requested_target', launch_requested_target, ProviderLocatorV1),
        ('launch_resources', launch_resources, ProviderPodResourceSnapshotV1),
        ('launch_workspace_identity', launch_workspace_identity,
         ProviderWorkspaceIdentityV1),
    ):
        if type(value) is not expected_type:
            raise TypeError(f'prior launch basis {field} has an invalid type.')
    expected_action_id = launch_resource_identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    if launch_action_id != expected_action_id:
        raise ValueError('prior launch basis action ID does not match its '
                         'launch resource identity.')
    if not launch_requested_target.is_authoritative_pod_locator:
        raise ValueError('prior launch basis requires the authoritative '
                         'Kubernetes Pod locator.')
    kubernetes = launch_requested_target.kubernetes
    assert kubernetes is not None
    if (kubernetes.replica_incarnation_label
            != str(launch_resource_identity.replica_incarnation)):
        raise ValueError('prior launch basis locator does not match its '
                         'replica incarnation.')
    if (launch_resources.cluster_fingerprint_sha256
            != kubernetes.cluster_fingerprint_sha256 or
            launch_resources.namespace != kubernetes.namespace):
        raise ValueError('prior launch basis resources do not match its '
                         'requested target.')
    if (launch_workspace_identity.kubernetes_scope.canonical_bytes
            != kubernetes.scope.canonical_bytes):
        raise ValueError('prior launch basis workspace scope does not match '
                         'its requested target.')
    _sha256(launch_cleanup_target_sha256,
            name='prior_launch_basis.launch_cleanup_target_sha256')
    _sha256(launch_immutable_spec_sha256,
            name='prior_launch_basis.launch_immutable_spec_sha256')
    if _boolean(exact_resources_override,
                name='prior_launch_basis.exact_resources_override') is not True:
        raise ValueError('prior launch basis exact_resources_override must be '
                         'true.')
    return source


@dataclasses.dataclass(frozen=True)
class CompletedLaunchBasisV1(_CanonicalContract):
    """Complete successful launch evidence retained by a primary down."""

    version: int
    basis_kind: ProviderKubernetesCleanupBasisKindV1
    source_store: ProviderPriorLaunchSourceStoreV1
    launch_action_id: uuid.UUID
    launch_resource_identity: ProviderResourceIdentityV1
    launch_requested_target: ProviderLocatorV1
    launch_resources: ProviderPodResourceSnapshotV1
    launch_workspace_identity: ProviderWorkspaceIdentityV1
    launch_resolved_target: ResolvedProviderTargetV1
    launch_resolved_target_sha256: str
    launch_handle: ProviderKubernetesHandleV1
    launch_handle_sha256: str
    launch_cleanup_target_sha256: str
    launch_immutable_spec_sha256: str
    exact_resources_override: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis_kind', 'source_store', 'launch_action_id',
        'launch_resource_identity', 'launch_requested_target',
        'launch_resources', 'launch_workspace_identity',
        'launch_resolved_target', 'launch_resolved_target_sha256',
        'launch_handle', 'launch_handle_sha256', 'launch_cleanup_target_sha256',
        'launch_immutable_spec_sha256', 'exact_resources_override'
    })
    _SOURCE_STORES: ClassVar[frozenset[ProviderPriorLaunchSourceStoreV1]] = (
        frozenset({
            ProviderPriorLaunchSourceStoreV1.API_RESOURCE_ACTIONS,
            ProviderPriorLaunchSourceStoreV1.
            SERVE_RESOURCE_ACTION_SHADOW_SAMPLES,
        }))

    def __post_init__(self) -> None:
        _version_one(self.version, name='completed launch basis version')
        basis_kind = _enum_value(ProviderKubernetesCleanupBasisKindV1,
                                 self.basis_kind,
                                 name='completed_launch_basis.basis_kind')
        if basis_kind is not (
                ProviderKubernetesCleanupBasisKindV1.COMPLETED_LAUNCH):
            raise ValueError('completed launch basis kind is unsupported.')
        source = _validate_prior_launch_basis_common_v1(
            source_store=self.source_store,
            allowed_source_stores=self._SOURCE_STORES,
            launch_action_id=self.launch_action_id,
            launch_resource_identity=self.launch_resource_identity,
            launch_requested_target=self.launch_requested_target,
            launch_resources=self.launch_resources,
            launch_workspace_identity=self.launch_workspace_identity,
            launch_cleanup_target_sha256=self.launch_cleanup_target_sha256,
            launch_immutable_spec_sha256=self.launch_immutable_spec_sha256,
            exact_resources_override=self.exact_resources_override)
        for field, expected_type in (
            ('launch_resolved_target', ResolvedProviderTargetV1),
            ('launch_handle', ProviderKubernetesHandleV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'completed launch basis {field} has an '
                                'invalid type.')
        resolved_hash = _sha256(
            self.launch_resolved_target_sha256,
            name='completed_launch_basis.launch_resolved_target_sha256')
        if resolved_hash != self.launch_resolved_target.sha256:
            raise ValueError('completed launch basis resolved target hash does '
                             'not match its complete preimage.')
        handle_hash = _sha256(
            self.launch_handle_sha256,
            name='completed_launch_basis.launch_handle_sha256')
        if handle_hash != self.launch_handle.sha256:
            raise ValueError('completed launch basis handle hash does not '
                             'match its complete preimage.')
        self.launch_resolved_target.validate_requested_target(
            self.launch_requested_target)
        self.launch_handle.validate_requested_target(
            self.launch_requested_target)
        if self.launch_handle.launched_resources_sha256 != (
                self.launch_resources.sha256):
            raise ValueError('completed launch basis handle resources hash '
                             'does not match launch resources.')
        handle_config = self.launch_handle.provider_config
        ssh_service, head_service, pod = (
            self.launch_resolved_target.kubernetes_objects)
        if (self.launch_resolved_target.provider_resource_id
                != f'pod/{handle_config.pod_name}' or
                self.launch_resolved_target.workload_uid
                != handle_config.pod_uid or
                handle_config.namespace != pod.namespace or
                handle_config.pod_name != pod.name or
                handle_config.pod_uid != pod.uid or
                handle_config.head_service_uid != head_service.uid or
                handle_config.head_ssh_service_uid != ssh_service.uid):
            raise ValueError('completed launch basis resolved object identity '
                             'does not match its launch handle.')
        pod_allocations = pod.server_allocations
        if (not pod_allocations or
                pod_allocations[0].json_pointer != '/spec/nodeName' or
                pod_allocations[0].value.canonical_value()
                != handle_config.node_name):
            raise ValueError('completed launch basis resolved Pod allocation '
                             'does not match its launch handle.')
        scope = self.launch_workspace_identity.kubernetes_scope
        if self.launch_handle.provider_config.scope_sha256 != scope.sha256:
            raise ValueError('completed launch basis handle scope does not '
                             'match its workspace identity.')
        object.__setattr__(self, 'basis_kind', basis_kind)
        object.__setattr__(self, 'source_store', source)
        object.__setattr__(self, 'launch_resolved_target_sha256', resolved_hash)
        object.__setattr__(self, 'launch_handle_sha256', handle_hash)
        object.__setattr__(
            self, 'launch_cleanup_target_sha256',
            _sha256(self.launch_cleanup_target_sha256,
                    name=('completed_launch_basis.'
                          'launch_cleanup_target_sha256')))
        object.__setattr__(
            self, 'launch_immutable_spec_sha256',
            _sha256(self.launch_immutable_spec_sha256,
                    name=('completed_launch_basis.'
                          'launch_immutable_spec_sha256')))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> CompletedLaunchBasisV1:
        _bounded_canonical_json_bytes(value,
                                      name='completed launch basis',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='completed launch basis',
                                     keys=cls._KEYS)
        return cls(
            version=raw['version'],
            basis_kind=raw['basis_kind'],
            source_store=raw['source_store'],
            launch_action_id=_uuid(raw['launch_action_id'],
                                   name='completed_launch_basis.action_id'),
            launch_resource_identity=ProviderResourceIdentityV1.from_value(
                raw['launch_resource_identity']),
            launch_requested_target=ProviderLocatorV1.from_value(
                raw['launch_requested_target']),
            launch_resources=ProviderPodResourceSnapshotV1.from_value(
                raw['launch_resources']),
            launch_workspace_identity=ProviderWorkspaceIdentityV1.from_value(
                raw['launch_workspace_identity']),
            launch_resolved_target=ResolvedProviderTargetV1.from_value(
                raw['launch_resolved_target']),
            launch_resolved_target_sha256=raw['launch_resolved_target_sha256'],
            launch_handle=ProviderKubernetesHandleV1.from_value(
                raw['launch_handle']),
            launch_handle_sha256=raw['launch_handle_sha256'],
            launch_cleanup_target_sha256=raw['launch_cleanup_target_sha256'],
            launch_immutable_spec_sha256=raw['launch_immutable_spec_sha256'],
            exact_resources_override=raw['exact_resources_override'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'basis_kind': self.basis_kind.value,
            'source_store': self.source_store.value,
            'launch_action_id': str(self.launch_action_id),
            'launch_resource_identity':
                self.launch_resource_identity.canonical_value(),
            'launch_requested_target':
                self.launch_requested_target.canonical_value(),
            'launch_resources': self.launch_resources.canonical_value(),
            'launch_workspace_identity':
                self.launch_workspace_identity.canonical_value(),
            'launch_resolved_target':
                self.launch_resolved_target.canonical_value(),
            'launch_resolved_target_sha256': self.launch_resolved_target_sha256,
            'launch_handle': self.launch_handle.canonical_value(),
            'launch_handle_sha256': self.launch_handle_sha256,
            'launch_cleanup_target_sha256': self.launch_cleanup_target_sha256,
            'launch_immutable_spec_sha256': self.launch_immutable_spec_sha256,
            'exact_resources_override': True,
        }


@dataclasses.dataclass(frozen=True)
class PartialLaunchCleanupBasisV1(_CanonicalContract):
    """Quiesced partial-launch evidence retained by a cleanup down."""

    version: int
    basis_kind: ProviderKubernetesCleanupBasisKindV1
    source_store: ProviderPriorLaunchSourceStoreV1
    launch_action_id: uuid.UUID
    launch_attempt: int
    launch_resource_identity: ProviderResourceIdentityV1
    launch_requested_target: ProviderLocatorV1
    launch_resources: ProviderPodResourceSnapshotV1
    launch_workspace_identity: ProviderWorkspaceIdentityV1
    launch_provider_cursor_sha256: str
    launch_provider_progress_revision: int
    launch_quiescence_sha256: str
    launch_cleanup_target_sha256: str
    launch_immutable_spec_sha256: str
    exact_resources_override: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis_kind', 'source_store', 'launch_action_id',
        'launch_attempt', 'launch_resource_identity', 'launch_requested_target',
        'launch_resources', 'launch_workspace_identity',
        'launch_provider_cursor_sha256', 'launch_provider_progress_revision',
        'launch_quiescence_sha256', 'launch_cleanup_target_sha256',
        'launch_immutable_spec_sha256', 'exact_resources_override'
    })
    _SOURCE_STORES: ClassVar[frozenset[ProviderPriorLaunchSourceStoreV1]] = (
        frozenset({ProviderPriorLaunchSourceStoreV1.API_RESOURCE_ACTIONS}))

    def __post_init__(self) -> None:
        _version_one(self.version, name='partial launch cleanup basis version')
        basis_kind = _enum_value(ProviderKubernetesCleanupBasisKindV1,
                                 self.basis_kind,
                                 name='partial_launch_cleanup_basis.basis_kind')
        if basis_kind is not (
                ProviderKubernetesCleanupBasisKindV1.PARTIAL_LAUNCH_CLEANUP):
            raise ValueError('partial launch cleanup basis kind is '
                             'unsupported.')
        source = _validate_prior_launch_basis_common_v1(
            source_store=self.source_store,
            allowed_source_stores=self._SOURCE_STORES,
            launch_action_id=self.launch_action_id,
            launch_resource_identity=self.launch_resource_identity,
            launch_requested_target=self.launch_requested_target,
            launch_resources=self.launch_resources,
            launch_workspace_identity=self.launch_workspace_identity,
            launch_cleanup_target_sha256=self.launch_cleanup_target_sha256,
            launch_immutable_spec_sha256=self.launch_immutable_spec_sha256,
            exact_resources_override=self.exact_resources_override)
        attempt = _positive_integer(
            self.launch_attempt,
            name='partial_launch_cleanup_basis.launch_attempt')
        revision = _positive_integer(self.launch_provider_progress_revision,
                                     name=('partial_launch_cleanup_basis.'
                                           'launch_provider_progress_revision'))
        cursor_hash = _sha256(self.launch_provider_cursor_sha256,
                              name=('partial_launch_cleanup_basis.'
                                    'launch_provider_cursor_sha256'))
        quiescence_hash = _sha256(
            self.launch_quiescence_sha256,
            name='partial_launch_cleanup_basis.launch_quiescence_sha256')
        object.__setattr__(self, 'basis_kind', basis_kind)
        object.__setattr__(self, 'source_store', source)
        object.__setattr__(self, 'launch_attempt', attempt)
        object.__setattr__(self, 'launch_provider_progress_revision', revision)
        object.__setattr__(self, 'launch_provider_cursor_sha256', cursor_hash)
        object.__setattr__(self, 'launch_quiescence_sha256', quiescence_hash)
        object.__setattr__(
            self, 'launch_cleanup_target_sha256',
            _sha256(self.launch_cleanup_target_sha256,
                    name=('partial_launch_cleanup_basis.'
                          'launch_cleanup_target_sha256')))
        object.__setattr__(
            self, 'launch_immutable_spec_sha256',
            _sha256(self.launch_immutable_spec_sha256,
                    name=('partial_launch_cleanup_basis.'
                          'launch_immutable_spec_sha256')))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> PartialLaunchCleanupBasisV1:
        _bounded_canonical_json_bytes(value,
                                      name='partial launch cleanup basis',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='partial launch cleanup basis',
                                     keys=cls._KEYS)
        return cls(
            version=raw['version'],
            basis_kind=raw['basis_kind'],
            source_store=raw['source_store'],
            launch_action_id=_uuid(
                raw['launch_action_id'],
                name='partial_launch_cleanup_basis.action_id'),
            launch_attempt=raw['launch_attempt'],
            launch_resource_identity=ProviderResourceIdentityV1.from_value(
                raw['launch_resource_identity']),
            launch_requested_target=ProviderLocatorV1.from_value(
                raw['launch_requested_target']),
            launch_resources=ProviderPodResourceSnapshotV1.from_value(
                raw['launch_resources']),
            launch_workspace_identity=ProviderWorkspaceIdentityV1.from_value(
                raw['launch_workspace_identity']),
            launch_provider_cursor_sha256=raw['launch_provider_cursor_sha256'],
            launch_provider_progress_revision=raw[
                'launch_provider_progress_revision'],
            launch_quiescence_sha256=raw['launch_quiescence_sha256'],
            launch_cleanup_target_sha256=raw['launch_cleanup_target_sha256'],
            launch_immutable_spec_sha256=raw['launch_immutable_spec_sha256'],
            exact_resources_override=raw['exact_resources_override'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'basis_kind': self.basis_kind.value,
            'source_store': self.source_store.value,
            'launch_action_id': str(self.launch_action_id),
            'launch_attempt': self.launch_attempt,
            'launch_resource_identity':
                self.launch_resource_identity.canonical_value(),
            'launch_requested_target':
                self.launch_requested_target.canonical_value(),
            'launch_resources': self.launch_resources.canonical_value(),
            'launch_workspace_identity':
                self.launch_workspace_identity.canonical_value(),
            'launch_provider_cursor_sha256': self.launch_provider_cursor_sha256,
            'launch_provider_progress_revision':
                self.launch_provider_progress_revision,
            'launch_quiescence_sha256': self.launch_quiescence_sha256,
            'launch_cleanup_target_sha256': self.launch_cleanup_target_sha256,
            'launch_immutable_spec_sha256': self.launch_immutable_spec_sha256,
            'exact_resources_override': True,
        }


PriorLaunchBasisV1 = CompletedLaunchBasisV1 | PartialLaunchCleanupBasisV1


def prior_launch_basis_from_value_v1(value: Any) -> PriorLaunchBasisV1:
    """Parse the exact discriminated prior-launch evidence union."""

    if type(value) is not dict:
        raise TypeError('prior launch basis must be a JSON object.')
    basis_kind = value.get('basis_kind')
    if type(basis_kind) is not str:
        raise TypeError('prior_launch_basis.basis_kind must be text.')
    if basis_kind == ProviderKubernetesCleanupBasisKindV1.COMPLETED_LAUNCH.value:
        return CompletedLaunchBasisV1.from_value(value)
    if basis_kind == (
            ProviderKubernetesCleanupBasisKindV1.PARTIAL_LAUNCH_CLEANUP.value):
        return PartialLaunchCleanupBasisV1.from_value(value)
    raise ValueError('prior launch basis kind is unsupported.')


def _validate_prior_launch_cleanup_target_binding_v1(
    prior_launch_basis: PriorLaunchBasisV1,
    cleanup_target: ProviderKubernetesCleanupTargetV1,
) -> None:
    """Bind the sole cleanup-target preimage to its retained-source basis."""

    if type(prior_launch_basis) not in (CompletedLaunchBasisV1,
                                        PartialLaunchCleanupBasisV1):
        raise TypeError('prior launch cleanup binding basis has an invalid '
                        'type.')
    if type(cleanup_target) is not ProviderKubernetesCleanupTargetV1:
        raise TypeError('prior launch cleanup binding target has an invalid '
                        'type.')
    cleanup_target.validate_requested_target(
        prior_launch_basis.launch_requested_target)
    if cleanup_target.basis_kind is not prior_launch_basis.basis_kind:
        raise ValueError('cleanup target basis kind does not match its prior '
                         'launch basis.')
    if cleanup_target.sha256 != prior_launch_basis.launch_cleanup_target_sha256:
        raise ValueError('cleanup target hash does not match its prior launch '
                         'basis commitment.')
    if type(prior_launch_basis) is CompletedLaunchBasisV1:
        if (cleanup_target.handle is None or
                cleanup_target.handle.canonical_bytes
                != prior_launch_basis.launch_handle.canonical_bytes):
            raise ValueError('completed cleanup target handle is not '
                             'byte-equal to its prior launch handle.')
        resolved_objects = (
            prior_launch_basis.launch_resolved_target.kubernetes_objects)
        for resolved, cleanup in zip(resolved_objects, cleanup_target.objects):
            plan = cleanup.plan
            if (resolved.role is not cleanup.role or
                    resolved.kind is not plan.kind or
                    resolved.namespace != plan.namespace or
                    resolved.name != plan.name or
                    resolved.uid != cleanup.committed_uid or
                    resolved.observed_semantic_sha256
                    != plan.requested_semantic_sha256 or
                    tuple(item.canonical_bytes
                          for item in resolved.server_allocations)
                    != tuple(item.canonical_bytes
                             for item in cleanup.committed_server_allocations)):
                raise ValueError('completed cleanup target object evidence is '
                                 'not byte-equal to its prior launch resolved '
                                 'target and source plan.')
        pod = cleanup_target.objects[2]
        if (prior_launch_basis.launch_resolved_target.provider_resource_id
                != f'pod/{pod.plan.name}' or
                prior_launch_basis.launch_resolved_target.workload_uid
                != pod.committed_uid):
            raise ValueError('completed cleanup target Pod identity does not '
                             'match its prior launch resolved target.')


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRendererV1(_CanonicalContract):
    """Typed renderer artifact references without resolving their content."""

    contract: str
    outer_template: ProviderRepoArtifactRefV1
    node_fragment: ProviderRepoArtifactRefV1
    binding_schema: ProviderRepoArtifactRefV1
    config_access_inventory: ProviderRepoArtifactRefV1
    admitted_object_normalization: ProviderRepoArtifactRefV1
    source: ProviderLaunchContentSourceV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'contract', 'outer_template', 'node_fragment', 'binding_schema',
        'config_access_inventory', 'admitted_object_normalization', 'source'
    })
    _ARTIFACT_FIELDS: ClassVar[tuple[str, ...]] = (
        'outer_template',
        'node_fragment',
        'binding_schema',
        'config_access_inventory',
        'admitted_object_normalization',
    )

    def __post_init__(self) -> None:
        if self.contract != 'serve_prebooted_direct_pod_v1':
            raise ValueError('Kubernetes renderer contract is unsupported.')
        for field in self._ARTIFACT_FIELDS:
            if type(getattr(self, field)) is not ProviderRepoArtifactRefV1:
                raise TypeError(f'Kubernetes renderer {field} has an invalid '
                                'type.')
        if type(self.source) is not ProviderLaunchContentSourceV1:
            raise TypeError('Kubernetes renderer source has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRendererV1:
        raw = _closed_object(value, name='Kubernetes renderer', keys=cls._KEYS)
        return cls(
            contract=raw['contract'],
            outer_template=ProviderRepoArtifactRefV1.from_value(
                raw['outer_template']),
            node_fragment=ProviderRepoArtifactRefV1.from_value(
                raw['node_fragment']),
            binding_schema=ProviderRepoArtifactRefV1.from_value(
                raw['binding_schema']),
            config_access_inventory=ProviderRepoArtifactRefV1.from_value(
                raw['config_access_inventory']),
            admitted_object_normalization=ProviderRepoArtifactRefV1.from_value(
                raw['admitted_object_normalization']),
            source=ProviderLaunchContentSourceV1.from_value(raw['source']))

    def canonical_value(self) -> JsonObject:
        return {
            'contract': 'serve_prebooted_direct_pod_v1',
            'outer_template': self.outer_template.canonical_value(),
            'node_fragment': self.node_fragment.canonical_value(),
            'binding_schema': self.binding_schema.canonical_value(),
            'config_access_inventory':
                self.config_access_inventory.canonical_value(),
            'admitted_object_normalization':
                self.admitted_object_normalization.canonical_value(),
            'source': self.source.canonical_value(),
        }


class ProviderWorkloadArtifactRoleV1(str, enum.Enum):
    """Exact runtime-artifact role order for the prebooted workload."""

    RAY_RUNTIME = 'ray_runtime'
    SKYLET_RUNTIME = 'skylet_runtime'
    SKYLET_JOB_PROTOCOL = 'skylet_job_protocol'
    SKYLET_STATE_SCHEMA = 'skylet_state_schema'
    STARTUP_PROBE = 'startup_probe'
    SERVE_CANARY_ENTRYPOINT = 'serve_canary_entrypoint'


@dataclasses.dataclass(frozen=True)
class ProviderWorkloadArtifactBindingV1(_CanonicalContract):
    """One typed content-addressed workload-runtime artifact binding."""

    role: ProviderWorkloadArtifactRoleV1
    workload_image_digest: str
    installed_root: str
    source_manifest: ProviderRepoArtifactRefV1
    image_build_attestation: ProviderRepoArtifactRefV1
    measurement_contract: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'workload_image_digest', 'installed_root', 'source_manifest',
        'image_build_attestation', 'measurement_contract'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'role',
            _enum_value(ProviderWorkloadArtifactRoleV1,
                        self.role,
                        name='workload artifact role'))
        object.__setattr__(
            self, 'workload_image_digest',
            _sha256_digest(self.workload_image_digest,
                           name='workload artifact image digest'))
        object.__setattr__(
            self, 'installed_root',
            _text(self.installed_root, name='workload artifact installed_root'))
        if type(self.source_manifest) is not ProviderRepoArtifactRefV1:
            raise TypeError('workload artifact source_manifest has an invalid '
                            'type.')
        if type(self.image_build_attestation) is not ProviderRepoArtifactRefV1:
            raise TypeError('workload artifact image_build_attestation has an '
                            'invalid type.')
        if self.measurement_contract != 'canonical_regular_file_tree_v1':
            raise ValueError('workload artifact measurement contract is '
                             'unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderWorkloadArtifactBindingV1:
        raw = _closed_object(value,
                             name='workload artifact binding',
                             keys=cls._KEYS)
        return cls(role=raw['role'],
                   workload_image_digest=raw['workload_image_digest'],
                   installed_root=raw['installed_root'],
                   source_manifest=ProviderRepoArtifactRefV1.from_value(
                       raw['source_manifest']),
                   image_build_attestation=ProviderRepoArtifactRefV1.from_value(
                       raw['image_build_attestation']),
                   measurement_contract=raw['measurement_contract'])

    def canonical_value(self) -> JsonObject:
        return {
            'role': self.role.value,
            'workload_image_digest': self.workload_image_digest,
            'installed_root': self.installed_root,
            'source_manifest': self.source_manifest.canonical_value(),
            'image_build_attestation':
                self.image_build_attestation.canonical_value(),
            'measurement_contract': 'canonical_regular_file_tree_v1',
        }


@dataclasses.dataclass(frozen=True)
class ProviderSkyletJobContractV1(_CanonicalContract):
    """Checked-in schema and renderer bindings for one closed Skylet job."""

    schema_id: str
    schema_artifact: ProviderRepoArtifactRefV1
    renderer_artifact: ProviderRepoArtifactRefV1
    state_store_schema_artifact: ProviderRepoArtifactRefV1
    protocol_artifact_role: ProviderWorkloadArtifactRoleV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'schema_id', 'schema_artifact', 'renderer_artifact',
        'state_store_schema_artifact', 'protocol_artifact_role'
    })
    _SCHEMA_ID: ClassVar[str] = 'skypilot.serve.prebooted-canary-job.v1'

    def __post_init__(self) -> None:
        if self.schema_id != self._SCHEMA_ID:
            raise ValueError('Skylet job schema_id is unsupported.')
        for field in ('schema_artifact', 'renderer_artifact',
                      'state_store_schema_artifact'):
            if type(getattr(self, field)) is not ProviderRepoArtifactRefV1:
                raise TypeError(f'Skylet job {field} has an invalid type.')
        protocol_role = _enum_value(ProviderWorkloadArtifactRoleV1,
                                    self.protocol_artifact_role,
                                    name='Skylet job protocol_artifact_role')
        if protocol_role is not ProviderWorkloadArtifactRoleV1.SKYLET_JOB_PROTOCOL:
            raise ValueError('Skylet job protocol_artifact_role is '
                             'unsupported.')
        object.__setattr__(self, 'protocol_artifact_role', protocol_role)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderSkyletJobContractV1:
        raw = _closed_object(value, name='Skylet job contract', keys=cls._KEYS)
        return cls(
            schema_id=raw['schema_id'],
            schema_artifact=ProviderRepoArtifactRefV1.from_value(
                raw['schema_artifact']),
            renderer_artifact=ProviderRepoArtifactRefV1.from_value(
                raw['renderer_artifact']),
            state_store_schema_artifact=ProviderRepoArtifactRefV1.from_value(
                raw['state_store_schema_artifact']),
            protocol_artifact_role=raw['protocol_artifact_role'])

    def canonical_value(self) -> JsonObject:
        return {
            'schema_id': self._SCHEMA_ID,
            'schema_artifact': self.schema_artifact.canonical_value(),
            'renderer_artifact': self.renderer_artifact.canonical_value(),
            'state_store_schema_artifact':
                self.state_store_schema_artifact.canonical_value(),
            'protocol_artifact_role': self.protocol_artifact_role.value,
        }


@dataclasses.dataclass(frozen=True)
class ProviderSkyletJobSpecV1(_CanonicalContract):
    """Closed policy-free Skylet job rendered for one Serve replica."""

    version: int
    schema_id: str
    source: ProviderLaunchContentSourceV1
    command_profile: str
    entrypoint_artifact_role: str
    replica_id: str
    environment_replica_id: str
    working_directory: None
    setup: None
    mounts: tuple[Any, ...]
    secrets: tuple[Any, ...]
    lifecycle: str
    restart_policy: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'schema_id', 'source', 'command_profile',
        'entrypoint_artifact_role', 'replica_id', 'environment',
        'working_directory', 'setup', 'mounts', 'secrets', 'lifecycle',
        'restart_policy'
    })
    _ENVIRONMENT_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'SKYPILOT_SERVE_REPLICA_ID'})
    _SCHEMA_ID: ClassVar[str] = 'skypilot.serve.prebooted-canary-job.v1'

    def __post_init__(self) -> None:
        _version_one(self.version, name='Skylet job spec version')
        if self.schema_id != self._SCHEMA_ID:
            raise ValueError('Skylet job spec schema_id is unsupported.')
        if type(self.source) is not ProviderLaunchContentSourceV1:
            raise TypeError('Skylet job spec source has an invalid type.')
        if self.command_profile != 'image_serve_canary_entrypoint_v1':
            raise ValueError('Skylet job spec command_profile is unsupported.')
        if self.entrypoint_artifact_role != 'serve_canary_entrypoint':
            raise ValueError('Skylet job spec entrypoint_artifact_role is '
                             'unsupported.')
        object.__setattr__(
            self, 'replica_id',
            _decimal_integer_text(self.replica_id,
                                  name='Skylet job spec replica_id'))
        object.__setattr__(
            self, 'environment_replica_id',
            _decimal_integer_text(
                self.environment_replica_id,
                name=('Skylet job spec '
                      'environment.SKYPILOT_SERVE_REPLICA_ID')))
        if self.replica_id != self.environment_replica_id:
            raise ValueError('Skylet job spec replica ID copies must be '
                             'byte-equal.')
        if self.working_directory is not None:
            raise ValueError('Skylet job spec working_directory must be null.')
        if self.setup is not None:
            raise ValueError('Skylet job spec setup must be null.')
        for field in ('mounts', 'secrets'):
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'Skylet job spec {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'Skylet job spec {field} must be empty.')
        if self.lifecycle != 'long_running_until_pod_delete':
            raise ValueError('Skylet job spec lifecycle is unsupported.')
        if self.restart_policy != 'same_pod_same_logical_job':
            raise ValueError('Skylet job spec restart_policy is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderSkyletJobSpecV1:
        raw = _closed_object(value, name='Skylet job spec', keys=cls._KEYS)
        environment = _closed_object(raw['environment'],
                                     name='Skylet job spec environment',
                                     keys=cls._ENVIRONMENT_KEYS)
        for field in ('mounts', 'secrets'):
            if not isinstance(raw[field], list):
                raise TypeError(f'Skylet job spec {field} must be a list.')
        return cls(
            version=raw['version'],
            schema_id=raw['schema_id'],
            source=ProviderLaunchContentSourceV1.from_value(raw['source']),
            command_profile=raw['command_profile'],
            entrypoint_artifact_role=raw['entrypoint_artifact_role'],
            replica_id=raw['replica_id'],
            environment_replica_id=environment['SKYPILOT_SERVE_REPLICA_ID'],
            working_directory=raw['working_directory'],
            setup=raw['setup'],
            mounts=tuple(raw['mounts']),
            secrets=tuple(raw['secrets']),
            lifecycle=raw['lifecycle'],
            restart_policy=raw['restart_policy'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'schema_id': self._SCHEMA_ID,
            'source': self.source.canonical_value(),
            'command_profile': 'image_serve_canary_entrypoint_v1',
            'entrypoint_artifact_role': 'serve_canary_entrypoint',
            'replica_id': self.replica_id,
            'environment': {
                'SKYPILOT_SERVE_REPLICA_ID': self.environment_replica_id
            },
            'working_directory': None,
            'setup': None,
            'mounts': [],
            'secrets': [],
            'lifecycle': 'long_running_until_pod_delete',
            'restart_policy': 'same_pod_same_logical_job',
        }


@dataclasses.dataclass(frozen=True)
class ProviderSkyletSubmitRequestV1(_CanonicalContract):
    """Idempotent Skylet submit request retaining the complete job spec."""

    protocol: str
    submission_key: uuid.UUID
    job_contract_sha256: str
    job_spec: ProviderSkyletJobSpecV1
    job_spec_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'protocol', 'submission_key', 'job_contract_sha256', 'job_spec',
        'job_spec_sha256'
    })
    _PROTOCOL: ClassVar[str] = 'skylet_idempotent_submit_v1'

    def __post_init__(self) -> None:
        if self.protocol != self._PROTOCOL:
            raise ValueError('Skylet submit request protocol is unsupported.')
        object.__setattr__(
            self, 'submission_key',
            _uuid(self.submission_key,
                  name='Skylet submit request submission_key'))
        object.__setattr__(
            self, 'job_contract_sha256',
            _sha256(self.job_contract_sha256,
                    name='Skylet submit request job_contract_sha256'))
        if not isinstance(self.job_spec, ProviderSkyletJobSpecV1):
            raise TypeError('Skylet submit request job_spec has an invalid '
                            'type.')
        object.__setattr__(
            self, 'job_spec_sha256',
            _sha256(self.job_spec_sha256,
                    name='Skylet submit request job_spec_sha256'))
        if self.job_spec_sha256 != self.job_spec.sha256:
            raise ValueError('Skylet submit request job_spec_sha256 does not '
                             'match job_spec.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderSkyletSubmitRequestV1:
        raw = _closed_object(value,
                             name='Skylet submit request',
                             keys=cls._KEYS)
        return cls(protocol=raw['protocol'],
                   submission_key=raw['submission_key'],
                   job_contract_sha256=raw['job_contract_sha256'],
                   job_spec=ProviderSkyletJobSpecV1.from_value(raw['job_spec']),
                   job_spec_sha256=raw['job_spec_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'protocol': self._PROTOCOL,
            'submission_key': str(self.submission_key),
            'job_contract_sha256': self.job_contract_sha256,
            'job_spec': self.job_spec.canonical_value(),
            'job_spec_sha256': self.job_spec_sha256,
        }


class ProviderSkyletJobReadDispositionV1(str, enum.Enum):
    """Closed outcomes of one exact Skylet job-record read."""

    PRESENT = 'present'
    NOT_FOUND = 'not_found'
    CONFLICT = 'conflict'
    UNCERTAIN = 'uncertain'


class ProviderSkyletJobDurableStateV1(str, enum.Enum):
    """Closed durable lifecycle states returned by Skylet readback."""

    COMMITTED_PENDING_START = 'COMMITTED_PENDING_START'
    START_INTENT = 'START_INTENT'
    START_COMMITTED = 'START_COMMITTED'
    RUNNING = 'RUNNING'
    RECOVERY_PENDING = 'RECOVERY_PENDING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    BLOCKED = 'BLOCKED'


@dataclasses.dataclass(frozen=True)
class ProviderSkyletJobEvidenceV1(_CanonicalContract):
    """Context-free typed evidence from one keyed Skylet job read."""

    protocol: str
    submission_key: uuid.UUID
    job_contract_sha256: str
    job_spec_sha256: str
    retained_submit_request: ProviderSkyletSubmitRequestV1 | None
    state_store_uuid: uuid.UUID
    read_disposition: ProviderSkyletJobReadDispositionV1
    durable_state: ProviderSkyletJobDurableStateV1 | None
    job_id: int | None
    run_epoch: int | None
    record_revision: int | None
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'protocol', 'submission_key', 'job_contract_sha256', 'job_spec_sha256',
        'retained_submit_request', 'state_store_uuid', 'read_disposition',
        'durable_state', 'job_id', 'run_epoch', 'record_revision', 'observed_at'
    })
    _PROTOCOL: ClassVar[str] = 'skylet_idempotent_submit_v1'

    def __post_init__(self) -> None:
        if self.protocol != self._PROTOCOL:
            raise ValueError('Skylet job evidence protocol is unsupported.')
        object.__setattr__(
            self, 'submission_key',
            _uuid(self.submission_key,
                  name='Skylet job evidence submission_key'))
        for field in ('job_contract_sha256', 'job_spec_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field),
                        name=f'Skylet job evidence {field}'))
        if (self.retained_submit_request is not None and not isinstance(
                self.retained_submit_request, ProviderSkyletSubmitRequestV1)):
            raise TypeError('Skylet job evidence retained_submit_request has '
                            'an invalid type.')
        if (self.retained_submit_request is not None and
                self.retained_submit_request.submission_key
                != self.submission_key):
            raise ValueError('Skylet job evidence retained request must use '
                             'the top-level submission key.')
        object.__setattr__(
            self, 'state_store_uuid',
            _uuid(self.state_store_uuid,
                  name='Skylet job evidence state_store_uuid'))
        disposition = _enum_value(ProviderSkyletJobReadDispositionV1,
                                  self.read_disposition,
                                  name='Skylet job evidence read_disposition')
        object.__setattr__(self, 'read_disposition', disposition)
        state = self.durable_state
        if state is not None:
            state = _enum_value(ProviderSkyletJobDurableStateV1,
                                state,
                                name='Skylet job evidence durable_state')
            object.__setattr__(self, 'durable_state', state)
        if self.job_id is not None:
            _positive_integer(self.job_id, name='Skylet job evidence job_id')
        if self.run_epoch is not None:
            _nonnegative_integer(self.run_epoch,
                                 name='Skylet job evidence run_epoch')
        if self.record_revision is not None:
            _positive_integer(self.record_revision,
                              name='Skylet job evidence record_revision')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at,
                       name='Skylet job evidence observed_at'))
        present_or_conflict = disposition in (
            ProviderSkyletJobReadDispositionV1.PRESENT,
            ProviderSkyletJobReadDispositionV1.CONFLICT)
        response_values = (self.retained_submit_request, state, self.job_id,
                           self.run_epoch, self.record_revision)
        if present_or_conflict and any(
                value is None for value in response_values):
            raise ValueError('present and conflict Skylet job evidence '
                             'requires complete retained record values.')
        if not present_or_conflict and any(
                value is not None for value in response_values):
            raise ValueError('not_found and uncertain Skylet job evidence '
                             'requires null retained record values.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderSkyletJobEvidenceV1:
        raw = _closed_object(value, name='Skylet job evidence', keys=cls._KEYS)
        return cls(
            protocol=raw['protocol'],
            submission_key=raw['submission_key'],
            job_contract_sha256=raw['job_contract_sha256'],
            job_spec_sha256=raw['job_spec_sha256'],
            retained_submit_request=(None if raw['retained_submit_request']
                                     is None else
                                     ProviderSkyletSubmitRequestV1.from_value(
                                         raw['retained_submit_request'])),
            state_store_uuid=raw['state_store_uuid'],
            read_disposition=raw['read_disposition'],
            durable_state=raw['durable_state'],
            job_id=raw['job_id'],
            run_epoch=raw['run_epoch'],
            record_revision=raw['record_revision'],
            observed_at=raw['observed_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'protocol': self._PROTOCOL,
            'submission_key': str(self.submission_key),
            'job_contract_sha256': self.job_contract_sha256,
            'job_spec_sha256': self.job_spec_sha256,
            'retained_submit_request':
                (None if self.retained_submit_request is None else
                 self.retained_submit_request.canonical_value()),
            'state_store_uuid': str(self.state_store_uuid),
            'read_disposition': self.read_disposition.value,
            'durable_state': (
                None if self.durable_state is None else self.durable_state.value
            ),
            'job_id': self.job_id,
            'run_epoch': self.run_epoch,
            'record_revision': self.record_revision,
            'observed_at': self.observed_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderSkyletDurabilityContractV1(_CanonicalContract):
    """Pure description of the reviewed node-local Skylet durability path."""

    volume_name: str
    volume_kind: str
    store: str
    schema_artifact: ProviderRepoArtifactRefV1
    transaction_contract: str
    drain_order: str
    launcher_contract: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'volume_name', 'volume_kind', 'store', 'schema_artifact',
        'transaction_contract', 'drain_order', 'launcher_contract'
    })

    def __post_init__(self) -> None:
        expected = {
            'volume_name': 'skylet-state',
            'volume_kind': 'emptyDir',
            'store': 'sqlite_wal_synchronous_full_v1',
            'transaction_contract': 'job_and_start_outbox_same_transaction_v1',
            'drain_order': 'job_id_ascending',
            'launcher_contract': 'durable_run_token_and_post_exec_handshake_v1',
        }
        for field, literal in expected.items():
            if getattr(self, field) != literal:
                raise ValueError(f'Skylet durability {field} is unsupported.')
        if type(self.schema_artifact) is not ProviderRepoArtifactRefV1:
            raise TypeError('Skylet durability schema_artifact has an invalid '
                            'type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderSkyletDurabilityContractV1:
        raw = _closed_object(value,
                             name='Skylet durability contract',
                             keys=cls._KEYS)
        return cls(volume_name=raw['volume_name'],
                   volume_kind=raw['volume_kind'],
                   store=raw['store'],
                   schema_artifact=ProviderRepoArtifactRefV1.from_value(
                       raw['schema_artifact']),
                   transaction_contract=raw['transaction_contract'],
                   drain_order=raw['drain_order'],
                   launcher_contract=raw['launcher_contract'])

    def canonical_value(self) -> JsonObject:
        return {
            'volume_name': 'skylet-state',
            'volume_kind': 'emptyDir',
            'store': 'sqlite_wal_synchronous_full_v1',
            'schema_artifact': self.schema_artifact.canonical_value(),
            'transaction_contract': 'job_and_start_outbox_same_transaction_v1',
            'drain_order': 'job_id_ascending',
            'launcher_contract': 'durable_run_token_and_post_exec_handshake_v1',
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesProvisionRuntimeMetadataV1(_CanonicalContract):
    """Exact asserted no-op metadata for the prebooted runtime."""

    runtime_setup_done: bool
    has_ray: bool
    has_skylet: bool
    has_job_queue: bool
    workdir_synced: bool
    file_mounts_synced: bool
    setup_done: bool
    run_started: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'runtime_setup_done', 'has_ray', 'has_skylet', 'has_job_queue',
        'workdir_synced', 'file_mounts_synced', 'setup_done', 'run_started'
    })
    _EXPECTED: ClassVar[Mapping[str, bool]] = types.MappingProxyType({
        'runtime_setup_done': True,
        'has_ray': True,
        'has_skylet': True,
        'has_job_queue': True,
        'workdir_synced': False,
        'file_mounts_synced': False,
        'setup_done': True,
        'run_started': False,
    })

    def __post_init__(self) -> None:
        for field, expected in self._EXPECTED.items():
            actual = _boolean(getattr(self, field),
                              name=f'provision_runtime_metadata.{field}')
            if actual is not expected:
                raise ValueError(f'provision_runtime_metadata.{field} has an '
                                 'unsupported value.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderKubernetesProvisionRuntimeMetadataV1:
        raw = _closed_object(value,
                             name='provision runtime metadata',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dict(self._EXPECTED)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesJobSubmissionV1(_CanonicalContract):
    """Closed action-keyed Skylet job-submission configuration."""

    protocol: str
    submission_key_source: str
    run_source: ProviderLaunchContentSourceV1
    contract: ProviderSkyletJobContractV1
    durability: ProviderSkyletDurabilityContractV1
    job_spec_profile: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'protocol', 'submission_key_source', 'run_source', 'contract',
        'durability', 'job_spec_profile'
    })

    def __post_init__(self) -> None:
        if self.protocol != 'skylet_idempotent_submit_v1':
            raise ValueError('job submission protocol is unsupported.')
        if self.submission_key_source != 'launch_action_id':
            raise ValueError('job submission key source is unsupported.')
        if type(self.run_source) is not ProviderLaunchContentSourceV1:
            raise TypeError('job submission run_source has an invalid type.')
        if type(self.contract) is not ProviderSkyletJobContractV1:
            raise TypeError('job submission contract has an invalid type.')
        if type(self.durability) is not ProviderSkyletDurabilityContractV1:
            raise TypeError('job submission durability has an invalid type.')
        if self.job_spec_profile != 'ProviderSkyletJobSpecV1':
            raise ValueError('job submission job_spec_profile is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesJobSubmissionV1:
        raw = _closed_object(value, name='job submission', keys=cls._KEYS)
        return cls(protocol=raw['protocol'],
                   submission_key_source=raw['submission_key_source'],
                   run_source=ProviderLaunchContentSourceV1.from_value(
                       raw['run_source']),
                   contract=ProviderSkyletJobContractV1.from_value(
                       raw['contract']),
                   durability=ProviderSkyletDurabilityContractV1.from_value(
                       raw['durability']),
                   job_spec_profile=raw['job_spec_profile'])

    def canonical_value(self) -> JsonObject:
        return {
            'protocol': 'skylet_idempotent_submit_v1',
            'submission_key_source': 'launch_action_id',
            'run_source': self.run_source.canonical_value(),
            'contract': self.contract.canonical_value(),
            'durability': self.durability.canonical_value(),
            'job_spec_profile': 'ProviderSkyletJobSpecV1',
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesPostProvisionV1(_CanonicalContract):
    """Closed prebooted runtime and action-keyed job configuration."""

    runtime_mode: str
    runtime_artifacts: tuple[ProviderWorkloadArtifactBindingV1, ...]
    provision_runtime_metadata: ProviderKubernetesProvisionRuntimeMetadataV1
    sync_workdir: str
    sync_file_mounts: str
    user_setup: str
    pre_exec_hooks_autostop: str
    management_transport: str
    management_port: str
    ssh_fallback: bool
    job_submission: ProviderKubernetesJobSubmissionV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'runtime_mode', 'runtime_artifacts', 'provision_runtime_metadata',
        'sync_workdir', 'sync_file_mounts', 'user_setup',
        'pre_exec_hooks_autostop', 'management_transport', 'management_port',
        'ssh_fallback', 'job_submission'
    })
    _EXPECTED_ARTIFACT_ROLES: ClassVar[tuple[ProviderWorkloadArtifactRoleV1,
                                             ...]] = tuple(
                                                 ProviderWorkloadArtifactRoleV1)

    def __post_init__(self) -> None:
        if self.runtime_mode != 'prebooted_ray_skylet_v1':
            raise ValueError('post-provision runtime_mode is unsupported.')
        if (type(self.runtime_artifacts) is not tuple or len(
                self.runtime_artifacts) != len(self._EXPECTED_ARTIFACT_ROLES) or
                any(
                    type(item) is not ProviderWorkloadArtifactBindingV1
                    for item in self.runtime_artifacts)):
            raise ValueError('post-provision runtime_artifacts must contain '
                             'the exact six typed role bindings.')
        roles = tuple(item.role for item in self.runtime_artifacts)
        if roles != self._EXPECTED_ARTIFACT_ROLES:
            raise ValueError('post-provision runtime artifact roles are not in '
                             'the exact protocol order.')
        if type(self.provision_runtime_metadata
               ) is not ProviderKubernetesProvisionRuntimeMetadataV1:
            raise TypeError('post-provision runtime metadata has an invalid '
                            'type.')
        expected_literals = {
            'sync_workdir': 'assert_absent_skip',
            'sync_file_mounts': 'assert_absent_skip',
            'user_setup': 'assert_null_skip',
            'pre_exec_hooks_autostop': 'assert_absent_skip',
            'management_transport': 'skylet_grpc_only',
        }
        for field, expected in expected_literals.items():
            if getattr(self, field) != expected:
                raise ValueError(f'post-provision {field} is unsupported.')
        management_port = _decimal_port_text(
            self.management_port, name='post-provision management_port')
        if management_port != '46590':
            raise ValueError('post-provision management_port must be 46590.')
        object.__setattr__(self, 'management_port', management_port)
        if _boolean(self.ssh_fallback, name='post-provision ssh_fallback'):
            raise ValueError('post-provision ssh_fallback must be false.')
        if type(self.job_submission) is not ProviderKubernetesJobSubmissionV1:
            raise TypeError(
                'post-provision job_submission has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesPostProvisionV1:
        raw = _closed_object(value,
                             name='Kubernetes post-provision contract',
                             keys=cls._KEYS)
        runtime_artifacts = raw['runtime_artifacts']
        if not isinstance(runtime_artifacts, list):
            raise TypeError('post-provision runtime_artifacts must be a list.')
        return cls(runtime_mode=raw['runtime_mode'],
                   runtime_artifacts=tuple(
                       ProviderWorkloadArtifactBindingV1.from_value(item)
                       for item in runtime_artifacts),
                   provision_runtime_metadata=(
                       ProviderKubernetesProvisionRuntimeMetadataV1.from_value(
                           raw['provision_runtime_metadata'])),
                   sync_workdir=raw['sync_workdir'],
                   sync_file_mounts=raw['sync_file_mounts'],
                   user_setup=raw['user_setup'],
                   pre_exec_hooks_autostop=raw['pre_exec_hooks_autostop'],
                   management_transport=raw['management_transport'],
                   management_port=raw['management_port'],
                   ssh_fallback=raw['ssh_fallback'],
                   job_submission=ProviderKubernetesJobSubmissionV1.from_value(
                       raw['job_submission']))

    def canonical_value(self) -> JsonObject:
        return {
            'runtime_mode': 'prebooted_ray_skylet_v1',
            'runtime_artifacts': [
                artifact.canonical_value()
                for artifact in self.runtime_artifacts
            ],
            'provision_runtime_metadata':
                self.provision_runtime_metadata.canonical_value(),
            'sync_workdir': 'assert_absent_skip',
            'sync_file_mounts': 'assert_absent_skip',
            'user_setup': 'assert_null_skip',
            'pre_exec_hooks_autostop': 'assert_absent_skip',
            'management_transport': 'skylet_grpc_only',
            'management_port': '46590',
            'ssh_fallback': False,
            'job_submission': self.job_submission.canonical_value(),
        }


class ProviderKubernetesEndpointCallerRoleV1(str, enum.Enum):
    """Exact caller order for both warm-standby Serve load balancers."""

    SERVE_LB_SLOT_0 = 'serve_lb_slot_0'
    SERVE_LB_SLOT_1 = 'serve_lb_slot_1'


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesEndpointCallerWorkloadV1(_CanonicalContract):
    """Pure typed projection of one frozen load-balancer Deployment."""

    api_version: str
    kind: str
    namespace: str
    name: str
    uid: str
    resource_version: str
    generation: int
    observed_generation: int
    deletion_timestamp: None
    selector: tuple[ProviderLabelV1, ...]
    pod_template_labels: tuple[ProviderLabelV1, ...]
    service_account_name: str
    automount_service_account_token: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'api_version', 'kind', 'namespace', 'name', 'uid', 'resource_version',
        'generation', 'observed_generation', 'deletion_timestamp', 'selector',
        'pod_template_labels', 'service_account_name',
        'automount_service_account_token'
    })

    def __post_init__(self) -> None:
        for field in ('api_version', 'kind', 'namespace', 'name', 'uid',
                      'resource_version', 'service_account_name'):
            if type(getattr(self, field)) is not str:
                raise TypeError(f'endpoint caller workload {field} must be '
                                'text.')
        if self.api_version != 'apps/v1':
            raise ValueError('endpoint caller workload api_version must be '
                             'apps/v1.')
        if self.kind != 'Deployment':
            raise ValueError(
                'endpoint caller workload kind must be Deployment.')
        if (type(self.generation) is not int or
                type(self.observed_generation) is not int):
            raise TypeError('endpoint caller workload generations must be '
                            'integers.')
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='endpoint caller workload namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('name', 'uid', 'resource_version',
                      'service_account_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'endpoint caller workload {field}'))
        object.__setattr__(
            self, 'generation',
            _positive_integer(self.generation,
                              name='endpoint caller workload generation'))
        object.__setattr__(
            self, 'observed_generation',
            _positive_integer(
                self.observed_generation,
                name='endpoint caller workload observed_generation'))
        if self.deletion_timestamp is not None:
            raise ValueError('endpoint caller workload deletion_timestamp must '
                             'be null.')
        selector = _provider_label_tuple(
            self.selector, name='endpoint caller workload selector')
        if not selector:
            raise ValueError('endpoint caller workload selector must be '
                             'nonempty.')
        object.__setattr__(self, 'selector', selector)
        pod_template_labels = _provider_label_tuple(
            self.pod_template_labels,
            name='endpoint caller workload pod_template_labels')
        if not pod_template_labels:
            raise ValueError(
                'endpoint caller workload pod_template_labels must '
                'be nonempty.')
        object.__setattr__(self, 'pod_template_labels', pod_template_labels)
        if _boolean(self.automount_service_account_token,
                    name=('endpoint caller workload '
                          'automount_service_account_token')):
            raise ValueError('endpoint caller workload token automount must be '
                             'false.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderKubernetesEndpointCallerWorkloadV1:
        raw = _closed_object_shallow(value,
                                     name='endpoint caller workload',
                                     keys=cls._KEYS)
        for field in ('selector', 'pod_template_labels'):
            _provider_bounded_raw_list(raw[field],
                                       name=f'endpoint caller workload {field}')
        return cls(
            api_version=raw['api_version'],
            kind=raw['kind'],
            namespace=raw['namespace'],
            name=raw['name'],
            uid=raw['uid'],
            resource_version=raw['resource_version'],
            generation=raw['generation'],
            observed_generation=raw['observed_generation'],
            deletion_timestamp=raw['deletion_timestamp'],
            selector=tuple(
                ProviderLabelV1.from_value(item) for item in raw['selector']),
            pod_template_labels=tuple(
                ProviderLabelV1.from_value(item)
                for item in raw['pod_template_labels']),
            service_account_name=raw['service_account_name'],
            automount_service_account_token=raw[
                'automount_service_account_token'])

    def canonical_value(self) -> JsonObject:
        return {
            'api_version': 'apps/v1',
            'kind': 'Deployment',
            'namespace': self.namespace,
            'name': self.name,
            'uid': self.uid,
            'resource_version': self.resource_version,
            'generation': self.generation,
            'observed_generation': self.observed_generation,
            'deletion_timestamp': None,
            'selector': [label.canonical_value() for label in self.selector],
            'pod_template_labels': [
                label.canonical_value() for label in self.pod_template_labels
            ],
            'service_account_name': self.service_account_name,
            'automount_service_account_token': False,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesEndpointCallerV1(_CanonicalContract):
    """Bounded nonsecret identity and Pod selector for one endpoint caller."""

    role: ProviderKubernetesEndpointCallerRoleV1
    namespace: str
    namespace_uid: str
    pod_selector: tuple[ProviderLabelV1, ...]
    service_account_name: str
    service_account_uid: str
    workload: ProviderKubernetesEndpointCallerWorkloadV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'namespace', 'namespace_uid', 'pod_selector',
        'service_account_name', 'service_account_uid', 'workload'
    })

    def __post_init__(self) -> None:
        if type(self.role) not in (str, ProviderKubernetesEndpointCallerRoleV1):
            raise TypeError('endpoint caller role must be text.')
        object.__setattr__(
            self, 'role',
            _enum_value(ProviderKubernetesEndpointCallerRoleV1,
                        self.role,
                        name='endpoint caller role'))
        for field in ('namespace', 'namespace_uid', 'service_account_name',
                      'service_account_uid'):
            if type(getattr(self, field)) is not str:
                raise TypeError(f'endpoint caller {field} must be text.')
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='endpoint caller namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('namespace_uid', 'service_account_name',
                      'service_account_uid'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field), name=f'endpoint caller {field}'))
        object.__setattr__(
            self, 'pod_selector',
            _provider_label_tuple(self.pod_selector,
                                  name='endpoint caller pod_selector'))
        if type(self.workload) is not (
                ProviderKubernetesEndpointCallerWorkloadV1):
            raise TypeError('endpoint caller workload has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesEndpointCallerV1:
        raw = _closed_object_shallow(value,
                                     name='endpoint caller',
                                     keys=cls._KEYS)
        pod_selector = raw['pod_selector']
        _provider_bounded_raw_list(pod_selector,
                                   name='endpoint caller pod_selector')
        return cls(
            role=raw['role'],
            namespace=raw['namespace'],
            namespace_uid=raw['namespace_uid'],
            pod_selector=tuple(
                ProviderLabelV1.from_value(item) for item in pod_selector),
            service_account_name=raw['service_account_name'],
            service_account_uid=raw['service_account_uid'],
            workload=ProviderKubernetesEndpointCallerWorkloadV1.from_value(
                raw['workload']))

    def canonical_value(self) -> JsonObject:
        return {
            'role': self.role.value,
            'namespace': self.namespace,
            'namespace_uid': self.namespace_uid,
            'pod_selector': [
                label.canonical_value() for label in self.pod_selector
            ],
            'service_account_name': self.service_account_name,
            'service_account_uid': self.service_account_uid,
            'workload': self.workload.canonical_value(),
        }


def _provider_kubernetes_endpoint_prerequisite_projection_tuple(
    value: Any,
    *,
    name: str,
) -> tuple[ProviderKubernetesPrerequisiteV1, ...]:
    """Validate the exact typed five-role launch endpoint projection."""

    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    expected_roles = (
        ProviderKubernetesPrerequisiteRoleV1.ENDPOINT_NETWORK_POLICY,
        ProviderKubernetesPrerequisiteRoleV1.SERVE_LB_SLOT_0_NAMESPACE,
        ProviderKubernetesPrerequisiteRoleV1.SERVE_LB_SLOT_0_SERVICE_ACCOUNT,
        ProviderKubernetesPrerequisiteRoleV1.SERVE_LB_SLOT_1_NAMESPACE,
        ProviderKubernetesPrerequisiteRoleV1.SERVE_LB_SLOT_1_SERVICE_ACCOUNT,
    )
    if (len(value) != len(expected_roles) or any(
            type(item) is not ProviderKubernetesPrerequisiteV1
            for item in value)):
        raise ValueError(f'{name} must contain the exact five typed role '
                         'projections.')
    if tuple(item.role for item in value) != expected_roles:
        raise ValueError(f'{name} roles are not in exact protocol order.')
    return value


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesEndpointContractV1(_CanonicalContract):
    """Pure frozen Pod-IP endpoint and caller projection."""

    mode: str
    application_port: str
    ambient_fallback: bool
    prerequisite_projection: tuple[ProviderKubernetesPrerequisiteV1, ...]
    required_callers: tuple[ProviderKubernetesEndpointCallerV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'mode', 'application_port', 'ambient_fallback',
        'prerequisite_projection', 'required_callers'
    })
    _EXPECTED_CALLER_ROLES: ClassVar[
        tuple[ProviderKubernetesEndpointCallerRoleV1,
              ...]] = tuple(ProviderKubernetesEndpointCallerRoleV1)

    def __post_init__(self) -> None:
        if type(self.mode) is not str:
            raise TypeError('endpoint mode must be text.')
        if self.mode != 'podip':
            raise ValueError('endpoint mode must be podip.')
        if type(self.application_port) is not str:
            raise TypeError('endpoint application_port must be text.')
        object.__setattr__(
            self, 'application_port',
            _decimal_port_text(self.application_port,
                               name='endpoint application_port'))
        if _boolean(self.ambient_fallback, name='endpoint ambient_fallback'):
            raise ValueError('endpoint ambient_fallback must be false.')
        object.__setattr__(
            self, 'prerequisite_projection',
            _provider_kubernetes_endpoint_prerequisite_projection_tuple(
                self.prerequisite_projection,
                name='endpoint prerequisite_projection'))
        if (type(self.required_callers) is not tuple or len(
                self.required_callers) != len(self._EXPECTED_CALLER_ROLES) or
                any(
                    type(item) is not ProviderKubernetesEndpointCallerV1
                    for item in self.required_callers)):
            raise ValueError('endpoint required_callers must contain the exact '
                             'two typed caller projections.')
        roles = tuple(item.role for item in self.required_callers)
        if roles != self._EXPECTED_CALLER_ROLES:
            raise ValueError('endpoint caller roles are not in the exact '
                             'protocol order.')
        self._validate_internal_projection()
        _ = self.canonical_bytes

    def _validate_internal_projection(self) -> None:
        (_, namespace_zero, service_account_zero, namespace_one,
         service_account_one) = self.prerequisite_projection

        namespace_zero_value = namespace_zero.canonical_value()
        namespace_one_value = namespace_one.canonical_value()
        del namespace_zero_value['role']
        del namespace_one_value['role']
        if canonical_json_bytes(namespace_zero_value) != canonical_json_bytes(
                namespace_one_value):
            raise ValueError('endpoint Namespace aliases must be byte-equal '
                             'after omitting only role.')

        service_accounts = (service_account_zero, service_account_one)
        callers = self.required_callers
        for caller, namespace, service_account in zip(
                callers, (namespace_zero, namespace_one), service_accounts):
            if type(service_account.spec) is not (
                    ProviderKubernetesServiceAccountPrerequisiteSpecV1):
                raise ValueError('endpoint ServiceAccount projection has an '
                                 'invalid typed spec.')
            service_account_projection = service_account.spec.projection
            if (caller.namespace != namespace.name or
                    caller.namespace_uid != namespace.uid or
                    service_account.namespace != caller.namespace or
                    caller.service_account_name != service_account.name or
                    caller.service_account_uid != service_account.uid):
                raise ValueError('endpoint caller identity does not match its '
                                 'prerequisite projections.')
            if (service_account_projection.automount_service_account_token or
                    service_account_projection.image_pull_secrets or
                    service_account_projection.legacy_secret_refs):
                raise ValueError('endpoint ServiceAccount must disable token '
                                 'automount and secret references.')

            workload = caller.workload
            if (workload.namespace != caller.namespace or
                    workload.service_account_name
                    != caller.service_account_name):
                raise ValueError('endpoint caller workload ServiceAccount '
                                 'association is invalid.')
            if workload.selector != caller.pod_selector:
                raise ValueError('endpoint caller workload selector does not '
                                 'match the caller selector.')
            template_labels = {(label.key, label.value)
                               for label in workload.pod_template_labels}
            if any((label.key, label.value) not in template_labels
                   for label in workload.selector):
                raise ValueError('endpoint caller workload template labels do '
                                 'not contain its selector.')
            if workload.observed_generation != workload.generation:
                raise ValueError('endpoint caller workload generation is not '
                                 'fully observed.')

        if ((service_account_zero.namespace, service_account_zero.name)
                == (service_account_one.namespace, service_account_one.name) or
                service_account_zero.uid == service_account_one.uid):
            raise ValueError('endpoint ServiceAccounts must be distinct.')
        workload_zero, workload_one = (caller.workload for caller in callers)
        if ((workload_zero.namespace, workload_zero.name)
                == (workload_one.namespace, workload_one.name) or
                workload_zero.uid == workload_one.uid):
            raise ValueError('endpoint caller Deployments must be distinct.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesEndpointContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes endpoint contract',
                                     keys=cls._KEYS)
        prerequisite_projection = raw['prerequisite_projection']
        required_callers = raw['required_callers']
        _provider_bounded_raw_list(prerequisite_projection,
                                   name='endpoint prerequisite_projection')
        _provider_bounded_raw_list(required_callers,
                                   name='endpoint required_callers')
        if len(prerequisite_projection) != 5:
            raise ValueError('endpoint prerequisite_projection must contain '
                             'the exact five role projections.')
        if len(required_callers) != 2:
            raise ValueError('endpoint required_callers must contain the exact '
                             'two caller projections.')
        return cls(mode=raw['mode'],
                   application_port=raw['application_port'],
                   ambient_fallback=raw['ambient_fallback'],
                   prerequisite_projection=tuple(
                       ProviderKubernetesPrerequisiteV1.from_value(item)
                       for item in prerequisite_projection),
                   required_callers=tuple(
                       ProviderKubernetesEndpointCallerV1.from_value(item)
                       for item in required_callers))

    def canonical_value(self) -> JsonObject:
        return {
            'mode': 'podip',
            'application_port': self.application_port,
            'ambient_fallback': False,
            'prerequisite_projection': [
                prerequisite.canonical_value()
                for prerequisite in self.prerequisite_projection
            ],
            'required_callers': [
                caller.canonical_value() for caller in self.required_callers
            ],
        }


def clean_username_for_explicit_user_v1(original_user: str) -> str:
    """Apply the frozen historical username cleaner without ambient fallback."""

    normalized = _text(original_user, name='request_identity.original_user')
    return common_utils.clean_username_for_explicit_user_v1(normalized)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRequestIdentityV1(_CanonicalContract):
    """Bounded nonsecret request-user identity retained by a capsule."""

    cleaned_user: str
    original_user: str
    frozen_user_hash: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'cleaned_user', 'original_user', 'frozen_user_hash'})

    def __post_init__(self) -> None:
        for field in ('cleaned_user', 'original_user', 'frozen_user_hash'):
            value = getattr(self, field)
            if type(value) is not str:
                raise TypeError(f'request_identity.{field} must be text.')
            object.__setattr__(self, field,
                               _text(value, name=f'request_identity.{field}'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRequestIdentityV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes request identity',
                                     keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


def project_provider_kubernetes_request_identity_v1(
    original_user: str,
    name_basis: ProviderWorkloadNameBasisV1,
) -> ProviderKubernetesRequestIdentityV1:
    """Project one explicit server-effective user into frozen Pod metadata."""

    if type(original_user) is not str:
        raise TypeError('request identity original_user must be text.')
    if type(name_basis) is not ProviderWorkloadNameBasisV1:
        raise TypeError('request identity name basis has an invalid type.')
    return ProviderKubernetesRequestIdentityV1(
        cleaned_user=clean_username_for_explicit_user_v1(original_user),
        original_user=original_user,
        frozen_user_hash=name_basis.frozen_user_hash)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesSchedulingContractV1(_CanonicalContract):
    """Policy-free scheduling inputs for the direct-Pod capsule."""

    node_count: int
    use_spot: bool
    accelerator: None
    node_selector: tuple[Any, ...]
    allowed_nodes: tuple[Any, ...]
    avoid_accelerator_label_keys: tuple[str, ...]
    runtime_class_name: None
    priority_class_name: None
    queue: None
    kueue: bool
    dws: bool
    autoscaler: None
    detected_network_type: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'node_count', 'use_spot', 'accelerator', 'node_selector',
        'allowed_nodes', 'avoid_accelerator_label_keys', 'runtime_class_name',
        'priority_class_name', 'queue', 'kueue', 'dws', 'autoscaler',
        'detected_network_type'
    })
    _NULL_FIELDS: ClassVar[tuple[str,
                                 ...]] = ('accelerator', 'runtime_class_name',
                                          'priority_class_name', 'queue',
                                          'autoscaler')
    _EMPTY_FIELDS: ClassVar[tuple[str,
                                  ...]] = ('node_selector', 'allowed_nodes')
    _FALSE_FIELDS: ClassVar[tuple[str, ...]] = ('use_spot', 'kueue', 'dws')

    def __post_init__(self) -> None:
        if type(self.node_count) is not int:
            raise TypeError('scheduling node_count must be an integer.')
        _positive_integer(self.node_count, name='scheduling.node_count')
        if self.node_count != 1:
            raise ValueError('scheduling node_count must be 1.')
        for field in self._FALSE_FIELDS:
            value = getattr(self, field)
            _boolean(value, name=f'scheduling.{field}')
            if value:
                raise ValueError(f'scheduling {field} must be false.')
        for field in self._NULL_FIELDS:
            if getattr(self, field) is not None:
                raise ValueError(f'scheduling {field} must be null.')
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'scheduling {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'scheduling {field} must be empty.')
        label_keys = self.avoid_accelerator_label_keys
        if type(label_keys) is not tuple:
            raise TypeError('scheduling avoid_accelerator_label_keys must be '
                            'a tuple.')
        if any(type(label_key) is not str for label_key in label_keys):
            raise TypeError('scheduling avoid_accelerator_label_keys must '
                            'contain text.')
        object.__setattr__(
            self, 'avoid_accelerator_label_keys',
            _sorted_text_tuple(label_keys,
                               name='scheduling avoid_accelerator_label_keys',
                               maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if (type(self.detected_network_type) is not str or
                self.detected_network_type != 'default'):
            raise ValueError('scheduling detected_network_type must be '
                             'default.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesSchedulingContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes scheduling contract',
                                     keys=cls._KEYS)
        normalized = dict(raw)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'scheduling {field} must be a list.')
            if len(items) != 0:
                raise ValueError(f'scheduling {field} must be empty.')
            normalized[field] = tuple(items)
        label_keys = raw['avoid_accelerator_label_keys']
        if type(label_keys) is not list:
            raise TypeError('scheduling avoid_accelerator_label_keys must be '
                            'a list.')
        if len(label_keys) > _MAX_LIST_ITEMS:
            raise ValueError('scheduling avoid_accelerator_label_keys must '
                             'contain at most 256 items.')
        normalized['avoid_accelerator_label_keys'] = tuple(label_keys)
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        return {
            'node_count': 1,
            'use_spot': False,
            'accelerator': None,
            'node_selector': [],
            'allowed_nodes': [],
            'avoid_accelerator_label_keys': list(
                self.avoid_accelerator_label_keys),
            'runtime_class_name': None,
            'priority_class_name': None,
            'queue': None,
            'kueue': False,
            'dws': False,
            'autoscaler': None,
            'detected_network_type': 'default',
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesStorageContractV1(_CanonicalContract):
    """Closed absence of storage and mount inputs for the first capsule."""

    persistent_volumes: tuple[Any, ...]
    object_stores: tuple[Any, ...]
    file_mounts: tuple[Any, ...]
    workdir: None
    fuse: bool
    docker_cache: bool
    auto_mounts: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'persistent_volumes', 'object_stores', 'file_mounts', 'workdir', 'fuse',
        'docker_cache', 'auto_mounts'
    })
    _EMPTY_FIELDS: ClassVar[tuple[str, ...]] = ('persistent_volumes',
                                                'object_stores', 'file_mounts')
    _FALSE_FIELDS: ClassVar[tuple[str, ...]] = ('fuse', 'docker_cache',
                                                'auto_mounts')

    def __post_init__(self) -> None:
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'storage {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'storage {field} must be empty.')
        if self.workdir is not None:
            raise ValueError('storage workdir must be null.')
        for field in self._FALSE_FIELDS:
            value = getattr(self, field)
            _boolean(value, name=f'storage.{field}')
            if value:
                raise ValueError(f'storage {field} must be false.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesStorageContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes storage contract',
                                     keys=cls._KEYS)
        normalized = dict(raw)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'storage {field} must be a list.')
            if len(items) != 0:
                raise ValueError(f'storage {field} must be empty.')
            normalized[field] = tuple(items)
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        return {
            'persistent_volumes': [],
            'object_stores': [],
            'file_mounts': [],
            'workdir': None,
            'fuse': False,
            'docker_cache': False,
            'auto_mounts': False,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesMetadataContractV1(_CanonicalContract):
    """Closed absence of caller-selected Kubernetes metadata."""

    global_labels: tuple[Any, ...]
    custom_pod_config: None
    custom_metadata: tuple[Any, ...]
    reserved_labels_injected_last: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'global_labels', 'custom_pod_config', 'custom_metadata',
        'reserved_labels_injected_last'
    })
    _EMPTY_FIELDS: ClassVar[tuple[str,
                                  ...]] = ('global_labels', 'custom_metadata')

    def __post_init__(self) -> None:
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'metadata {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'metadata {field} must be empty.')
        if self.custom_pod_config is not None:
            raise ValueError('metadata custom_pod_config must be null.')
        _boolean(self.reserved_labels_injected_last,
                 name='metadata.reserved_labels_injected_last')
        if not self.reserved_labels_injected_last:
            raise ValueError('metadata reserved_labels_injected_last must be '
                             'true.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesMetadataContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes metadata contract',
                                     keys=cls._KEYS)
        normalized = dict(raw)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'metadata {field} must be a list.')
            if len(items) != 0:
                raise ValueError(f'metadata {field} must be empty.')
            normalized[field] = tuple(items)
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        return {
            'global_labels': [],
            'custom_pod_config': None,
            'custom_metadata': [],
            'reserved_labels_injected_last': True,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesSecurityContractV1(_CanonicalContract):
    """Closed absence of security bootstrap and retained secret material."""

    tls_material: None
    managed_secrets: tuple[Any, ...]
    task_secrets: tuple[Any, ...]
    service_account_bootstrap: bool
    rbac_bootstrap: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'tls_material', 'managed_secrets', 'task_secrets',
        'service_account_bootstrap', 'rbac_bootstrap'
    })
    _EMPTY_FIELDS: ClassVar[tuple[str,
                                  ...]] = ('managed_secrets', 'task_secrets')
    _FALSE_FIELDS: ClassVar[tuple[str, ...]] = ('service_account_bootstrap',
                                                'rbac_bootstrap')

    def __post_init__(self) -> None:
        if self.tls_material is not None:
            raise ValueError('security tls_material must be null.')
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'security {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'security {field} must be empty.')
        for field in self._FALSE_FIELDS:
            value = getattr(self, field)
            _boolean(value, name=f'security.{field}')
            if value:
                raise ValueError(f'security {field} must be false.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesSecurityContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes security contract',
                                     keys=cls._KEYS)
        normalized = dict(raw)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'security {field} must be a list.')
            if len(items) != 0:
                raise ValueError(f'security {field} must be empty.')
            normalized[field] = tuple(items)
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        return {
            'tls_material': None,
            'managed_secrets': [],
            'task_secrets': [],
            'service_account_bootstrap': False,
            'rbac_bootstrap': False,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesObjectMutationEffectV1(_CanonicalContract):
    """One context-free object-mutation scalar union."""

    sequence: int
    role: ProviderObjectRoleV1
    kind: ProviderPodTopologyMutableObjectKindV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({'sequence', 'role', 'kind'})

    def __post_init__(self) -> None:
        if type(self.sequence) is not int:
            raise TypeError('mutation_effect.sequence must be an integer.')
        sequence = _nonnegative_integer(self.sequence,
                                        name='mutation_effect.sequence',
                                        maximum=2)
        if (not isinstance(self.role, ProviderObjectRoleV1) and
                type(self.role) is not str):
            raise TypeError('mutation_effect.role must be text.')
        role = _enum_value(ProviderObjectRoleV1,
                           self.role,
                           name='mutation_effect.role')
        if (not isinstance(self.kind, ProviderPodTopologyMutableObjectKindV1)
                and type(self.kind) is not str):
            raise TypeError('mutation_effect.kind must be text.')
        kind = _enum_value(ProviderPodTopologyMutableObjectKindV1,
                           self.kind,
                           name='mutation_effect.kind')
        object.__setattr__(self, 'sequence', sequence)
        object.__setattr__(self, 'role', role)
        object.__setattr__(self, 'kind', kind)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesObjectMutationEffectV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes object mutation effect',
                                     keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'sequence': self.sequence,
            'role': self.role.value,
            'kind': self.kind.value,
        }


def _validate_provider_kubernetes_mutation_effects_v1(
        value: Any, *, name: str, order_field: str
) -> tuple[ProviderKubernetesObjectMutationEffectV1, ...]:
    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    if len(value) != 3:
        raise ValueError(f'{name} must contain exactly three effects.')
    if any(
            type(effect) is not ProviderKubernetesObjectMutationEffectV1
            for effect in value):
        raise ValueError(f'{name} must contain typed mutation effects.')
    ordered_role_map = sorted(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1,
                              key=lambda entry: getattr(entry, order_field))
    expected = tuple((getattr(entry, order_field), entry.role, entry.kind)
                     for entry in ordered_role_map)
    actual = tuple(
        (effect.sequence, effect.role, effect.kind) for effect in value)
    if actual != expected:
        raise ValueError(f'{name} does not match the exact protocol order.')
    return value


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesLaunchMutationContractV1(_CanonicalContract):
    """Exact bounded launch effect graph for the direct-Pod capsule."""

    role_map_contract: str
    create_effects: tuple[ProviderKubernetesObjectMutationEffectV1, ...]
    delete_effects: tuple[ProviderKubernetesObjectMutationEffectV1, ...]
    job_effect: str
    allowed_patches: tuple[Any, ...]
    allowed_updates: tuple[Any, ...]
    allowed_collection_deletes: tuple[Any, ...]
    delete_requires_identity_labels_and_uid_precondition: bool
    create_409: str
    create_422: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role_map_contract', 'create_effects', 'delete_effects', 'job_effect',
        'allowed_patches', 'allowed_updates', 'allowed_collection_deletes',
        'delete_requires_identity_labels_and_uid_precondition', 'create_409',
        'create_422'
    })
    _EMPTY_FIELDS: ClassVar[tuple[str,
                                  ...]] = ('allowed_patches', 'allowed_updates',
                                           'allowed_collection_deletes')

    def __post_init__(self) -> None:
        if (type(self.role_map_contract) is not str or
                self.role_map_contract != 'ProviderKubernetesObjectRoleMapV1'):
            raise ValueError('launch mutation role_map_contract is '
                             'unsupported.')
        object.__setattr__(
            self, 'create_effects',
            _validate_provider_kubernetes_mutation_effects_v1(
                self.create_effects,
                name='launch mutation create_effects',
                order_field='create_sequence'))
        object.__setattr__(
            self, 'delete_effects',
            _validate_provider_kubernetes_mutation_effects_v1(
                self.delete_effects,
                name='launch mutation delete_effects',
                order_field='delete_sequence'))
        if (type(self.job_effect) is not str or
                self.job_effect != 'one_action_keyed_skylet_submit'):
            raise ValueError('launch mutation job_effect is unsupported.')
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'launch mutation {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'launch mutation {field} must be empty.')
        _boolean(self.delete_requires_identity_labels_and_uid_precondition,
                 name=('launch mutation '
                       'delete_requires_identity_labels_and_uid_precondition'))
        if not self.delete_requires_identity_labels_and_uid_precondition:
            raise ValueError('launch mutation deletes must require identity '
                             'labels and a UID precondition.')
        if (type(self.create_409) is not str or
                self.create_409 != 'exact_admitted_readback_or_conflict'):
            raise ValueError('launch mutation create_409 is unsupported.')
        if (type(self.create_422) is not str or
                self.create_422 != 'terminal_no_rewrite'):
            raise ValueError('launch mutation create_422 is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderKubernetesLaunchMutationContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes launch mutation contract',
                                     keys=cls._KEYS)
        normalized = dict(raw)
        for field in ('create_effects', 'delete_effects'):
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'launch mutation {field} must be a list.')
            if len(items) != 3:
                raise ValueError(f'launch mutation {field} must contain '
                                 'exactly three effects.')
            normalized[field] = tuple(
                ProviderKubernetesObjectMutationEffectV1.from_value(item)
                for item in items)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'launch mutation {field} must be a list.')
            if len(items) != 0:
                raise ValueError(f'launch mutation {field} must be empty.')
            normalized[field] = tuple(items)
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        return {
            'role_map_contract': 'ProviderKubernetesObjectRoleMapV1',
            'create_effects': [
                effect.canonical_value() for effect in self.create_effects
            ],
            'delete_effects': [
                effect.canonical_value() for effect in self.delete_effects
            ],
            'job_effect': 'one_action_keyed_skylet_submit',
            'allowed_patches': [],
            'allowed_updates': [],
            'allowed_collection_deletes': [],
            'delete_requires_identity_labels_and_uid_precondition': True,
            'create_409': 'exact_admitted_readback_or_conflict',
            'create_422': 'terminal_no_rewrite',
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesDownMutationContractV1(_CanonicalContract):
    """Exact bounded down effect graph for the direct-Pod capsule."""

    role_map_contract: str
    delete_effects: tuple[ProviderKubernetesObjectMutationEffectV1, ...]
    delete_requires_identity_labels_and_uid_precondition: bool
    cluster_record_removal: str
    allowed_creates: tuple[Any, ...]
    allowed_patches: tuple[Any, ...]
    allowed_updates: tuple[Any, ...]
    allowed_collection_deletes: tuple[Any, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role_map_contract', 'delete_effects',
        'delete_requires_identity_labels_and_uid_precondition',
        'cluster_record_removal', 'allowed_creates', 'allowed_patches',
        'allowed_updates', 'allowed_collection_deletes'
    })
    _EMPTY_FIELDS: ClassVar[tuple[str,
                                  ...]] = ('allowed_creates', 'allowed_patches',
                                           'allowed_updates',
                                           'allowed_collection_deletes')

    def __post_init__(self) -> None:
        if (type(self.role_map_contract) is not str or
                self.role_map_contract != 'ProviderKubernetesObjectRoleMapV1'):
            raise ValueError('down mutation role_map_contract is unsupported.')
        object.__setattr__(
            self, 'delete_effects',
            _validate_provider_kubernetes_mutation_effects_v1(
                self.delete_effects,
                name='down mutation delete_effects',
                order_field='delete_sequence'))
        _boolean(self.delete_requires_identity_labels_and_uid_precondition,
                 name=('down mutation '
                       'delete_requires_identity_labels_and_uid_precondition'))
        if not self.delete_requires_identity_labels_and_uid_precondition:
            raise ValueError('down mutation deletes must require identity '
                             'labels and a UID precondition.')
        if (type(self.cluster_record_removal) is not str or
                self.cluster_record_removal
                != 'same_uuid_exact_handle_after_absence_v1'):
            raise ValueError('down mutation cluster_record_removal is '
                             'unsupported.')
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if type(value) is not tuple:
                raise TypeError(f'down mutation {field} must be a tuple.')
            if len(value) != 0:
                raise ValueError(f'down mutation {field} must be empty.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesDownMutationContractV1:
        raw = _closed_object_shallow(value,
                                     name='Kubernetes down mutation contract',
                                     keys=cls._KEYS)
        delete_effects = raw['delete_effects']
        if type(delete_effects) is not list:
            raise TypeError('down mutation delete_effects must be a list.')
        if len(delete_effects) != 3:
            raise ValueError('down mutation delete_effects must contain '
                             'exactly three effects.')
        normalized = dict(raw)
        normalized['delete_effects'] = tuple(
            ProviderKubernetesObjectMutationEffectV1.from_value(item)
            for item in delete_effects)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if type(items) is not list:
                raise TypeError(f'down mutation {field} must be a list.')
            if len(items) != 0:
                raise ValueError(f'down mutation {field} must be empty.')
            normalized[field] = tuple(items)
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        return {
            'role_map_contract': 'ProviderKubernetesObjectRoleMapV1',
            'delete_effects': [
                effect.canonical_value() for effect in self.delete_effects
            ],
            'delete_requires_identity_labels_and_uid_precondition': True,
            'cluster_record_removal': 'same_uuid_exact_handle_after_absence_v1',
            'allowed_creates': [],
            'allowed_patches': [],
            'allowed_updates': [],
            'allowed_collection_deletes': [],
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesExecutionCapsuleSeedV1(_CanonicalContract):
    """Closed policy-free launch capsule fields available before rendering."""

    version: int
    implementation_contract: str
    executor_cohort: ProviderAuthorityWorkerCohortV1
    config_projection: ProviderKubernetesConfigProjectionV1
    config_projection_sha256: str
    scope: ProviderKubernetesScopeV1
    principals: ProviderKubernetesPrincipalsV1
    prerequisites: tuple[ProviderKubernetesPrerequisiteV1, ...]
    request_identity: ProviderKubernetesRequestIdentityV1
    resources: ProviderKubernetesResourceContractV1
    renderer: ProviderKubernetesRendererV1
    post_provision: ProviderKubernetesPostProvisionV1
    endpoint: ProviderKubernetesEndpointContractV1
    scheduling: ProviderKubernetesSchedulingContractV1
    storage: ProviderKubernetesStorageContractV1
    metadata: ProviderKubernetesMetadataContractV1
    security: ProviderKubernetesSecurityContractV1
    topology: ProviderPodTopologyV1
    mutation_contract: ProviderKubernetesLaunchMutationContractV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'implementation_contract', 'executor_cohort',
        'config_projection', 'config_projection_sha256', 'scope', 'principals',
        'prerequisites', 'request_identity', 'resources', 'renderer',
        'post_provision', 'endpoint', 'scheduling', 'storage', 'metadata',
        'security', 'topology', 'mutation_contract'
    })
    _IMPLEMENTATION_CONTRACT: ClassVar[
        str] = 'kubernetes_serve_prebooted_runtime_v1'

    def __post_init__(self) -> None:
        _version_one(self.version, name='launch execution capsule seed version')
        if type(self.implementation_contract) is not str:
            raise TypeError('launch execution capsule seed '
                            'implementation_contract must be text.')
        if self.implementation_contract != self._IMPLEMENTATION_CONTRACT:
            raise ValueError('launch execution capsule seed '
                             'implementation_contract is unsupported.')
        child_types: tuple[tuple[str, type[Any]], ...] = (
            ('executor_cohort', ProviderAuthorityWorkerCohortV1),
            ('config_projection', ProviderKubernetesConfigProjectionV1),
            ('scope', ProviderKubernetesScopeV1),
            ('principals', ProviderKubernetesPrincipalsV1),
            ('request_identity', ProviderKubernetesRequestIdentityV1),
            ('resources', ProviderKubernetesResourceContractV1),
            ('renderer', ProviderKubernetesRendererV1),
            ('post_provision', ProviderKubernetesPostProvisionV1),
            ('endpoint', ProviderKubernetesEndpointContractV1),
            ('scheduling', ProviderKubernetesSchedulingContractV1),
            ('storage', ProviderKubernetesStorageContractV1),
            ('metadata', ProviderKubernetesMetadataContractV1),
            ('security', ProviderKubernetesSecurityContractV1),
            ('topology', ProviderPodTopologyV1),
            ('mutation_contract', ProviderKubernetesLaunchMutationContractV1),
        )
        for field, expected_type in child_types:
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'launch execution capsule seed {field} has '
                                'an invalid type.')
        object.__setattr__(
            self, 'prerequisites',
            _provider_kubernetes_prerequisite_inventory_tuple(
                self.prerequisites,
                name='launch execution capsule seed prerequisites'))
        projection_sha256 = _sha256(
            self.config_projection_sha256,
            name='launch execution capsule seed config_projection_sha256')
        object.__setattr__(self, 'config_projection_sha256', projection_sha256)
        if projection_sha256 != self.config_projection.sha256:
            raise ValueError('launch execution capsule seed config projection '
                             'hash does not match.')
        if (self.renderer.source.canonical_bytes !=
                self.post_provision.job_submission.run_source.canonical_bytes):
            raise ValueError('launch execution capsule seed renderer and run '
                             'source must be byte-equal.')
        if (self.config_projection.config_access_inventory.canonical_bytes
                != self.renderer.config_access_inventory.canonical_bytes):
            raise ValueError('launch execution capsule seed config-access '
                             'inventory bindings must be byte-equal.')
        self._validate_internal_projection()
        _ = self.canonical_bytes

    def _validate_internal_projection(self) -> None:
        """Validate every cross-field projection which does not need objects."""

        config = self.config_projection
        scope = self.scope
        principals = self.principals
        if not scope.in_cluster:
            raise ValueError('launch execution capsule seed scope must be '
                             'in-cluster.')
        target_namespaces = (
            scope.namespace,
            config.target_namespace,
            principals.workload.namespace,
            principals.caller_authorization.rules.namespace,
        )
        if any(namespace != scope.namespace for namespace in target_namespaces):
            raise ValueError('launch execution capsule seed target namespaces '
                             'are not byte-equal.')

        caller_scope = (
            scope.caller_service_account_namespace,
            scope.caller_service_account_name,
            scope.caller_service_account_uid,
        )
        caller_principal = (principals.caller.namespace, principals.caller.name,
                            principals.caller.uid)
        workload_scope = (
            scope.workload_service_account_namespace,
            scope.workload_service_account_name,
            scope.workload_service_account_uid,
        )
        workload_principal = (principals.workload.namespace,
                              principals.workload.name, principals.workload.uid)
        if (caller_scope != caller_principal or
                workload_scope != workload_principal):
            raise ValueError('launch execution capsule seed principals do not '
                             'match the Kubernetes scope.')

        by_role = {item.role: item for item in self.prerequisites}
        authority_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.AUTHORITY_RELEASE_NAMESPACE]
        target_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.TARGET_NAMESPACE]
        kube_system_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.KUBE_SYSTEM_NAMESPACE]
        if (authority_namespace.name != self.executor_cohort.manifest.namespace
                or authority_namespace.name != principals.caller.namespace or
                self.executor_cohort.manifest.service_account_name
                != principals.caller.name or
                self.executor_cohort.service_account_uid
                != principals.caller.uid):
            raise ValueError('launch execution capsule seed authority cohort, '
                             'Namespace, and caller principal do not match.')
        if (target_namespace.name != scope.namespace or
                target_namespace.uid != scope.target_namespace_uid or
                kube_system_namespace.name != 'kube-system' or
                kube_system_namespace.uid != scope.kube_system_namespace_uid):
            raise ValueError('launch execution capsule seed Namespace '
                             'prerequisites do not match the Kubernetes scope.')
        for role, principal in (
            (ProviderKubernetesPrerequisiteRoleV1.CALLER_SERVICE_ACCOUNT,
             principals.caller),
            (ProviderKubernetesPrerequisiteRoleV1.WORKLOAD_SERVICE_ACCOUNT,
             principals.workload),
        ):
            prerequisite = by_role[role]
            if type(prerequisite.spec) is not (
                    ProviderKubernetesServiceAccountPrerequisiteSpecV1):
                raise ValueError('launch execution capsule seed '
                                 'ServiceAccount prerequisite has an invalid '
                                 'spec.')
            if (prerequisite.spec.projection.canonical_bytes
                    != principal.canonical_bytes):
                raise ValueError('launch execution capsule seed '
                                 'ServiceAccount prerequisite does not match '
                                 'its principal.')

        for projected in self.endpoint.prerequisite_projection:
            if projected.canonical_bytes != by_role[
                    projected.role].canonical_bytes:
                raise ValueError('launch execution capsule seed endpoint '
                                 'prerequisite projection is not byte-equal to '
                                 'the full inventory.')
        network_policy = by_role[
            ProviderKubernetesPrerequisiteRoleV1.ENDPOINT_NETWORK_POLICY]
        if network_policy.namespace != scope.namespace:
            raise ValueError('launch execution capsule seed NetworkPolicy '
                             'namespace does not match the target namespace.')
        for caller in self.endpoint.required_callers:
            if (caller.namespace != authority_namespace.name or
                    caller.namespace_uid != authority_namespace.uid):
                raise ValueError('launch execution capsule seed endpoint '
                                 'caller Namespace does not match authority '
                                 'release.')

        resource_contract = self.resources
        if (config.port_mode != resource_contract.port_mode or
                self.endpoint.mode != resource_contract.port_mode or
                self.endpoint.application_port
                != resource_contract.application_port or
                self.topology.application_port
                != resource_contract.application_port or
                self.topology.resources_ports
                != resource_contract.resources_ports or
                self.scheduling.node_count != self.topology.node_count or
                self.scheduling.use_spot):
            raise ValueError('launch execution capsule seed resource, '
                             'topology, endpoint, and scheduling projections '
                             'do not match.')

        scheduling_fields = ('runtime_class_name', 'priority_class_name',
                             'queue', 'kueue', 'dws', 'autoscaler',
                             'detected_network_type')
        if any(
                getattr(config, field) != getattr(self.scheduling, field)
                for field in scheduling_fields):
            raise ValueError('launch execution capsule seed scheduling '
                             'projection does not match config.')
        storage_fields = ('persistent_volumes', 'object_stores', 'file_mounts',
                          'workdir', 'fuse', 'docker_cache', 'auto_mounts')
        if any(
                getattr(config, field) != getattr(self.storage, field)
                for field in storage_fields):
            raise ValueError('launch execution capsule seed storage '
                             'projection does not match config.')
        metadata_fields = ('global_labels', 'custom_pod_config',
                           'custom_metadata')
        if any(
                getattr(config, field) != getattr(self.metadata, field)
                for field in metadata_fields):
            raise ValueError('launch execution capsule seed metadata '
                             'projection does not match config.')
        security_fields = ('tls_material', 'managed_secrets', 'task_secrets',
                           'service_account_bootstrap', 'rbac_bootstrap')
        if any(
                getattr(config, field) != getattr(self.security, field)
                for field in security_fields):
            raise ValueError('launch execution capsule seed security '
                             'projection does not match config.')

        cleaned_user = self.request_identity.cleaned_user
        for topology_object in self.topology.mutable_objects:
            labels = {
                label.key: label.value for label in topology_object.labels
            }
            expected_labels = {
                key: labels.get(key)
                for key in _PROVIDER_KUBERNETES_IDENTITY_LABEL_KEYS_V1
            }
            expected_labels['skypilot-user'] = cleaned_user
            if topology_object.role is ProviderObjectRoleV1.HEAD_POD:
                expected_labels['component'] = topology_object.name
            else:
                expected_labels['service-role'] = topology_object.role.value
            if labels != expected_labels:
                raise ValueError('launch execution capsule seed topology role '
                                 'label map is not exact.')

        manifest_digest = (
            resource_contract.image.qualification.oci_manifest_digest)
        if any(binding.workload_image_digest != manifest_digest
               for binding in self.post_provision.runtime_artifacts):
            raise ValueError('launch execution capsule seed runtime artifact '
                             'image digests do not match the workload image.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesExecutionCapsuleSeedV1:
        _bounded_canonical_json_bytes(
            value,
            name='Kubernetes launch execution capsule seed',
            require_object=True,
            allow_empty_strings=True)
        raw = _closed_object_shallow(
            value,
            name='Kubernetes launch execution capsule seed',
            keys=cls._KEYS)
        return cls(
            version=raw['version'],
            implementation_contract=raw['implementation_contract'],
            executor_cohort=ProviderAuthorityWorkerCohortV1.from_value(
                raw['executor_cohort']),
            config_projection=ProviderKubernetesConfigProjectionV1.from_value(
                raw['config_projection']),
            config_projection_sha256=raw['config_projection_sha256'],
            scope=ProviderKubernetesScopeV1.from_value(raw['scope']),
            principals=ProviderKubernetesPrincipalsV1.from_value(
                raw['principals']),
            prerequisites=_provider_kubernetes_prerequisite_inventory_from_value(
                raw['prerequisites'],
                name='launch execution capsule seed prerequisites'),
            request_identity=ProviderKubernetesRequestIdentityV1.from_value(
                raw['request_identity']),
            resources=ProviderKubernetesResourceContractV1.from_value(
                raw['resources']),
            renderer=ProviderKubernetesRendererV1.from_value(raw['renderer']),
            post_provision=ProviderKubernetesPostProvisionV1.from_value(
                raw['post_provision']),
            endpoint=ProviderKubernetesEndpointContractV1.from_value(
                raw['endpoint']),
            scheduling=ProviderKubernetesSchedulingContractV1.from_value(
                raw['scheduling']),
            storage=ProviderKubernetesStorageContractV1.from_value(
                raw['storage']),
            metadata=ProviderKubernetesMetadataContractV1.from_value(
                raw['metadata']),
            security=ProviderKubernetesSecurityContractV1.from_value(
                raw['security']),
            topology=ProviderPodTopologyV1.from_value(raw['topology']),
            mutation_contract=(
                ProviderKubernetesLaunchMutationContractV1.from_value(
                    raw['mutation_contract'])))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'implementation_contract': self._IMPLEMENTATION_CONTRACT,
            'executor_cohort': self.executor_cohort.canonical_value(),
            'config_projection': self.config_projection.canonical_value(),
            'config_projection_sha256': self.config_projection_sha256,
            'scope': self.scope.canonical_value(),
            'principals': self.principals.canonical_value(),
            'prerequisites': [
                item.canonical_value() for item in self.prerequisites
            ],
            'request_identity': self.request_identity.canonical_value(),
            'resources': self.resources.canonical_value(),
            'renderer': self.renderer.canonical_value(),
            'post_provision': self.post_provision.canonical_value(),
            'endpoint': self.endpoint.canonical_value(),
            'scheduling': self.scheduling.canonical_value(),
            'storage': self.storage.canonical_value(),
            'metadata': self.metadata.canonical_value(),
            'security': self.security.canonical_value(),
            'topology': self.topology.canonical_value(),
            'mutation_contract': self.mutation_contract.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRendererInputV1(_CanonicalContract):
    """Sole closed pointer root accepted by the pure Kubernetes renderer."""

    version: int
    contract: str
    resource_identity: ProviderResourceIdentityV1
    sky_cluster_name: str
    sky_cluster_record_uuid: uuid.UUID
    name_basis: ProviderWorkloadNameBasisV1
    seed: ProviderKubernetesExecutionCapsuleSeedV1
    retained_source: ProviderLaunchContentSourceV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'contract', 'resource_identity', 'sky_cluster_name',
        'sky_cluster_record_uuid', 'name_basis', 'seed', 'retained_source'
    })
    _CONTRACT: ClassVar[str] = 'validated_launch_spec_v1'

    def __post_init__(self) -> None:
        _version_one(self.version, name='Kubernetes renderer input version')
        if type(self.contract) is not str:
            raise TypeError('Kubernetes renderer input contract must be text.')
        if self.contract != self._CONTRACT:
            raise ValueError('Kubernetes renderer input contract is '
                             'unsupported.')
        for field, expected_type in (
            ('resource_identity', ProviderResourceIdentityV1),
            ('name_basis', ProviderWorkloadNameBasisV1),
            ('seed', ProviderKubernetesExecutionCapsuleSeedV1),
            ('retained_source', ProviderLaunchContentSourceV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'Kubernetes renderer input {field} has an '
                                'invalid type.')
        object.__setattr__(
            self, 'sky_cluster_name',
            _text(self.sky_cluster_name,
                  name='Kubernetes renderer input sky_cluster_name'))
        object.__setattr__(
            self, 'sky_cluster_record_uuid',
            _uuid(self.sky_cluster_record_uuid,
                  name='Kubernetes renderer input sky_cluster_record_uuid'))
        expected_basis = ProviderWorkloadNameBasisV1(
            version=1,
            display_name=self.sky_cluster_name,
            frozen_user_hash=self.seed.request_identity.frozen_user_hash,
            max_length=42,
            cluster_name_hash_length=8)
        if (self.sky_cluster_name != self.name_basis.display_name or
                self.name_basis.canonical_bytes
                != expected_basis.canonical_bytes):
            raise ValueError('Kubernetes renderer input name basis does not '
                             'match its independently copied inputs.')
        sources = (
            self.retained_source.canonical_bytes,
            self.seed.renderer.source.canonical_bytes,
            self.seed.post_provision.job_submission.run_source.canonical_bytes,
        )
        if len(set(sources)) != 1:
            raise ValueError('Kubernetes renderer input source copies must be '
                             'byte-equal.')
        if (self.retained_source.service_incarnation
                != self.resource_identity.service_incarnation):
            raise ValueError('Kubernetes renderer input retained source does '
                             'not match the resource service incarnation.')
        self._validate_topology_identity()
        _ = self.canonical_bytes

    def _validate_topology_identity(self) -> None:
        """Bind seed topology fields to independently copied input identity."""

        provider_cluster_name = self.name_basis.provider_cluster_name
        workload_name = self.name_basis.workload_name
        cluster_uuid = str(self.sky_cluster_record_uuid)
        replica_uuid = str(self.resource_identity.replica_incarnation)
        cleaned_user = self.seed.request_identity.cleaned_user
        expected = (
            (ProviderObjectRoleV1.HEAD_SSH_SERVICE, f'{workload_name}-ssh', {
                'service-role': 'head_ssh_service',
                'skypilot-cluster-name': provider_cluster_name,
                'skypilot-user': cleaned_user,
                'skypilot.co/cluster-record-uuid': cluster_uuid,
                'skypilot.co/serve-replica-incarnation': replica_uuid,
            }),
            (ProviderObjectRoleV1.HEAD_SERVICE, workload_name, {
                'service-role': 'head_service',
                'skypilot-cluster-name': provider_cluster_name,
                'skypilot-user': cleaned_user,
                'skypilot.co/cluster-record-uuid': cluster_uuid,
                'skypilot.co/serve-replica-incarnation': replica_uuid,
            }),
            (ProviderObjectRoleV1.HEAD_POD, workload_name, {
                'component': workload_name,
                'skypilot-cluster-name': provider_cluster_name,
                'skypilot-user': cleaned_user,
                'skypilot.co/cluster-record-uuid': cluster_uuid,
                'skypilot.co/serve-replica-incarnation': replica_uuid,
            }),
        )
        actual = tuple((item.role, item.name, {
            label.key: label.value for label in item.labels
        }) for item in self.seed.topology.mutable_objects)
        if actual != expected:
            raise ValueError('Kubernetes renderer input topology names or '
                             'labels do not match its independent identity.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRendererInputV1:
        _bounded_canonical_json_bytes(value,
                                      name='Kubernetes renderer input',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='Kubernetes renderer input',
                                     keys=cls._KEYS)
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            resource_identity=ProviderResourceIdentityV1.from_value(
                raw['resource_identity']),
            sky_cluster_name=raw['sky_cluster_name'],
            sky_cluster_record_uuid=_uuid(
                raw['sky_cluster_record_uuid'],
                name='Kubernetes renderer input sky_cluster_record_uuid'),
            name_basis=ProviderWorkloadNameBasisV1.from_value(
                raw['name_basis']),
            seed=ProviderKubernetesExecutionCapsuleSeedV1.from_value(
                raw['seed']),
            retained_source=ProviderLaunchContentSourceV1.from_value(
                raw['retained_source']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': self._CONTRACT,
            'resource_identity': self.resource_identity.canonical_value(),
            'sky_cluster_name': self.sky_cluster_name,
            'sky_cluster_record_uuid': str(self.sky_cluster_record_uuid),
            'name_basis': self.name_basis.canonical_value(),
            'seed': self.seed.canonical_value(),
            'retained_source': self.retained_source.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesExecutionCapsuleV1(_CanonicalContract):
    """Closed policy-free launch execution preimage for direct Pod actuation."""

    version: int
    implementation_contract: str
    executor_cohort: ProviderAuthorityWorkerCohortV1
    config_projection: ProviderKubernetesConfigProjectionV1
    config_projection_sha256: str
    scope: ProviderKubernetesScopeV1
    principals: ProviderKubernetesPrincipalsV1
    prerequisites: tuple[ProviderKubernetesPrerequisiteV1, ...]
    request_identity: ProviderKubernetesRequestIdentityV1
    resources: ProviderKubernetesResourceContractV1
    renderer: ProviderKubernetesRendererV1
    objects: tuple[ProviderKubernetesObjectPlanV1, ...]
    post_provision: ProviderKubernetesPostProvisionV1
    endpoint: ProviderKubernetesEndpointContractV1
    scheduling: ProviderKubernetesSchedulingContractV1
    storage: ProviderKubernetesStorageContractV1
    metadata: ProviderKubernetesMetadataContractV1
    security: ProviderKubernetesSecurityContractV1
    topology: ProviderPodTopologyV1
    mutation_contract: ProviderKubernetesLaunchMutationContractV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'implementation_contract', 'executor_cohort',
        'config_projection', 'config_projection_sha256', 'scope', 'principals',
        'prerequisites', 'request_identity', 'resources', 'renderer', 'objects',
        'post_provision', 'endpoint', 'scheduling', 'storage', 'metadata',
        'security', 'topology', 'mutation_contract'
    })
    _IMPLEMENTATION_CONTRACT: ClassVar[
        str] = 'kubernetes_serve_prebooted_runtime_v1'

    def __post_init__(self) -> None:
        _version_one(self.version, name='launch execution capsule version')
        if type(self.implementation_contract) is not str:
            raise TypeError('launch execution capsule implementation_contract '
                            'must be text.')
        if self.implementation_contract != self._IMPLEMENTATION_CONTRACT:
            raise ValueError('launch execution capsule implementation_contract '
                             'is unsupported.')
        child_types: tuple[tuple[str, type[Any]], ...] = (
            ('executor_cohort', ProviderAuthorityWorkerCohortV1),
            ('config_projection', ProviderKubernetesConfigProjectionV1),
            ('scope', ProviderKubernetesScopeV1),
            ('principals', ProviderKubernetesPrincipalsV1),
            ('request_identity', ProviderKubernetesRequestIdentityV1),
            ('resources', ProviderKubernetesResourceContractV1),
            ('renderer', ProviderKubernetesRendererV1),
            ('post_provision', ProviderKubernetesPostProvisionV1),
            ('endpoint', ProviderKubernetesEndpointContractV1),
            ('scheduling', ProviderKubernetesSchedulingContractV1),
            ('storage', ProviderKubernetesStorageContractV1),
            ('metadata', ProviderKubernetesMetadataContractV1),
            ('security', ProviderKubernetesSecurityContractV1),
            ('topology', ProviderPodTopologyV1),
            ('mutation_contract', ProviderKubernetesLaunchMutationContractV1),
        )
        for field, expected_type in child_types:
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'launch execution capsule {field} has an '
                                'invalid type.')
        object.__setattr__(
            self, 'prerequisites',
            _provider_kubernetes_prerequisite_inventory_tuple(
                self.prerequisites,
                name='launch execution capsule prerequisites'))
        if type(self.objects) is not tuple:
            raise TypeError('launch execution capsule objects must be a tuple.')
        if len(self.objects) != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1):
            raise ValueError('launch execution capsule objects must contain '
                             'exactly three plans.')
        if any(
                type(item) is not ProviderKubernetesObjectPlanV1
                for item in self.objects):
            raise ValueError('launch execution capsule objects must contain '
                             'exact typed object plans.')
        expected_objects = tuple(
            (entry.create_sequence, entry.role, entry.kind)
            for entry in PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
        actual_objects = tuple(
            (item.sequence, item.role, item.kind) for item in self.objects)
        if actual_objects != expected_objects:
            raise ValueError('launch execution capsule objects do not match '
                             'the exact create order.')
        projection_sha256 = _sha256(
            self.config_projection_sha256,
            name='launch execution capsule config_projection_sha256')
        object.__setattr__(self, 'config_projection_sha256', projection_sha256)
        if projection_sha256 != self.config_projection.sha256:
            raise ValueError('launch execution capsule config projection hash '
                             'does not match.')
        if (self.renderer.source.canonical_bytes !=
                self.post_provision.job_submission.run_source.canonical_bytes):
            raise ValueError('launch execution capsule renderer and run source '
                             'must be byte-equal.')
        if (self.config_projection.config_access_inventory.canonical_bytes
                != self.renderer.config_access_inventory.canonical_bytes):
            raise ValueError('launch execution capsule config-access inventory '
                             'bindings must be byte-equal.')
        ProviderKubernetesExecutionCapsuleSeedV1(
            version=self.version,
            implementation_contract=self.implementation_contract,
            executor_cohort=self.executor_cohort,
            config_projection=self.config_projection,
            config_projection_sha256=self.config_projection_sha256,
            scope=self.scope,
            principals=self.principals,
            prerequisites=self.prerequisites,
            request_identity=self.request_identity,
            resources=self.resources,
            renderer=self.renderer,
            post_provision=self.post_provision,
            endpoint=self.endpoint,
            scheduling=self.scheduling,
            storage=self.storage,
            metadata=self.metadata,
            security=self.security,
            topology=self.topology,
            mutation_contract=self.mutation_contract)
        normalization = self.renderer.admitted_object_normalization.canonical_bytes
        if any(item.normalization_profile.canonical_bytes != normalization
               for item in self.objects):
            raise ValueError('launch execution capsule object normalization '
                             'profiles must be byte-equal to the renderer.')
        self._validate_internal_projection()
        _ = self.canonical_bytes

    def _validate_internal_projection(self) -> None:
        """Validate every provider-affecting duplicate owned by the capsule."""

        config = self.config_projection
        scope = self.scope
        principals = self.principals
        if not scope.in_cluster:
            raise ValueError(
                'launch execution capsule scope must be in-cluster.')
        target_namespaces = (
            scope.namespace,
            config.target_namespace,
            principals.workload.namespace,
            principals.caller_authorization.rules.namespace,
            *(item.namespace for item in self.objects),
        )
        if any(namespace != scope.namespace for namespace in target_namespaces):
            raise ValueError('launch execution capsule target namespaces are '
                             'not byte-equal.')

        caller_scope = (
            scope.caller_service_account_namespace,
            scope.caller_service_account_name,
            scope.caller_service_account_uid,
        )
        caller_principal = (principals.caller.namespace, principals.caller.name,
                            principals.caller.uid)
        workload_scope = (
            scope.workload_service_account_namespace,
            scope.workload_service_account_name,
            scope.workload_service_account_uid,
        )
        workload_principal = (principals.workload.namespace,
                              principals.workload.name, principals.workload.uid)
        if caller_scope != caller_principal or workload_scope != workload_principal:
            raise ValueError('launch execution capsule principals do not match '
                             'the Kubernetes scope.')

        by_role = {item.role: item for item in self.prerequisites}
        authority_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.AUTHORITY_RELEASE_NAMESPACE]
        target_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.TARGET_NAMESPACE]
        kube_system_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.KUBE_SYSTEM_NAMESPACE]
        if (authority_namespace.name != self.executor_cohort.manifest.namespace
                or authority_namespace.name != principals.caller.namespace or
                self.executor_cohort.manifest.service_account_name
                != principals.caller.name or
                self.executor_cohort.service_account_uid
                != principals.caller.uid):
            raise ValueError('launch execution capsule authority cohort, '
                             'Namespace, and caller principal do not match.')
        if (target_namespace.name != scope.namespace or
                target_namespace.uid != scope.target_namespace_uid or
                kube_system_namespace.name != 'kube-system' or
                kube_system_namespace.uid != scope.kube_system_namespace_uid):
            raise ValueError('launch execution capsule Namespace prerequisites '
                             'do not match the Kubernetes scope.')
        for role, principal in (
            (ProviderKubernetesPrerequisiteRoleV1.CALLER_SERVICE_ACCOUNT,
             principals.caller),
            (ProviderKubernetesPrerequisiteRoleV1.WORKLOAD_SERVICE_ACCOUNT,
             principals.workload),
        ):
            prerequisite = by_role[role]
            if type(prerequisite.spec) is not (
                    ProviderKubernetesServiceAccountPrerequisiteSpecV1):
                raise ValueError('launch execution capsule ServiceAccount '
                                 'prerequisite has an invalid spec.')
            if (prerequisite.spec.projection.canonical_bytes
                    != principal.canonical_bytes):
                raise ValueError('launch execution capsule ServiceAccount '
                                 'prerequisite does not match its principal.')

        for projected in self.endpoint.prerequisite_projection:
            if (projected.canonical_bytes
                    != by_role[projected.role].canonical_bytes):
                raise ValueError('launch execution capsule endpoint '
                                 'prerequisite projection is not byte-equal '
                                 'to the full inventory.')
        network_policy = by_role[
            ProviderKubernetesPrerequisiteRoleV1.ENDPOINT_NETWORK_POLICY]
        if network_policy.namespace != scope.namespace:
            raise ValueError('launch execution capsule NetworkPolicy '
                             'namespace does not match the target namespace.')
        for caller in self.endpoint.required_callers:
            if (caller.namespace != authority_namespace.name or
                    caller.namespace_uid != authority_namespace.uid):
                raise ValueError('launch execution capsule endpoint caller '
                                 'Namespace does not match authority release.')

        resource_contract = self.resources
        if (config.port_mode != resource_contract.port_mode or
                self.endpoint.mode != resource_contract.port_mode or
                self.endpoint.application_port
                != resource_contract.application_port or
                self.topology.application_port
                != resource_contract.application_port or
                self.topology.resources_ports
                != resource_contract.resources_ports or
                self.scheduling.node_count != self.topology.node_count or
                self.scheduling.use_spot):
            raise ValueError('launch execution capsule resource, topology, '
                             'endpoint, and scheduling projections do not '
                             'match.')

        scheduling_fields = ('runtime_class_name', 'priority_class_name',
                             'queue', 'kueue', 'dws', 'autoscaler',
                             'detected_network_type')
        if any(
                getattr(config, field) != getattr(self.scheduling, field)
                for field in scheduling_fields):
            raise ValueError('launch execution capsule scheduling projection '
                             'does not match config.')
        storage_fields = ('persistent_volumes', 'object_stores', 'file_mounts',
                          'workdir', 'fuse', 'docker_cache', 'auto_mounts')
        if any(
                getattr(config, field) != getattr(self.storage, field)
                for field in storage_fields):
            raise ValueError('launch execution capsule storage projection does '
                             'not match config.')
        metadata_fields = ('global_labels', 'custom_pod_config',
                           'custom_metadata')
        if any(
                getattr(config, field) != getattr(self.metadata, field)
                for field in metadata_fields):
            raise ValueError('launch execution capsule metadata projection '
                             'does not match config.')
        security_fields = ('tls_material', 'managed_secrets', 'task_secrets',
                           'service_account_bootstrap', 'rbac_bootstrap')
        if any(
                getattr(config, field) != getattr(self.security, field)
                for field in security_fields):
            raise ValueError('launch execution capsule security projection '
                             'does not match config.')

        cleaned_user = self.request_identity.cleaned_user
        original_user = self.request_identity.original_user
        expected_identity_label_keys = (
            'skypilot-cluster-name',
            'skypilot.co/cluster-record-uuid',
            'skypilot.co/serve-replica-incarnation',
        )
        for topology_object, object_plan in zip(self.topology.mutable_objects,
                                                self.objects):
            if (topology_object.role is not object_plan.role or
                    topology_object.kind is not object_plan.kind or
                    topology_object.name != object_plan.name):
                raise ValueError('launch execution capsule object plan does '
                                 'not match its topology entry.')
            topology_labels = {
                label.key: label.value for label in topology_object.labels
            }
            expected_role_labels = {
                'skypilot-cluster-name':
                    topology_labels.get('skypilot-cluster-name'),
                'skypilot.co/cluster-record-uuid':
                    topology_labels.get('skypilot.co/cluster-record-uuid'),
                'skypilot.co/serve-replica-incarnation': topology_labels.get(
                    'skypilot.co/serve-replica-incarnation'),
                'skypilot-user': cleaned_user,
            }
            if object_plan.role is ProviderObjectRoleV1.HEAD_POD:
                expected_role_labels['component'] = object_plan.name
            else:
                expected_role_labels['service-role'] = object_plan.role.value
            if topology_labels != expected_role_labels:
                raise ValueError('launch execution capsule object role label '
                                 'map is not exact.')
            expected_identity_labels = tuple(
                (key, topology_labels.get(key))
                for key in expected_identity_label_keys)
            actual_identity_labels = tuple(
                (label.key, label.value)
                for label in object_plan.required_identity_labels)
            if actual_identity_labels != expected_identity_labels:
                raise ValueError('launch execution capsule object identity '
                                 'labels do not match topology.')
            ValidatedKubernetesServeThreeObjectBodyV1(
                role=object_plan.role, body=object_plan.request_body)
            body = object_plan.request_body.canonical_value()
            metadata = body['metadata']
            if (metadata['labels'] != topology_labels or
                    topology_labels.get('skypilot-user') != cleaned_user):
                raise ValueError('launch execution capsule object body labels '
                                 'do not match topology and request identity.')
            spec = body['spec']
            if object_plan.role is ProviderObjectRoleV1.HEAD_POD:
                self._validate_head_pod_projection(spec, metadata,
                                                   original_user)
            self._validate_requested_semantic_projection(object_plan, body)

        manifest_digest = resource_contract.image.qualification.oci_manifest_digest
        if any(binding.workload_image_digest != manifest_digest
               for binding in self.post_provision.runtime_artifacts):
            raise ValueError('launch execution capsule runtime artifact image '
                             'digests do not match the workload image.')

    def _validate_head_pod_projection(self, spec: JsonObject,
                                      metadata: JsonObject,
                                      original_user: str) -> None:
        """Validate the capsule-owned dynamic direct-Pod request copies."""

        if metadata.get('annotations') != {'skypilot-user': original_user}:
            raise ValueError('launch execution capsule Pod user annotation '
                             'does not match request identity.')
        if (spec['serviceAccount'] != self.principals.workload.name or
                spec['serviceAccountName'] != self.principals.workload.name):
            raise ValueError('launch execution capsule Pod principal does not '
                             'match.')
        container = spec['containers'][0]
        qualification = self.resources.image.qualification
        expected_resources = {
            'requests': {
                'cpu': self.resources.pod_cpu_request,
                'memory': self.resources.pod_memory_request,
            },
            'limits': {
                'cpu': self.resources.pod_cpu_limit,
                'memory': self.resources.pod_memory_limit,
            },
        }
        if (container['image'] != qualification.requested_reference or
                container['imagePullPolicy'] != self.resources.image_pull_policy
                or container['resources'] != expected_resources):
            raise ValueError('launch execution capsule Pod image or resource '
                             'projection does not match.')
        if self.post_provision.management_port != '46590':
            raise ValueError('launch execution capsule management port does '
                             'not match the Pod request.')

    @staticmethod
    def _validate_requested_semantic_projection(
            object_plan: ProviderKubernetesObjectPlanV1,
            request_body: JsonObject) -> None:
        """Require the exact request-side allocation projection for one role."""

        expected_semantic = request_body
        if object_plan.role is ProviderObjectRoleV1.HEAD_SERVICE:
            del expected_semantic['spec']['clusterIP']
        if (object_plan.requested_semantic.canonical_bytes
                != canonical_json_bytes(expected_semantic)):
            raise ValueError('launch execution capsule requested semantic does '
                             'not match the exact request normalization.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesExecutionCapsuleV1:
        _bounded_canonical_json_bytes(
            value,
            name='Kubernetes launch execution capsule',
            require_object=True,
            allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='Kubernetes launch execution capsule',
                                     keys=cls._KEYS)
        objects = raw['objects']
        if type(objects) is not list:
            raise TypeError('launch execution capsule objects must be a list.')
        if len(objects) != len(PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1):
            raise ValueError('launch execution capsule objects must contain '
                             'exactly three plans.')
        return cls(
            version=raw['version'],
            implementation_contract=raw['implementation_contract'],
            executor_cohort=ProviderAuthorityWorkerCohortV1.from_value(
                raw['executor_cohort']),
            config_projection=ProviderKubernetesConfigProjectionV1.from_value(
                raw['config_projection']),
            config_projection_sha256=raw['config_projection_sha256'],
            scope=ProviderKubernetesScopeV1.from_value(raw['scope']),
            principals=ProviderKubernetesPrincipalsV1.from_value(
                raw['principals']),
            prerequisites=_provider_kubernetes_prerequisite_inventory_from_value(
                raw['prerequisites'],
                name='launch execution capsule prerequisites'),
            request_identity=ProviderKubernetesRequestIdentityV1.from_value(
                raw['request_identity']),
            resources=ProviderKubernetesResourceContractV1.from_value(
                raw['resources']),
            renderer=ProviderKubernetesRendererV1.from_value(raw['renderer']),
            objects=tuple(
                ProviderKubernetesObjectPlanV1.from_value(item)
                for item in objects),
            post_provision=ProviderKubernetesPostProvisionV1.from_value(
                raw['post_provision']),
            endpoint=ProviderKubernetesEndpointContractV1.from_value(
                raw['endpoint']),
            scheduling=ProviderKubernetesSchedulingContractV1.from_value(
                raw['scheduling']),
            storage=ProviderKubernetesStorageContractV1.from_value(
                raw['storage']),
            metadata=ProviderKubernetesMetadataContractV1.from_value(
                raw['metadata']),
            security=ProviderKubernetesSecurityContractV1.from_value(
                raw['security']),
            topology=ProviderPodTopologyV1.from_value(raw['topology']),
            mutation_contract=(
                ProviderKubernetesLaunchMutationContractV1.from_value(
                    raw['mutation_contract'])))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'implementation_contract': self._IMPLEMENTATION_CONTRACT,
            'executor_cohort': self.executor_cohort.canonical_value(),
            'config_projection': self.config_projection.canonical_value(),
            'config_projection_sha256': self.config_projection_sha256,
            'scope': self.scope.canonical_value(),
            'principals': self.principals.canonical_value(),
            'prerequisites': [
                item.canonical_value() for item in self.prerequisites
            ],
            'request_identity': self.request_identity.canonical_value(),
            'resources': self.resources.canonical_value(),
            'renderer': self.renderer.canonical_value(),
            'objects': [item.canonical_value() for item in self.objects],
            'post_provision': self.post_provision.canonical_value(),
            'endpoint': self.endpoint.canonical_value(),
            'scheduling': self.scheduling.canonical_value(),
            'storage': self.storage.canonical_value(),
            'metadata': self.metadata.canonical_value(),
            'security': self.security.canonical_value(),
            'topology': self.topology.canonical_value(),
            'mutation_contract': self.mutation_contract.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchPolicySubjectV1(_CanonicalContract):
    """Closed launch policy projection bound by its enclosing config."""

    version: int
    source: ProviderLaunchSourceV1
    requested_target: ProviderLocatorV1
    resources: ProviderPodResourceSnapshotV1
    topology: ProviderPodTopologyV1
    execution_capsule_sha256: str
    replica_id_text: str
    security_group_scope: str
    admin_policy_mode: str
    managed_secrets_mode: str
    retry_until_up: bool
    exact_resources_override: bool
    backend: str
    optimize_target: str
    dryrun: bool
    no_setup: bool
    clone_disk_from: None
    fast: bool
    file_mounts_blob_id: None
    tls_material_ref: None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'source', 'requested_target', 'resources', 'topology',
        'execution_capsule_sha256', 'replica_env', 'security_group_scope',
        'admin_policy_mode', 'managed_secrets_mode', 'retry_until_up',
        'exact_resources_override', 'backend', 'optimize_target', 'dryrun',
        'no_setup', 'clone_disk_from', 'fast', 'file_mounts_blob_id',
        'tls_material_ref'
    })
    _REPLICA_ENV_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'SKYPILOT_SERVE_REPLICA_ID'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='launch policy subject version')
        for field, expected_type in (
            ('source', ProviderLaunchSourceV1),
            ('requested_target', ProviderLocatorV1),
            ('resources', ProviderPodResourceSnapshotV1),
            ('topology', ProviderPodTopologyV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'launch policy subject {field} has an invalid '
                                'type.')
        if not self.requested_target.is_authoritative_pod_locator:
            raise ValueError('launch policy subject requires the authoritative '
                             'Kubernetes Pod locator.')
        object.__setattr__(
            self, 'execution_capsule_sha256',
            _sha256(self.execution_capsule_sha256,
                    name='launch policy subject execution_capsule_sha256'))
        object.__setattr__(
            self, 'replica_id_text',
            _decimal_integer_text(self.replica_id_text,
                                  name='launch policy subject replica ID'))
        fixed_text = {
            'security_group_scope': 'not_applicable:kubernetes',
            'admin_policy_mode': 'absent_controller_and_executor',
            'managed_secrets_mode': 'absent',
            'backend': 'cloud_vm_ray',
            'optimize_target': 'cost',
        }
        for field, expected in fixed_text.items():
            if type(getattr(self, field)) is not str:
                raise TypeError(f'launch policy subject {field} must be text.')
            if getattr(self, field) != expected:
                raise ValueError(f'launch policy subject {field} is '
                                 'unsupported.')
        _boolean(self.retry_until_up,
                 name='launch policy subject retry_until_up')
        for field, expected in (
            ('exact_resources_override', True),
            ('dryrun', False),
            ('no_setup', False),
            ('fast', False),
        ):
            value = _boolean(getattr(self, field),
                             name=f'launch policy subject {field}')
            if value is not expected:
                raise ValueError(f'launch policy subject {field} has an '
                                 'unsupported value.')
        for field in ('clone_disk_from', 'file_mounts_blob_id',
                      'tls_material_ref'):
            if getattr(self, field) is not None:
                raise ValueError(f'launch policy subject {field} must be null.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchPolicySubjectV1:
        _bounded_canonical_json_bytes(value,
                                      name='launch policy subject',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='launch policy subject',
                                     keys=cls._KEYS)
        replica_env = _closed_object_shallow(
            raw['replica_env'],
            name='launch policy subject replica_env',
            keys=cls._REPLICA_ENV_KEYS)
        return cls(version=raw['version'],
                   source=ProviderLaunchSourceV1.from_value(raw['source']),
                   requested_target=ProviderLocatorV1.from_value(
                       raw['requested_target']),
                   resources=ProviderPodResourceSnapshotV1.from_value(
                       raw['resources']),
                   topology=ProviderPodTopologyV1.from_value(raw['topology']),
                   execution_capsule_sha256=raw['execution_capsule_sha256'],
                   replica_id_text=replica_env['SKYPILOT_SERVE_REPLICA_ID'],
                   security_group_scope=raw['security_group_scope'],
                   admin_policy_mode=raw['admin_policy_mode'],
                   managed_secrets_mode=raw['managed_secrets_mode'],
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
            'version': 1,
            'source': self.source.canonical_value(),
            'requested_target': self.requested_target.canonical_value(),
            'resources': self.resources.canonical_value(),
            'topology': self.topology.canonical_value(),
            'execution_capsule_sha256': self.execution_capsule_sha256,
            'replica_env': {
                'SKYPILOT_SERVE_REPLICA_ID': self.replica_id_text
            },
            'security_group_scope': 'not_applicable:kubernetes',
            'admin_policy_mode': 'absent_controller_and_executor',
            'managed_secrets_mode': 'absent',
            'retry_until_up': self.retry_until_up,
            'exact_resources_override': True,
            'backend': 'cloud_vm_ray',
            'optimize_target': 'cost',
            'dryrun': False,
            'no_setup': False,
            'clone_disk_from': None,
            'fast': False,
            'file_mounts_blob_id': None,
            'tls_material_ref': None,
        }


def _validate_provider_launch_resource_translation_v1(
    resources: ProviderPodResourceSnapshotV1,
    topology: ProviderPodTopologyV1,
    capsule: ProviderKubernetesExecutionCapsuleV1,
) -> None:
    """Require exact outer-resource translation into the launch capsule."""

    capsule_resources = capsule.resources
    expected_instance_type = (f'{capsule_resources.source_cpus}CPU--'
                              f'{capsule_resources.source_memory_gb}GB')
    if (resources.namespace != capsule.scope.namespace or
            resources.instance_type != expected_instance_type or
            resources.accelerator is not None or
            resources.cpus != capsule_resources.source_cpus or
            resources.memory != capsule_resources.source_memory_gb or
            resources.image_id
            != capsule_resources.image.qualification.requested_reference or
            resources.ports != capsule_resources.resources_ports or
            resources.labels or resources.use_spot or
            topology.canonical_bytes != capsule.topology.canonical_bytes or
            topology.application_port != capsule_resources.application_port or
            topology.resources_ports != capsule_resources.resources_ports):
        raise ValueError('launch resources do not match the exact execution '
                         'capsule translation.')


def project_provider_launch_policy_subject_v1(
    resource_identity: ProviderResourceIdentityV1,
    source: ProviderLaunchSourceV1,
    requested_target: ProviderLocatorV1,
    resources: ProviderPodResourceSnapshotV1,
    topology: ProviderPodTopologyV1,
    replica_id: int,
    retry_until_up: bool,
    capsule: ProviderKubernetesExecutionCapsuleV1,
) -> ProviderLaunchPolicySubjectV1:
    """Construct the only valid launch policy subject from typed preimages."""

    exact_inputs: tuple[tuple[str, Any, type[Any]], ...] = (
        ('resource_identity', resource_identity, ProviderResourceIdentityV1),
        ('source', source, ProviderLaunchSourceV1),
        ('requested_target', requested_target, ProviderLocatorV1),
        ('resources', resources, ProviderPodResourceSnapshotV1),
        ('topology', topology, ProviderPodTopologyV1),
        ('capsule', capsule, ProviderKubernetesExecutionCapsuleV1),
    )
    for name, value, expected_type in exact_inputs:
        if type(value) is not expected_type:
            raise TypeError(f'launch policy projector {name} has an invalid '
                            'type.')
    if type(replica_id) is not int:
        raise TypeError(
            'launch policy projector replica_id must be an integer.')
    if replica_id != resource_identity.replica_id:
        raise ValueError('launch policy projector replica ID does not match '
                         'resource identity.')
    _boolean(retry_until_up, name='launch policy projector retry_until_up')
    proof_identity = source.identity_canonicalization.context.input.resource_identity
    if proof_identity.canonical_bytes != resource_identity.canonical_bytes:
        raise ValueError('launch policy projector resource identity does not '
                         'match the source proof.')
    if (source.content.canonical_bytes
            != capsule.renderer.source.canonical_bytes):
        raise ValueError('launch policy projector source does not match the '
                         'execution capsule.')
    if source.content.workspace != capsule.config_projection.workspace:
        raise ValueError('launch policy projector source workspace does not '
                         'match the execution capsule.')
    proof = source.identity_canonicalization
    kubernetes = requested_target.kubernetes
    if kubernetes is None:
        raise ValueError('launch policy projector requires a Kubernetes '
                         'requested target.')
    projected_identity = project_provider_kubernetes_request_identity_v1(
        proof.effective_original_user, kubernetes.name_basis)
    if (proof.context.cohort_id != capsule.executor_cohort.cohort_id or
            proof.effective_user_hash != kubernetes.name_basis.frozen_user_hash
            or kubernetes.replica_incarnation_label != str(
                resource_identity.replica_incarnation) or
            projected_identity.canonical_bytes
            != capsule.request_identity.canonical_bytes):
        raise ValueError('launch policy projector request identity does not '
                         'match the source proof and target name basis.')
    if (requested_target.sky_cluster_name != kubernetes.name_basis.display_name
            or
            kubernetes.scope.canonical_bytes != capsule.scope.canonical_bytes or
            kubernetes.topology.canonical_bytes
            != capsule.topology.canonical_bytes or
            topology.canonical_bytes != kubernetes.topology.canonical_bytes):
        raise ValueError('launch policy projector topology does not match the '
                         'requested target and execution capsule.')
    if (resources.cluster_fingerprint_sha256
            != kubernetes.cluster_fingerprint_sha256 or
            resources.namespace != kubernetes.namespace or
            resources.namespace != capsule.scope.namespace or
            resources.namespace != capsule.config_projection.target_namespace):
        raise ValueError('launch policy projector requested target, resources, '
                         'and capsule scope do not match.')
    _validate_provider_launch_resource_translation_v1(resources, topology,
                                                      capsule)
    pod_spec = capsule.objects[2].request_body.canonical_value()['spec']
    container = pod_spec['containers'][0]
    replica_entries = [
        entry for entry in container['env'] if type(entry) is dict and
        entry.get('name') == 'SKYPILOT_SERVE_REPLICA_ID'
    ]
    if replica_entries[0]['value'] != str(replica_id):
        raise ValueError('launch policy projector replica ID does not match '
                         'the Pod request environment.')
    return ProviderLaunchPolicySubjectV1(
        version=1,
        source=source,
        requested_target=requested_target,
        resources=resources,
        topology=topology,
        execution_capsule_sha256=capsule.sha256,
        replica_id_text=str(replica_id),
        security_group_scope='not_applicable:kubernetes',
        admin_policy_mode='absent_controller_and_executor',
        managed_secrets_mode='absent',
        retry_until_up=retry_until_up,
        exact_resources_override=True,
        backend='cloud_vm_ray',
        optimize_target='cost',
        dryrun=False,
        no_setup=False,
        clone_disk_from=None,
        fast=False,
        file_mounts_blob_id=None,
        tls_material_ref=None)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesExecutionConfigV1(_CanonicalContract):
    """Launch capsule and policy proofs bound into one immutable graph."""

    version: int
    capsule: ProviderKubernetesExecutionCapsuleV1
    execution_capsule_sha256: str
    policy_subject: ProviderLaunchPolicySubjectV1
    policy_subject_sha256: str
    controller: ProviderPolicyBoundaryProofV1
    executor: ProviderPolicyBoundaryProofV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'capsule', 'execution_capsule_sha256', 'policy_subject',
        'policy_subject_sha256', 'policy'
    })
    _POLICY_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'controller', 'executor'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='launch execution config version')
        for field, expected_type in (
            ('capsule', ProviderKubernetesExecutionCapsuleV1),
            ('policy_subject', ProviderLaunchPolicySubjectV1),
            ('controller', ProviderPolicyBoundaryProofV1),
            ('executor', ProviderPolicyBoundaryProofV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'launch execution config {field} has an '
                                'invalid type.')
        capsule_sha256 = _sha256(
            self.execution_capsule_sha256,
            name='launch execution config execution_capsule_sha256')
        object.__setattr__(self, 'execution_capsule_sha256', capsule_sha256)
        if capsule_sha256 != self.capsule.sha256:
            raise ValueError('launch execution config capsule hash does not '
                             'match.')
        if self.policy_subject.execution_capsule_sha256 != capsule_sha256:
            raise ValueError('launch execution config policy subject is not '
                             'bound to its capsule.')
        subject_sha256 = _sha256(
            self.policy_subject_sha256,
            name='launch execution config policy_subject_sha256')
        object.__setattr__(self, 'policy_subject_sha256', subject_sha256)
        if subject_sha256 != self.policy_subject.sha256:
            raise ValueError('launch execution config policy subject hash does '
                             'not match.')
        subject = self.policy_subject
        resource_identity = (subject.source.identity_canonicalization.context.
                             input.resource_identity)
        projected_subject = project_provider_launch_policy_subject_v1(
            resource_identity, subject.source, subject.requested_target,
            subject.resources, subject.topology, resource_identity.replica_id,
            subject.retry_until_up, self.capsule)
        if projected_subject.canonical_bytes != subject.canonical_bytes:
            raise ValueError('launch execution config policy subject is not '
                             'byte-equal to its capsule projection.')
        if self.controller.boundary != 'serve_controller_prepare':
            raise ValueError('launch execution config controller proof is in '
                             'the wrong boundary slot.')
        if self.executor.boundary != 'api_executor_pre_io':
            raise ValueError('launch execution config executor proof is in the '
                             'wrong boundary slot.')
        for field, proof in (('controller', self.controller), ('executor',
                                                               self.executor)):
            if proof.config_projection_sha256 != self.capsule.config_projection_sha256:
                raise ValueError(
                    f'launch execution config {field} proof config '
                    'projection hash does not match.')
            if proof.policy_subject_sha256 != subject_sha256:
                raise ValueError(
                    f'launch execution config {field} proof policy '
                    'subject hash does not match.')
            if (proof.projection_before_sha256 != subject_sha256 or
                    proof.projection_after_sha256 != subject_sha256):
                raise ValueError(f'launch execution config {field} proof '
                                 'projection hashes do not match.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesExecutionConfigV1:
        _bounded_canonical_json_bytes(value,
                                      name='Kubernetes launch execution config',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='Kubernetes launch execution config',
                                     keys=cls._KEYS)
        policy = _closed_object_shallow(raw['policy'],
                                        name='launch execution config policy',
                                        keys=cls._POLICY_KEYS)
        return cls(version=raw['version'],
                   capsule=ProviderKubernetesExecutionCapsuleV1.from_value(
                       raw['capsule']),
                   execution_capsule_sha256=raw['execution_capsule_sha256'],
                   policy_subject=ProviderLaunchPolicySubjectV1.from_value(
                       raw['policy_subject']),
                   policy_subject_sha256=raw['policy_subject_sha256'],
                   controller=ProviderPolicyBoundaryProofV1.from_value(
                       policy['controller']),
                   executor=ProviderPolicyBoundaryProofV1.from_value(
                       policy['executor']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'capsule': self.capsule.canonical_value(),
            'execution_capsule_sha256': self.execution_capsule_sha256,
            'policy_subject': self.policy_subject.canonical_value(),
            'policy_subject_sha256': self.policy_subject_sha256,
            'policy': {
                'controller': self.controller.canonical_value(),
                'executor': self.executor.canonical_value(),
            },
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesDownExecutionCapsuleV1(_CanonicalContract):
    """Closed current-authority preimage for exact Kubernetes cleanup."""

    version: int
    implementation_contract: str
    executor_cohort: ProviderAuthorityWorkerCohortV1
    config_projection: ProviderKubernetesConfigProjectionV1
    config_projection_sha256: str
    scope: ProviderKubernetesScopeV1
    principals: ProviderKubernetesPrincipalsV1
    prerequisites: tuple[ProviderKubernetesPrerequisiteV1, ...]
    cleanup_target: ProviderKubernetesCleanupTargetV1
    cleanup_target_sha256: str
    mutation_contract: ProviderKubernetesDownMutationContractV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'implementation_contract', 'executor_cohort',
        'config_projection', 'config_projection_sha256', 'scope', 'principals',
        'prerequisites', 'cleanup_target', 'cleanup_target_sha256',
        'mutation_contract'
    })
    _IMPLEMENTATION_CONTRACT: ClassVar[
        str] = 'kubernetes_serve_exact_cleanup_v1'

    def __post_init__(self) -> None:
        _version_one(self.version, name='down execution capsule version')
        if type(self.implementation_contract) is not str:
            raise TypeError('down execution capsule implementation_contract '
                            'must be text.')
        if self.implementation_contract != self._IMPLEMENTATION_CONTRACT:
            raise ValueError('down execution capsule implementation_contract '
                             'is unsupported.')
        for field, expected_type in (
            ('executor_cohort', ProviderAuthorityWorkerCohortV1),
            ('config_projection', ProviderKubernetesConfigProjectionV1),
            ('scope', ProviderKubernetesScopeV1),
            ('principals', ProviderKubernetesPrincipalsV1),
            ('cleanup_target', ProviderKubernetesCleanupTargetV1),
            ('mutation_contract', ProviderKubernetesDownMutationContractV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'down execution capsule {field} has an '
                                'invalid type.')
        object.__setattr__(
            self, 'prerequisites',
            _provider_kubernetes_prerequisite_inventory_tuple(
                self.prerequisites,
                name='down execution capsule prerequisites'))
        projection_hash = _sha256(
            self.config_projection_sha256,
            name='down execution capsule config_projection_sha256')
        if projection_hash != self.config_projection.sha256:
            raise ValueError('down execution capsule config projection hash '
                             'does not match its complete preimage.')
        cleanup_hash = _sha256(
            self.cleanup_target_sha256,
            name='down execution capsule cleanup_target_sha256')
        if cleanup_hash != self.cleanup_target.sha256:
            raise ValueError('down execution capsule cleanup target hash does '
                             'not match its complete preimage.')
        object.__setattr__(self, 'config_projection_sha256', projection_hash)
        object.__setattr__(self, 'cleanup_target_sha256', cleanup_hash)
        self._validate_internal_projection()
        _ = self.canonical_bytes

    def _validate_internal_projection(self) -> None:
        config = self.config_projection
        scope = self.scope
        principals = self.principals
        if not scope.in_cluster:
            raise ValueError('down execution capsule scope must be in-cluster.')
        namespaces = (
            scope.namespace,
            config.target_namespace,
            principals.workload.namespace,
            principals.caller_authorization.rules.namespace,
            *(item.plan.namespace for item in self.cleanup_target.objects),
        )
        if any(namespace != scope.namespace for namespace in namespaces):
            raise ValueError('down execution capsule target namespaces are '
                             'not byte-equal.')
        caller_scope = (
            scope.caller_service_account_namespace,
            scope.caller_service_account_name,
            scope.caller_service_account_uid,
        )
        caller_principal = (principals.caller.namespace, principals.caller.name,
                            principals.caller.uid)
        workload_scope = (
            scope.workload_service_account_namespace,
            scope.workload_service_account_name,
            scope.workload_service_account_uid,
        )
        workload_principal = (principals.workload.namespace,
                              principals.workload.name, principals.workload.uid)
        if caller_scope != caller_principal or workload_scope != workload_principal:
            raise ValueError('down execution capsule principals do not match '
                             'the Kubernetes scope.')
        by_role = {item.role: item for item in self.prerequisites}
        authority_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.AUTHORITY_RELEASE_NAMESPACE]
        target_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.TARGET_NAMESPACE]
        kube_system_namespace = by_role[
            ProviderKubernetesPrerequisiteRoleV1.KUBE_SYSTEM_NAMESPACE]
        if (authority_namespace.name != self.executor_cohort.manifest.namespace
                or authority_namespace.name != principals.caller.namespace or
                self.executor_cohort.manifest.service_account_name
                != principals.caller.name or
                self.executor_cohort.service_account_uid
                != principals.caller.uid):
            raise ValueError('down execution capsule authority cohort, '
                             'Namespace, and caller principal do not match.')
        if (target_namespace.name != scope.namespace or
                target_namespace.uid != scope.target_namespace_uid or
                kube_system_namespace.name != 'kube-system' or
                kube_system_namespace.uid != scope.kube_system_namespace_uid):
            raise ValueError('down execution capsule Namespace prerequisites '
                             'do not match the Kubernetes scope.')
        for role, principal in (
            (ProviderKubernetesPrerequisiteRoleV1.CALLER_SERVICE_ACCOUNT,
             principals.caller),
            (ProviderKubernetesPrerequisiteRoleV1.WORKLOAD_SERVICE_ACCOUNT,
             principals.workload),
        ):
            prerequisite = by_role[role]
            if type(prerequisite.spec) is not (
                    ProviderKubernetesServiceAccountPrerequisiteSpecV1):
                raise ValueError('down execution capsule ServiceAccount '
                                 'prerequisite has an invalid spec.')
            if prerequisite.spec.projection.canonical_bytes != (
                    principal.canonical_bytes):
                raise ValueError('down execution capsule ServiceAccount '
                                 'prerequisite does not match its principal.')
        network_policy = by_role[
            ProviderKubernetesPrerequisiteRoleV1.ENDPOINT_NETWORK_POLICY]
        if network_policy.namespace != scope.namespace:
            raise ValueError('down execution capsule NetworkPolicy namespace '
                             'does not match the target namespace.')
        if (self.cleanup_target.handle is not None and
                self.cleanup_target.handle.provider_config.scope_sha256
                != scope.sha256):
            raise ValueError('down execution capsule cleanup handle scope does '
                             'not match the current Kubernetes scope.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesDownExecutionCapsuleV1:
        _bounded_canonical_json_bytes(value,
                                      name='Kubernetes down execution capsule',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='Kubernetes down execution capsule',
                                     keys=cls._KEYS)
        return cls(
            version=raw['version'],
            implementation_contract=raw['implementation_contract'],
            executor_cohort=ProviderAuthorityWorkerCohortV1.from_value(
                raw['executor_cohort']),
            config_projection=ProviderKubernetesConfigProjectionV1.from_value(
                raw['config_projection']),
            config_projection_sha256=raw['config_projection_sha256'],
            scope=ProviderKubernetesScopeV1.from_value(raw['scope']),
            principals=ProviderKubernetesPrincipalsV1.from_value(
                raw['principals']),
            prerequisites=_provider_kubernetes_prerequisite_inventory_from_value(
                raw['prerequisites'],
                name='down execution capsule prerequisites'),
            cleanup_target=ProviderKubernetesCleanupTargetV1.from_value(
                raw['cleanup_target']),
            cleanup_target_sha256=raw['cleanup_target_sha256'],
            mutation_contract=(
                ProviderKubernetesDownMutationContractV1.from_value(
                    raw['mutation_contract'])))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'implementation_contract': self._IMPLEMENTATION_CONTRACT,
            'executor_cohort': self.executor_cohort.canonical_value(),
            'config_projection': self.config_projection.canonical_value(),
            'config_projection_sha256': self.config_projection_sha256,
            'scope': self.scope.canonical_value(),
            'principals': self.principals.canonical_value(),
            'prerequisites': [
                item.canonical_value() for item in self.prerequisites
            ],
            'cleanup_target': self.cleanup_target.canonical_value(),
            'cleanup_target_sha256': self.cleanup_target_sha256,
            'mutation_contract': self.mutation_contract.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderDownPolicySubjectV1(_CanonicalContract):
    """Closed down policy projection bound by its enclosing config."""

    version: int
    requested_target: ProviderLocatorV1
    workspace: str
    prior_launch_basis_sha256: str
    cleanup_target_sha256: str
    execution_capsule_sha256: str
    admin_policy_mode: str
    managed_secrets_mode: str
    purge: bool
    graceful: bool
    graceful_timeout: None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'requested_target', 'workspace', 'prior_launch_basis_sha256',
        'cleanup_target_sha256', 'execution_capsule_sha256',
        'admin_policy_mode', 'managed_secrets_mode', 'purge', 'graceful',
        'graceful_timeout'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='down policy subject version')
        if type(self.requested_target) is not ProviderLocatorV1:
            raise TypeError('down policy subject requested_target has an '
                            'invalid type.')
        if not self.requested_target.is_authoritative_pod_locator:
            raise ValueError('down policy subject requires the authoritative '
                             'Kubernetes Pod locator.')
        object.__setattr__(
            self, 'workspace',
            _text(self.workspace, name='down_policy_subject.workspace'))
        for field in ('prior_launch_basis_sha256', 'cleanup_target_sha256',
                      'execution_capsule_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field),
                        name=f'down_policy_subject.{field}'))
        fixed_text = {
            'admin_policy_mode': 'absent_controller_and_executor',
            'managed_secrets_mode': 'absent',
        }
        for field, expected in fixed_text.items():
            if type(getattr(self, field)) is not str:
                raise TypeError(f'down policy subject {field} must be text.')
            if getattr(self, field) != expected:
                raise ValueError(f'down policy subject {field} is unsupported.')
        for field in ('purge', 'graceful'):
            value = _boolean(getattr(self, field),
                             name=f'down policy subject {field}')
            if value:
                raise ValueError(f'down policy subject {field} must be false.')
        if self.graceful_timeout is not None:
            raise ValueError('down policy subject graceful_timeout must be '
                             'null.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderDownPolicySubjectV1:
        _bounded_canonical_json_bytes(value,
                                      name='down policy subject',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='down policy subject',
                                     keys=cls._KEYS)
        return cls(version=raw['version'],
                   requested_target=ProviderLocatorV1.from_value(
                       raw['requested_target']),
                   workspace=raw['workspace'],
                   prior_launch_basis_sha256=raw['prior_launch_basis_sha256'],
                   cleanup_target_sha256=raw['cleanup_target_sha256'],
                   execution_capsule_sha256=raw['execution_capsule_sha256'],
                   admin_policy_mode=raw['admin_policy_mode'],
                   managed_secrets_mode=raw['managed_secrets_mode'],
                   purge=raw['purge'],
                   graceful=raw['graceful'],
                   graceful_timeout=raw['graceful_timeout'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'requested_target': self.requested_target.canonical_value(),
            'workspace': self.workspace,
            'prior_launch_basis_sha256': self.prior_launch_basis_sha256,
            'cleanup_target_sha256': self.cleanup_target_sha256,
            'execution_capsule_sha256': self.execution_capsule_sha256,
            'admin_policy_mode': 'absent_controller_and_executor',
            'managed_secrets_mode': 'absent',
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        }


def project_provider_down_policy_subject_v1(
    requested_target: ProviderLocatorV1,
    workspace: str,
    prior_launch_basis: PriorLaunchBasisV1,
    capsule: ProviderKubernetesDownExecutionCapsuleV1,
) -> ProviderDownPolicySubjectV1:
    """Construct the only valid down policy subject from typed preimages."""

    for field, value, expected_types in (
        ('requested_target', requested_target, (ProviderLocatorV1,)),
        ('prior_launch_basis', prior_launch_basis,
         (CompletedLaunchBasisV1, PartialLaunchCleanupBasisV1)),
        ('capsule', capsule, (ProviderKubernetesDownExecutionCapsuleV1,)),
    ):
        if type(value) not in expected_types:
            raise TypeError(f'down policy projector {field} has an invalid '
                            'type.')
    workspace = _text(workspace, name='down policy projector workspace')
    if prior_launch_basis.launch_requested_target.canonical_bytes != (
            requested_target.canonical_bytes):
        raise ValueError('down policy projector requested target does not '
                         'match its prior launch basis.')
    if prior_launch_basis.launch_workspace_identity.workspace != workspace:
        raise ValueError('down policy projector workspace does not match its '
                         'prior launch basis.')
    cleanup_target = capsule.cleanup_target
    _validate_prior_launch_cleanup_target_binding_v1(prior_launch_basis,
                                                     cleanup_target)
    cleanup_hash = cleanup_target.sha256
    if capsule.cleanup_target_sha256 != cleanup_hash:
        raise ValueError('down policy projector cleanup target hashes do not '
                         'match the complete input.')
    cleanup_target.validate_requested_target(requested_target)
    kubernetes = requested_target.kubernetes
    assert kubernetes is not None
    if (kubernetes.scope.canonical_bytes != capsule.scope.canonical_bytes or
            capsule.config_projection.workspace != workspace or
            capsule.config_projection.target_namespace != kubernetes.namespace):
        raise ValueError('down policy projector target, workspace, and capsule '
                         'scope do not match.')
    return ProviderDownPolicySubjectV1(
        version=1,
        requested_target=requested_target,
        workspace=workspace,
        prior_launch_basis_sha256=prior_launch_basis.sha256,
        cleanup_target_sha256=cleanup_hash,
        execution_capsule_sha256=capsule.sha256,
        admin_policy_mode='absent_controller_and_executor',
        managed_secrets_mode='absent',
        purge=False,
        graceful=False,
        graceful_timeout=None)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesDownExecutionConfigV1(_CanonicalContract):
    """Down capsule and policy proofs bound into one immutable graph."""

    version: int
    capsule: ProviderKubernetesDownExecutionCapsuleV1
    execution_capsule_sha256: str
    policy_subject: ProviderDownPolicySubjectV1
    policy_subject_sha256: str
    controller: ProviderPolicyBoundaryProofV1
    executor: ProviderPolicyBoundaryProofV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'capsule', 'execution_capsule_sha256', 'policy_subject',
        'policy_subject_sha256', 'policy'
    })
    _POLICY_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'controller', 'executor'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='down execution config version')
        for field, expected_type in (
            ('capsule', ProviderKubernetesDownExecutionCapsuleV1),
            ('policy_subject', ProviderDownPolicySubjectV1),
            ('controller', ProviderPolicyBoundaryProofV1),
            ('executor', ProviderPolicyBoundaryProofV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'down execution config {field} has an invalid '
                                'type.')
        capsule_hash = _sha256(
            self.execution_capsule_sha256,
            name='down execution config execution_capsule_sha256')
        if capsule_hash != self.capsule.sha256:
            raise ValueError('down execution config capsule hash does not '
                             'match its complete preimage.')
        subject_hash = _sha256(
            self.policy_subject_sha256,
            name='down execution config policy_subject_sha256')
        if subject_hash != self.policy_subject.sha256:
            raise ValueError('down execution config policy subject hash does '
                             'not match its complete preimage.')
        subject = self.policy_subject
        if (subject.execution_capsule_sha256 != capsule_hash or
                subject.cleanup_target_sha256
                != self.capsule.cleanup_target_sha256 or
                subject.workspace != self.capsule.config_projection.workspace):
            raise ValueError('down execution config policy subject is not '
                             'bound to its capsule.')
        self.capsule.cleanup_target.validate_requested_target(
            subject.requested_target)
        if self.controller.boundary != 'serve_controller_prepare':
            raise ValueError('down execution config controller proof is in '
                             'the wrong boundary slot.')
        if self.executor.boundary != 'api_executor_pre_io':
            raise ValueError('down execution config executor proof is in the '
                             'wrong boundary slot.')
        for field, proof in (('controller', self.controller), ('executor',
                                                               self.executor)):
            if proof.config_projection_sha256 != (
                    self.capsule.config_projection_sha256):
                raise ValueError(f'down execution config {field} proof config '
                                 'projection hash does not match.')
            if proof.policy_subject_sha256 != subject_hash:
                raise ValueError(f'down execution config {field} proof policy '
                                 'subject hash does not match.')
            if (proof.projection_before_sha256 != subject_hash or
                    proof.projection_after_sha256 != subject_hash):
                raise ValueError(f'down execution config {field} proof '
                                 'projection hashes do not match.')
        object.__setattr__(self, 'execution_capsule_sha256', capsule_hash)
        object.__setattr__(self, 'policy_subject_sha256', subject_hash)
        _ = self.canonical_bytes

    def validate_outer_projection(
        self,
        requested_target: ProviderLocatorV1,
        workspace: str,
        prior_launch_basis: PriorLaunchBasisV1,
    ) -> None:
        projected = project_provider_down_policy_subject_v1(
            requested_target, workspace, prior_launch_basis, self.capsule)
        if projected.canonical_bytes != self.policy_subject.canonical_bytes:
            raise ValueError('down execution config policy subject is not '
                             'byte-equal to its outer projection.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesDownExecutionConfigV1:
        _bounded_canonical_json_bytes(value,
                                      name='Kubernetes down execution config',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='Kubernetes down execution config',
                                     keys=cls._KEYS)
        policy = _closed_object_shallow(raw['policy'],
                                        name='down execution config policy',
                                        keys=cls._POLICY_KEYS)
        return cls(version=raw['version'],
                   capsule=ProviderKubernetesDownExecutionCapsuleV1.from_value(
                       raw['capsule']),
                   execution_capsule_sha256=raw['execution_capsule_sha256'],
                   policy_subject=ProviderDownPolicySubjectV1.from_value(
                       raw['policy_subject']),
                   policy_subject_sha256=raw['policy_subject_sha256'],
                   controller=ProviderPolicyBoundaryProofV1.from_value(
                       policy['controller']),
                   executor=ProviderPolicyBoundaryProofV1.from_value(
                       policy['executor']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'capsule': self.capsule.canonical_value(),
            'execution_capsule_sha256': self.execution_capsule_sha256,
            'policy_subject': self.policy_subject.canonical_value(),
            'policy_subject_sha256': self.policy_subject_sha256,
            'policy': {
                'controller': self.controller.canonical_value(),
                'executor': self.executor.canonical_value(),
            },
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchInvocationV1(_CanonicalContract):
    """Redacted provider-effective launch invocation."""

    source: ProviderLaunchSourceV1
    resources: ProviderPodResourceSnapshotV1
    topology: ProviderPodTopologyV1
    execution_config: ProviderKubernetesExecutionConfigV1
    replica_id_text: str
    security_group_scope: str
    admin_policy_mode: str
    managed_secrets_mode: str
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
        'source', 'resources', 'topology', 'execution_config', 'replica_env',
        'security_group_scope', 'admin_policy_mode', 'managed_secrets_mode',
        'retry_until_up', 'exact_resources_override', 'backend',
        'optimize_target', 'dryrun', 'no_setup', 'clone_disk_from', 'fast',
        'file_mounts_blob_id', 'tls_material_ref'
    })
    _REPLICA_ENV_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'SKYPILOT_SERVE_REPLICA_ID'})

    def __post_init__(self) -> None:
        if type(self.source) is not ProviderLaunchSourceV1:
            raise TypeError('launch.source has an invalid type.')
        if type(self.resources) is not ProviderPodResourceSnapshotV1:
            raise TypeError('launch.resources has an invalid type.')
        if type(self.topology) is not ProviderPodTopologyV1:
            raise TypeError('launch.topology has an invalid type.')
        if type(self.execution_config
               ) is not ProviderKubernetesExecutionConfigV1:
            raise TypeError('launch.execution_config has an invalid type.')
        replica_id_text = _decimal_integer_text(self.replica_id_text,
                                                name='launch replica ID')
        object.__setattr__(self, 'replica_id_text', replica_id_text)
        for field, expected in (
            ('security_group_scope', 'not_applicable:kubernetes'),
            ('admin_policy_mode', 'absent_controller_and_executor'),
            ('managed_secrets_mode', 'absent'),
            ('backend', 'cloud_vm_ray'),
            ('optimize_target', 'cost'),
        ):
            if type(getattr(self, field)) is not str:
                raise TypeError(f'launch.{field} must be text.')
            if getattr(self, field) != expected:
                raise ValueError(f'launch {field} is unsupported.')
        _boolean(self.retry_until_up, name='launch.retry_until_up')
        for field, expected in (
            ('exact_resources_override', True),
            ('dryrun', False),
            ('no_setup', False),
            ('fast', False),
        ):
            if _boolean(getattr(self, field),
                        name=f'launch.{field}') is not expected:
                raise ValueError(f'launch {field} has an unsupported value.')
        for field in ('clone_disk_from', 'file_mounts_blob_id',
                      'tls_material_ref'):
            if getattr(self, field) is not None:
                raise ValueError(f'launch {field} must be null.')
        subject = self.execution_config.policy_subject
        byte_equal_fields = (
            ('source', self.source, subject.source),
            ('resources', self.resources, subject.resources),
            ('topology', self.topology, subject.topology),
        )
        for field, outer, projected in byte_equal_fields:
            if outer.canonical_bytes != projected.canonical_bytes:
                raise ValueError(f'launch {field} is not byte-equal to the '
                                 'policy subject.')
        _validate_provider_launch_resource_translation_v1(
            self.resources, self.topology, self.execution_config.capsule)
        scalar_fields = ('replica_id_text', 'security_group_scope',
                         'admin_policy_mode', 'managed_secrets_mode',
                         'retry_until_up', 'exact_resources_override',
                         'backend', 'optimize_target', 'dryrun', 'no_setup',
                         'clone_disk_from', 'fast', 'file_mounts_blob_id',
                         'tls_material_ref')
        if any(
                getattr(self, field) != getattr(subject, field)
                for field in scalar_fields):
            raise ValueError('launch options are not byte-equal to the policy '
                             'subject.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchInvocationV1:
        _bounded_canonical_json_bytes(value,
                                      name='launch invocation',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='launch invocation',
                                     keys=cls._KEYS)
        replica_env = _closed_object_shallow(raw['replica_env'],
                                             name='launch.replica_env',
                                             keys=cls._REPLICA_ENV_KEYS)
        return cls(
            source=ProviderLaunchSourceV1.from_value(raw['source']),
            resources=ProviderPodResourceSnapshotV1.from_value(
                raw['resources']),
            topology=ProviderPodTopologyV1.from_value(raw['topology']),
            execution_config=ProviderKubernetesExecutionConfigV1.from_value(
                raw['execution_config']),
            replica_id_text=replica_env['SKYPILOT_SERVE_REPLICA_ID'],
            security_group_scope=raw['security_group_scope'],
            admin_policy_mode=raw['admin_policy_mode'],
            managed_secrets_mode=raw['managed_secrets_mode'],
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
            'topology': self.topology.canonical_value(),
            'execution_config': self.execution_config.canonical_value(),
            'replica_env': {
                'SKYPILOT_SERVE_REPLICA_ID': self.replica_id_text
            },
            'security_group_scope': 'not_applicable:kubernetes',
            'admin_policy_mode': 'absent_controller_and_executor',
            'managed_secrets_mode': 'absent',
            'retry_until_up': self.retry_until_up,
            'exact_resources_override': self.exact_resources_override,
            'backend': 'cloud_vm_ray',
            'optimize_target': 'cost',
            'dryrun': False,
            'no_setup': False,
            'clone_disk_from': None,
            'fast': False,
            'file_mounts_blob_id': None,
            'tls_material_ref': None,
        }

    @property
    def first_authority_cohort_redacted(self) -> bool:
        return True

    def validate_outer_projection(self,
                                  resource_identity: ProviderResourceIdentityV1,
                                  requested_target: ProviderLocatorV1) -> None:
        """Reproject from outer invocation fields and require exact equality."""

        projected = project_provider_launch_policy_subject_v1(
            resource_identity, self.source, requested_target, self.resources,
            self.topology, resource_identity.replica_id, self.retry_until_up,
            self.execution_config.capsule)
        if (projected.canonical_bytes
                != self.execution_config.policy_subject.canonical_bytes):
            raise ValueError('launch policy subject is not byte-equal to the '
                             'outer invocation projection.')


@dataclasses.dataclass(frozen=True)
class ProviderDownInvocationV1(_CanonicalContract):
    """Identity-fenced provider-effective down invocation."""

    cluster_name: str
    expected_cluster_record_uuid: uuid.UUID
    workspace: str
    prior_launch_basis: PriorLaunchBasisV1
    execution_config: ProviderKubernetesDownExecutionConfigV1
    purge: bool
    graceful: bool
    graceful_timeout: None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'cluster_name', 'expected_cluster_record_uuid', 'workspace',
        'prior_launch_basis', 'execution_config', 'purge', 'graceful',
        'graceful_timeout'
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
        if type(self.prior_launch_basis) not in (CompletedLaunchBasisV1,
                                                 PartialLaunchCleanupBasisV1):
            raise TypeError('down prior_launch_basis has an invalid type.')
        if type(self.execution_config) is not (
                ProviderKubernetesDownExecutionConfigV1):
            raise TypeError('down execution_config has an invalid type.')
        for name, value in (('purge', self.purge), ('graceful', self.graceful)):
            _boolean(value, name=f'down.{name}')
            if value:
                raise ValueError(f'down {name} must be false.')
        if self.graceful_timeout is not None:
            raise ValueError('down graceful_timeout must be null.')
        self.validate_outer_projection(
            self.prior_launch_basis.launch_requested_target)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderDownInvocationV1:
        _bounded_canonical_json_bytes(value,
                                      name='down invocation',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_object_shallow(value,
                                     name='down invocation',
                                     keys=cls._KEYS)
        return cls(cluster_name=raw['cluster_name'],
                   expected_cluster_record_uuid=_uuid(
                       raw['expected_cluster_record_uuid'],
                       name='down.expected_cluster_record_uuid'),
                   workspace=raw['workspace'],
                   prior_launch_basis=prior_launch_basis_from_value_v1(
                       raw['prior_launch_basis']),
                   execution_config=(
                       ProviderKubernetesDownExecutionConfigV1.from_value(
                           raw['execution_config'])),
                   purge=raw['purge'],
                   graceful=raw['graceful'],
                   graceful_timeout=raw['graceful_timeout'])

    def canonical_value(self) -> JsonObject:
        return {
            'cluster_name': self.cluster_name,
            'expected_cluster_record_uuid': str(
                self.expected_cluster_record_uuid),
            'workspace': self.workspace,
            'prior_launch_basis': self.prior_launch_basis.canonical_value(),
            'execution_config': self.execution_config.canonical_value(),
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        }

    def validate_outer_projection(self,
                                  requested_target: ProviderLocatorV1) -> None:
        """Require exact equality to target, basis, capsule, and policy."""

        if type(requested_target) is not ProviderLocatorV1:
            raise TypeError('down outer requested target has an invalid type.')
        basis = self.prior_launch_basis
        cleanup_target = self.execution_config.capsule.cleanup_target
        if requested_target.canonical_bytes != (
                basis.launch_requested_target.canonical_bytes):
            raise ValueError('down requested target is not byte-equal to its '
                             'prior launch basis.')
        if (self.cluster_name != requested_target.sky_cluster_name or
                self.cluster_name != cleanup_target.cluster_name or
                self.expected_cluster_record_uuid
                != requested_target.sky_cluster_record_uuid or
                self.expected_cluster_record_uuid
                != cleanup_target.cluster_record_uuid):
            raise ValueError('down cluster identity does not match target and '
                             'cleanup evidence.')
        if self.workspace != basis.launch_workspace_identity.workspace:
            raise ValueError('down workspace does not match its prior launch '
                             'basis.')
        self.execution_config.validate_outer_projection(requested_target,
                                                        self.workspace, basis)


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
            action_kind = _action_kind(self.action_kind,
                                       name='invocation.action_kind')
        except (TypeError, ValueError) as e:
            raise ValueError('invocation action kind is unsupported.') from e
        object.__setattr__(self, 'action_kind', action_kind)
        if type(self.resource_identity) is not ProviderResourceIdentityV1:
            raise TypeError('invocation resource identity has an invalid type.')
        if type(self.requested_target) is not ProviderLocatorV1:
            raise TypeError('invocation requested target has an invalid type.')
        if self.requested_target.profile is not profile:
            raise ValueError('invocation and locator profiles do not match.')
        if (self.requested_target.kubernetes is not None and
                self.requested_target.kubernetes.replica_incarnation_label
                != str(self.resource_identity.replica_incarnation)):
            raise ValueError('Kubernetes replica label does not match the '
                             'resource identity.')
        if action_kind is kernel_actions.ActionKind.LAUNCH:
            if type(self.launch) is not ProviderLaunchInvocationV1 or (
                    self.down is not None):
                raise ValueError('launch invocation requires only launch.')
            launch = self.launch
            if (launch.source.content.service_incarnation
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
            launch.validate_outer_projection(self.resource_identity,
                                             self.requested_target)
        else:
            if type(self.down) is not ProviderDownInvocationV1 or (self.launch
                                                                   is not None):
                raise ValueError('down invocation requires only down.')
            if (self.down.cluster_name != self.requested_target.sky_cluster_name
                    or self.down.expected_cluster_record_uuid
                    != self.requested_target.sky_cluster_record_uuid):
                raise ValueError('down invocation does not match the requested '
                                 'target.')
            self.down.validate_outer_projection(self.requested_target)
            prior_identity = self.down.prior_launch_basis.launch_resource_identity
            stable_identity_fields = ('service_hash', 'service_incarnation',
                                      'replica_id', 'replica_incarnation')
            if any(
                    getattr(self.resource_identity, field) != getattr(
                        prior_identity, field)
                    for field in stable_identity_fields):
                raise ValueError('down invocation resource identity does not '
                                 'match its prior launch identity.')
            if self.resource_identity.desired_generation != (
                    prior_identity.desired_generation + 1):
                raise ValueError('down invocation generation must immediately '
                                 'follow its prior launch generation.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLifecycleInvocationV1:
        # Preserve the action-kind discriminator's exact wire-type error before
        # walking the remainder of a potentially hostile nested envelope.
        shallow = _closed_object_shallow(value,
                                         name='provider lifecycle invocation',
                                         keys=cls._KEYS)
        raw_action_kind = shallow['action_kind']
        if type(raw_action_kind) is not str:
            raise TypeError('invocation.action_kind must be text.')
        _action_kind(raw_action_kind, name='invocation.action_kind')
        _bounded_canonical_json_bytes(value,
                                      name='provider lifecycle invocation',
                                      require_object=True,
                                      allow_empty_strings=True)
        raw = _closed_action_kind_object(
            value,
            name='provider lifecycle invocation',
            keys=cls._KEYS,
            action_kind_name='invocation.action_kind')
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

    @property
    def executor_cohort(self) -> ProviderAuthorityWorkerCohortV1:
        """Return the sole complete executor cohort frozen in the capsule."""

        if self.action_kind is kernel_actions.ActionKind.LAUNCH:
            return self.require_launch(
            ).execution_config.capsule.executor_cohort
        return self.require_down().execution_config.capsule.executor_cohort

    def validate_prepared_worker_cohort_reference_v1(
        self,
        service_name: str,
        controller_owner_fence: str,
        lifecycle_epoch: int,
        reference: WorkerCohortReferenceInputV1,
        locked_cohort: ProviderAuthorityWorkerCohortV1,
    ) -> None:
        """Bind a PREPARING reference to this exact immutable invocation.

        The locked cohort is authoritative only as an equality fence: no field
        may be populated from it.  Launch reconstructs the entire reference
        from its post-auth canonicalization context, including the preparation
        capability commitment.  Down has no identity-canonicalization call, so
        its capability commitment remains reference-owned while every other
        field is reconstructed from the invocation and locked service fences.
        """

        service_name = _text(service_name,
                             name='prepared reference service_name',
                             maximum_bytes=_MAX_SERVICE_NAME_BYTES)
        controller_owner_fence = _text(
            controller_owner_fence,
            name='prepared reference controller_owner_fence')
        lifecycle_epoch = _positive_integer(
            lifecycle_epoch, name='prepared reference lifecycle_epoch')
        if type(reference) is not WorkerCohortReferenceInputV1:
            raise TypeError('prepared worker cohort reference has an invalid '
                            'type.')
        if type(locked_cohort) is not ProviderAuthorityWorkerCohortV1:
            raise TypeError('locked worker cohort has an invalid type.')

        embedded_cohort = self.executor_cohort
        if (embedded_cohort.canonical_bytes != locked_cohort.canonical_bytes or
                embedded_cohort.cohort_id != reference.cohort_id):
            raise ValueError('prepared reference executor cohort does not '
                             'match the locked cohort canonical bytes and ID.')
        identity = self.resource_identity
        if self.action_kind is kernel_actions.ActionKind.LAUNCH:
            context = (
                self.require_launch().source.identity_canonicalization.context)
            if (context.input.service_name != service_name or
                    context.input.resource_identity.canonical_bytes
                    != identity.canonical_bytes or
                    context.decision_id != self.action_id or
                    context.cohort_id != embedded_cohort.cohort_id or
                    context.controller_owner_fence != controller_owner_fence or
                    context.lifecycle_epoch != lifecycle_epoch):
                raise ValueError('launch preparation context does not match '
                                 'the invocation, cohort, or locked service '
                                 'fences.')
            capability_sha256 = context.preparation_capability_sha256
        else:
            capability_sha256 = reference.preparation_capability_sha256

        expected = WorkerCohortReferenceInputV1(
            version=1,
            decision_id=self.action_id,
            cohort_id=embedded_cohort.cohort_id,
            service_hash=identity.service_hash,
            replica_incarnation=identity.replica_incarnation,
            desired_generation=identity.desired_generation,
            action_type=self.action_kind,
            controller_owner_fence=controller_owner_fence,
            lifecycle_epoch=lifecycle_epoch,
            preparation_capability_sha256=capability_sha256)
        if expected.canonical_bytes != reference.canonical_bytes:
            raise ValueError('prepared reference does not match every '
                             'decision/resource/service/replica/generation/'
                             'action/owner/lifecycle field.')

    def as_launch(self) -> ProviderLaunchLifecycleInvocationV1:
        """Return a statically refined launch view with identical bytes."""

        self.require_launch()
        return ProviderLaunchLifecycleInvocationV1.from_value(
            self.canonical_value())

    def as_down(self) -> ProviderDownLifecycleInvocationV1:
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
    def from_value(cls, value: Any) -> ProviderLaunchLifecycleInvocationV1:
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
    def from_value(cls, value: Any) -> ProviderDownLifecycleInvocationV1:
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
class ServeLegacyDownRequestV1(_CanonicalContract):
    """Exact legacy SDK arguments retained by a launch-cleanup child."""

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
                  name='legacy cleanup down.cluster_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'expected_cluster_record_uuid',
            _uuid(self.expected_cluster_record_uuid,
                  name='legacy cleanup down.expected_cluster_record_uuid'))
        object.__setattr__(
            self, 'workspace',
            _text(self.workspace, name='legacy cleanup down.workspace'))
        for field in ('purge', 'graceful'):
            if _boolean(getattr(self, field),
                        name=f'legacy cleanup down.{field}'):
                raise ValueError(f'legacy cleanup down {field} must be false.')
        if self.graceful_timeout is not None:
            raise ValueError('legacy cleanup down graceful_timeout must be '
                             'null.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeLegacyDownRequestV1:
        raw = _closed_object(value,
                             name='legacy cleanup down request',
                             keys=cls._KEYS)
        return cls(cluster_name=raw['cluster_name'],
                   expected_cluster_record_uuid=_uuid(
                       raw['expected_cluster_record_uuid'],
                       name=('legacy cleanup down.'
                             'expected_cluster_record_uuid')),
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
class ServeLegacyLaunchCleanupDownInvocationV1(_CanonicalContract):
    """Child-only fingerprint for the legacy cleanup between launch retries."""

    version: int
    contract: str
    request_role: ShadowRequestRole
    effect_kind: kernel_actions.ActionKind
    profile: ProviderProfile
    redaction_profile: str
    parent_launch_action_id: uuid.UUID
    parent_launch_request_payload_sha256: str
    resource_identity: ProviderResourceIdentityV1
    requested_target: ProviderLocatorV1
    legacy_down_request: ServeLegacyDownRequestV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'contract', 'request_role', 'effect_kind', 'profile',
        'redaction_profile', 'parent_launch_action_id',
        'parent_launch_request_payload_sha256', 'resource_identity',
        'requested_target', 'legacy_down_request'
    })

    def __post_init__(self) -> None:
        _version_one(self.version,
                     name='legacy launch cleanup invocation version')
        if self.contract != 'serve_legacy_launch_cleanup_down_v1':
            raise ValueError('legacy launch cleanup invocation contract is '
                             'unsupported.')
        request_role = _enum_value(
            ShadowRequestRole,
            self.request_role,
            name='legacy launch cleanup invocation.request_role')
        object.__setattr__(self, 'request_role', request_role)
        if request_role is not ShadowRequestRole.LAUNCH_CLEANUP_DOWN:
            raise ValueError('legacy launch cleanup invocation requires the '
                             'cleanup request role.')
        try:
            effect_kind = _action_kind(
                self.effect_kind,
                name='legacy launch cleanup invocation.effect_kind')
        except (TypeError, ValueError) as e:
            raise ValueError('legacy launch cleanup invocation effect kind is '
                             'unsupported.') from e
        object.__setattr__(self, 'effect_kind', effect_kind)
        if effect_kind is not kernel_actions.ActionKind.DOWN:
            raise ValueError('legacy launch cleanup invocation effect kind '
                             'must be down.')
        profile = _enum_value(ProviderProfile,
                              self.profile,
                              name='legacy launch cleanup invocation.profile')
        object.__setattr__(self, 'profile', profile)
        if profile is not ProviderProfile.POD_CLUSTER_V1:
            raise ValueError('legacy launch cleanup invocation profile is '
                             'unsupported.')
        if self.redaction_profile != 'provider_lifecycle_redaction_v1':
            raise ValueError('legacy launch cleanup invocation redaction '
                             'profile is unsupported.')
        object.__setattr__(
            self, 'parent_launch_action_id',
            _uuid(self.parent_launch_action_id,
                  name=('legacy launch cleanup invocation.'
                        'parent_launch_action_id')))
        object.__setattr__(
            self, 'parent_launch_request_payload_sha256',
            _sha256(self.parent_launch_request_payload_sha256,
                    name=('legacy launch cleanup invocation.'
                          'parent_launch_request_payload_sha256')))
        if type(self.resource_identity) is not ProviderResourceIdentityV1:
            raise TypeError('legacy launch cleanup resource identity has an '
                            'invalid type.')
        if type(self.requested_target) is not ProviderLocatorV1:
            raise TypeError('legacy launch cleanup requested target has an '
                            'invalid type.')
        if type(self.legacy_down_request) is not ServeLegacyDownRequestV1:
            raise TypeError('legacy launch cleanup down request has an '
                            'invalid type.')
        expected_action_id = self.resource_identity.action_identity(
            kernel_actions.ActionKind.LAUNCH).action_id
        if self.parent_launch_action_id != expected_action_id:
            raise ValueError('legacy launch cleanup parent action ID does not '
                             'match its resource identity.')
        if self.requested_target.profile is not profile:
            raise ValueError('legacy launch cleanup target profile differs.')
        kubernetes = self.requested_target.kubernetes
        if (kubernetes is not None and kubernetes.replica_incarnation_label
                != str(self.resource_identity.replica_incarnation)):
            raise ValueError('legacy launch cleanup target does not match its '
                             'replica incarnation.')
        request = self.legacy_down_request
        if (request.cluster_name != self.requested_target.sky_cluster_name or
                request.expected_cluster_record_uuid
                != self.requested_target.sky_cluster_record_uuid):
            raise ValueError('legacy launch cleanup down request does not '
                             'match its frozen target.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeLegacyLaunchCleanupDownInvocationV1:
        raw = _closed_object(value,
                             name='legacy launch cleanup invocation',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   request_role=_enum_value(
                       ShadowRequestRole,
                       raw['request_role'],
                       name='legacy launch cleanup invocation.request_role'),
                   effect_kind=_action_kind(
                       raw['effect_kind'],
                       name='legacy launch cleanup invocation.effect_kind'),
                   profile=_enum_value(
                       ProviderProfile,
                       raw['profile'],
                       name='legacy launch cleanup invocation.profile'),
                   redaction_profile=raw['redaction_profile'],
                   parent_launch_action_id=_uuid(
                       raw['parent_launch_action_id'],
                       name=('legacy launch cleanup invocation.'
                             'parent_launch_action_id')),
                   parent_launch_request_payload_sha256=raw[
                       'parent_launch_request_payload_sha256'],
                   resource_identity=ProviderResourceIdentityV1.from_value(
                       raw['resource_identity']),
                   requested_target=ProviderLocatorV1.from_value(
                       raw['requested_target']),
                   legacy_down_request=ServeLegacyDownRequestV1.from_value(
                       raw['legacy_down_request']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': 'serve_legacy_launch_cleanup_down_v1',
            'request_role': ShadowRequestRole.LAUNCH_CLEANUP_DOWN.value,
            'effect_kind': kernel_actions.ActionKind.DOWN.value,
            'profile': ProviderProfile.POD_CLUSTER_V1.value,
            'redaction_profile': 'provider_lifecycle_redaction_v1',
            'parent_launch_action_id': str(self.parent_launch_action_id),
            'parent_launch_request_payload_sha256':
                self.parent_launch_request_payload_sha256,
            'resource_identity': self.resource_identity.canonical_value(),
            'requested_target': self.requested_target.canonical_value(),
            'legacy_down_request': self.legacy_down_request.canonical_value(),
        }


ServeShadowAttemptInvocationV1 = (ProviderLifecycleInvocationV1 |
                                  ServeLegacyLaunchCleanupDownInvocationV1)


def serve_shadow_attempt_invocation_from_value_v1(
        value: Any,
        request_role: ShadowRequestRole) -> ServeShadowAttemptInvocationV1:
    """Decode the exact child union member selected by its persisted role."""

    role = _enum_value(ShadowRequestRole,
                       request_role,
                       name='shadow attempt request role')
    if role is ShadowRequestRole.LAUNCH_CLEANUP_DOWN:
        return ServeLegacyLaunchCleanupDownInvocationV1.from_value(value)
    return ProviderLifecycleInvocationV1.from_value(value)


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
    prior_launch_basis_sha256: str | None
    prior_cleanup_target_sha256: str | None
    request_payload_sha256: str
    redaction_profile: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'profile', 'action_kind', 'resource_identity',
        'placement_decision_sha256', 'resources_snapshot_sha256',
        'workspace_identity_sha256', 'requested_target',
        'prior_launch_basis_sha256', 'prior_cleanup_target_sha256',
        'request_payload_sha256', 'redaction_profile'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='provider plan version')
        profile = (self.profile if isinstance(self.profile, ProviderProfile)
                   else _enum_value(
                       ProviderProfile, self.profile, name='plan.profile'))
        object.__setattr__(self, 'profile', profile)
        try:
            action_kind = _action_kind(self.action_kind,
                                       name='plan.action_kind')
        except (TypeError, ValueError) as e:
            raise ValueError('provider plan action kind is unsupported.') from e
        object.__setattr__(self, 'action_kind', action_kind)
        if type(self.resource_identity) is not ProviderResourceIdentityV1:
            raise TypeError('plan resource identity has an invalid type.')
        for field in ('placement_decision_sha256', 'resources_snapshot_sha256',
                      'workspace_identity_sha256', 'request_payload_sha256'):
            object.__setattr__(
                self, field, _sha256(getattr(self, field),
                                     name=f'plan.{field}'))
        if type(self.requested_target) is not ProviderLocatorV1:
            raise TypeError('plan requested target has an invalid type.')
        if self.requested_target.profile is not profile:
            raise ValueError('plan and locator profiles do not match.')
        if (self.requested_target.kubernetes is not None and
                self.requested_target.kubernetes.replica_incarnation_label
                != str(self.resource_identity.replica_incarnation)):
            raise ValueError('Kubernetes replica label does not match plan '
                             'resource identity.')
        for field in ('prior_launch_basis_sha256',
                      'prior_cleanup_target_sha256'):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field,
                                   _sha256(value, name=f'plan.{field}'))
        if action_kind is kernel_actions.ActionKind.LAUNCH:
            if (self.prior_launch_basis_sha256 is not None or
                    self.prior_cleanup_target_sha256 is not None):
                raise ValueError('launch plan prior hashes must be null.')
        else:
            if (self.prior_launch_basis_sha256 is None or
                    self.prior_cleanup_target_sha256 is None):
                raise ValueError('down plan prior hashes must be nonnull.')
        if self.redaction_profile != 'provider_lifecycle_redaction_v1':
            raise ValueError('provider plan redaction profile is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLifecyclePlanV1:
        raw = _closed_action_kind_object(value,
                                         name='provider lifecycle plan',
                                         keys=cls._KEYS,
                                         action_kind_name='plan.action_kind')
        return cls(
            version=raw['version'],
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
            prior_launch_basis_sha256=raw['prior_launch_basis_sha256'],
            prior_cleanup_target_sha256=raw['prior_cleanup_target_sha256'],
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
            'prior_launch_basis_sha256': self.prior_launch_basis_sha256,
            'prior_cleanup_target_sha256': self.prior_cleanup_target_sha256,
            'request_payload_sha256': self.request_payload_sha256,
            'redaction_profile': self.redaction_profile,
        }

    @property
    def action_id(self) -> uuid.UUID:
        return self.resource_identity.action_identity(
            self.action_kind).action_id

    def validate_invocation(self,
                            invocation: ProviderLifecycleInvocationV1) -> None:
        if type(invocation) is not ProviderLifecycleInvocationV1:
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
        if invocation.down is not None:
            basis = invocation.down.prior_launch_basis
            cleanup = invocation.down.execution_config.capsule.cleanup_target
            if self.prior_launch_basis_sha256 != basis.sha256:
                raise ValueError('down plan prior launch basis hash does not '
                                 'match the invocation preimage.')
            if self.prior_cleanup_target_sha256 != cleanup.sha256:
                raise ValueError('down plan cleanup target hash does not match '
                                 'the invocation capsule preimage.')
            _validate_prior_launch_cleanup_target_binding_v1(basis, cleanup)
            if basis.launch_requested_target.canonical_bytes != (
                    self.requested_target.canonical_bytes):
                raise ValueError('down plan requested target is not byte-equal '
                                 'to its prior launch basis.')
            stable_identity_fields = ('service_hash', 'service_incarnation',
                                      'replica_id', 'replica_incarnation')
            if any(
                    getattr(self.resource_identity, field) != getattr(
                        basis.launch_resource_identity, field)
                    for field in stable_identity_fields):
                raise ValueError('down plan resource identity does not match '
                                 'its prior launch identity.')
            if self.resource_identity.desired_generation != (
                    basis.launch_resource_identity.desired_generation + 1):
                raise ValueError('down plan generation must immediately follow '
                                 'its prior launch generation.')
            if self.resources_snapshot_sha256 != basis.launch_resources.sha256:
                raise ValueError('down plan resources hash does not match its '
                                 'prior launch resources.')
            if self.workspace_identity_sha256 != (
                    basis.launch_workspace_identity.sha256):
                raise ValueError('down plan workspace hash does not match its '
                                 'prior launch workspace identity.')


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
        if type(self.provider_plan) is not ProviderLifecyclePlanV1:
            raise TypeError('provider_plan has an invalid type.')
        if type(self.invocation) is not ProviderLifecycleInvocationV1:
            raise TypeError('invocation has an invalid type.')
        if self.provider_plan.action_id != self.invocation.action_id:
            raise ValueError('provider plan and invocation action IDs differ.')
        self.provider_plan.validate_invocation(self.invocation)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeReplicaActionSpecV1:
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

        if type(provider_plan) is not ProviderLifecyclePlanV1:
            raise TypeError('parent provider_plan has an invalid type.')
        if provider_plan.canonical_bytes != self.provider_plan.canonical_bytes:
            raise ValueError('parent provider plan is not byte-equal to the '
                             'immutable spec provider plan.')

    def launch_cleanup_down_invocation(
            self) -> ServeLegacyLaunchCleanupDownInvocationV1:
        """Derive the sole non-primary invocation allowed by this wrapper."""

        if self.invocation.action_kind is not kernel_actions.ActionKind.LAUNCH:
            raise ValueError('cleanup down requires a launch action spec.')
        launch = self.invocation.launch
        if launch is None:
            raise ValueError('cleanup down requires a launch invocation.')
        target = self.invocation.requested_target
        return ServeLegacyLaunchCleanupDownInvocationV1(
            version=1,
            contract='serve_legacy_launch_cleanup_down_v1',
            request_role=ShadowRequestRole.LAUNCH_CLEANUP_DOWN,
            effect_kind=kernel_actions.ActionKind.DOWN,
            profile=self.invocation.profile,
            redaction_profile=self.invocation.redaction_profile,
            parent_launch_action_id=self.action_id,
            parent_launch_request_payload_sha256=self.invocation.sha256,
            resource_identity=self.invocation.resource_identity,
            requested_target=target,
            legacy_down_request=ServeLegacyDownRequestV1(
                cluster_name=target.sky_cluster_name,
                expected_cluster_record_uuid=target.sky_cluster_record_uuid,
                workspace=launch.source.content.workspace,
                purge=False,
                graceful=False,
                graceful_timeout=None))

    def validate_shadow_child_invocation(
            self, role: ShadowRequestRole,
            invocation: ServeShadowAttemptInvocationV1) -> None:
        """Require the exact primary invocation or derived cleanup exception."""

        if type(role) is not ShadowRequestRole:
            raise TypeError('shadow request role has an invalid type.')
        if role is ShadowRequestRole.LAUNCH_CLEANUP_DOWN:
            if type(invocation) is not (
                    ServeLegacyLaunchCleanupDownInvocationV1):
                raise TypeError('cleanup child invocation has an invalid '
                                'type.')
            expected: ServeShadowAttemptInvocationV1 = (
                self.launch_cleanup_down_invocation())
        else:
            if not isinstance(invocation, ProviderLifecycleInvocationV1):
                raise TypeError('primary child invocation has an invalid '
                                'type.')
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
    def from_value(cls, value: Any) -> ProviderErrorV1:
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
    def from_value(cls, value: Any) -> ProviderSubmissionV1:
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
    def from_value(cls, value: Any) -> ProviderLifecycleObservationV1:
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
    def from_value(cls, value: Any) -> ServeReplicaActionOutcomeV1:
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
            self, invocation: ServeShadowAttemptInvocationV1) -> None:
        """Require action-specific evidence before accepting this outcome.

        Provider submission acknowledgement is not proof that a resource
        reached its requested lifecycle state.  A successful launch therefore
        requires an authoritative, identity-matched, ready observation; a
        successful down requires authoritative absence.  Non-success outcomes
        retain their closed retry/error semantics, but any attached observation
        must still belong to the invocation's frozen target.
        """

        if (not isinstance(invocation, ProviderLifecycleInvocationV1) and
                type(invocation)
                is not ServeLegacyLaunchCleanupDownInvocationV1):
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
        action_kind = (invocation.action_kind if isinstance(
            invocation, ProviderLifecycleInvocationV1) else
                       invocation.effect_kind)
        if action_kind is kernel_actions.ActionKind.LAUNCH:
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
            action_kind = _action_kind(self.action_kind,
                                       name='projection.action_kind')
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
    def from_value(cls, value: Any) -> ServeShadowProjectionV1:
        raw = _closed_action_kind_object(
            value,
            name='Serve shadow projection',
            keys=cls._KEYS,
            action_kind_name='projection.action_kind')
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
    def from_value(cls, value: Any) -> ServeShadowRetryDecisionV1:
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
