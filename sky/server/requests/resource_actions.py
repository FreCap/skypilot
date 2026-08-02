"""Typed contracts for durable resource-scoped actions.

This module deliberately contains no database or SkyServe imports.  It owns
the versioned canonical encodings used by the PostgreSQL action journal and
the small set of values exchanged with its store.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import datetime
import enum
import hashlib
import json
from typing import Any, Protocol, TYPE_CHECKING
import unicodedata
import uuid

from sky.server.requests import registry as request_registry
from sky.server.requests import requests as requests_lib

if TYPE_CHECKING:
    import sqlalchemy

RESOURCE_ACTION_NAMESPACE = uuid.UUID('ffa24895-49b7-5f76-9a32-ff22809e4dff')
_MAX_CANONICAL_OBJECT_BYTES = 65536

JsonValue = Any
JsonObject = dict[str, JsonValue]


class ResourceActionError(RuntimeError):
    """Base class for a closed resource-action store failure."""


class ActionConflict(ResourceActionError):
    """An immutable identity or materialization commitment conflicted."""


class StaleRevision(ResourceActionError):
    """The action no longer matches the caller's observed revision."""


class ClaimLost(ResourceActionError):
    """The executing request no longer owns a live claim."""


class InvariantViolation(ResourceActionError):
    """Persisted action state violates the versioned storage contract."""


class KernelState(enum.Enum):
    """States owned by the generic resource-action kernel."""

    READY = 'READY'
    QUEUED = 'QUEUED'
    BLOCKED = 'BLOCKED'
    TERMINAL = 'TERMINAL'


class MutationBoundary(enum.Enum):
    """Request-attempt lifecycle, including terminal evidence settlement."""

    NOT_STARTED = 'NOT_STARTED'
    INTENT_COMMITTED = 'INTENT_COMMITTED'
    SUBMITTED_OR_AMBIGUOUS = 'SUBMITTED_OR_AMBIGUOUS'
    SETTLED = 'SETTLED'


class ProviderIOBoundary(enum.Enum):
    """Last provider-I/O watermark retained after attempt settlement."""

    NOT_STARTED = 'NOT_STARTED'
    INTENT_COMMITTED = 'INTENT_COMMITTED'
    SUBMITTED_OR_AMBIGUOUS = 'SUBMITTED_OR_AMBIGUOUS'


class ActionKind(enum.Enum):
    """Resource mutations supported by identity version 1."""

    LAUNCH = 'launch'
    DOWN = 'down'


def _normalize_text(value: str) -> str:
    return unicodedata.normalize('NFC', value)


def _normalize_json(value: Any) -> JsonValue:
    """Return the NFC-normalized JSON value used by the storage contract."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        raise TypeError('Canonical resource-action JSON forbids floats.')
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in value]
    if isinstance(value, Mapping):
        normalized: JsonObject = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError('Canonical JSON object keys must be strings.')
            normalized_key = _normalize_text(key)
            if normalized_key in normalized:
                raise ValueError('NFC normalization produced a duplicate JSON '
                                 f'key: {normalized_key!r}.')
            normalized[normalized_key] = _normalize_json(item)
        return normalized
    raise TypeError(f'Value {value!r} is not in the canonical JSON domain.')


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize one value with the exact resource-action JSON contract."""
    normalized = _normalize_json(value)
    return json.dumps(normalized,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def canonical_sha256(value: Any) -> str:
    """Return lowercase SHA-256 over canonical JSON bytes."""
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _canonical_object(value: Mapping[str, Any], *, name: str) -> JsonObject:
    normalized = _normalize_json(value)
    if not isinstance(normalized, dict):
        raise TypeError(f'{name} must be a JSON object.')
    encoded = canonical_json_bytes(normalized)
    if len(encoded) > _MAX_CANONICAL_OBJECT_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_CANONICAL_OBJECT_BYTES} bytes.')
    return normalized


def _uuid(value: uuid.UUID | str, *, name: str) -> uuid.UUID:
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as e:
        raise ValueError(f'{name} must be a UUID.') from e


def _bounded_text(value: str,
                  *,
                  name: str,
                  maximum_bytes: int,
                  allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f'{name} must be text.')
    normalized = _normalize_text(value)
    size = len(normalized.encode('utf-8'))
    if (not allow_empty and size == 0) or size > maximum_bytes:
        qualifier = f'1..{maximum_bytes}' if not allow_empty else (
            f'0..{maximum_bytes}')
        raise ValueError(f'{name} must be {qualifier} UTF-8 bytes.')
    return normalized


@dataclasses.dataclass(frozen=True)
class ResourceActionIdentity:
    """Stable SkyServe replica-action identity version 1."""

    service_hash: str
    service_incarnation: uuid.UUID
    replica_id: int
    replica_incarnation: uuid.UUID
    desired_generation: int
    action_kind: ActionKind

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'service_hash',
            _bounded_text(self.service_hash,
                          name='service_hash',
                          maximum_bytes=512))
        object.__setattr__(
            self, 'service_incarnation',
            _uuid(self.service_incarnation, name='service_incarnation'))
        object.__setattr__(
            self, 'replica_incarnation',
            _uuid(self.replica_incarnation, name='replica_incarnation'))
        if (not isinstance(self.replica_id, int) or
                isinstance(self.replica_id, bool) or self.replica_id < 0):
            raise ValueError('replica_id must be a nonnegative integer.')
        if (not isinstance(self.desired_generation, int) or
                isinstance(self.desired_generation, bool) or
                self.desired_generation <= 0):
            raise ValueError('desired_generation must be a positive integer.')
        try:
            raw_action_kind: Any = self.action_kind
            action_kind = (raw_action_kind if isinstance(
                raw_action_kind, ActionKind) else ActionKind(
                    _normalize_text(raw_action_kind)))
        except (TypeError, ValueError) as e:
            raise ValueError('action_kind must be launch or down.') from e
        object.__setattr__(self, 'action_kind', action_kind)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'domain': 'serve',
            'resource_type': 'replica',
            'service_hash': self.service_hash,
            'service_incarnation': str(self.service_incarnation),
            'replica_id': self.replica_id,
            'replica_incarnation': str(self.replica_incarnation),
            'desired_generation': self.desired_generation,
            'action_kind': self.action_kind.value,
        }

    def resource_identity_value(self) -> JsonObject:
        return {
            'version': 1,
            'service_hash': self.service_hash,
            'service_incarnation': str(self.service_incarnation),
            'replica_id': self.replica_id,
            'replica_incarnation': str(self.replica_incarnation),
        }

    @property
    def action_id(self) -> uuid.UUID:
        preimage = canonical_json_bytes(self.canonical_value()).decode('utf-8')
        return uuid.uuid5(RESOURCE_ACTION_NAMESPACE, preimage)

    @property
    def resource_identity(self) -> str:
        return canonical_json_bytes(
            self.resource_identity_value()).decode('utf-8')


def request_id_for_attempt(action_id: uuid.UUID | str, attempt: int) -> str:
    """Derive the one existing API request ID owned by an attempt."""
    parsed_action_id = _uuid(action_id, name='action_id')
    if (not isinstance(attempt, int) or isinstance(attempt, bool) or
            attempt <= 0):
        raise ValueError('attempt must be a positive integer.')
    return str(uuid.uuid5(parsed_action_id, f'attempt:{attempt}'))


@dataclasses.dataclass(frozen=True)
class NewResourceAction:
    """Immutable values admitted with a new logical action."""

    identity: ResourceActionIdentity
    immutable_spec: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'immutable_spec',
            _canonical_object(self.immutable_spec, name='immutable_spec'))

    @property
    def action_id(self) -> uuid.UUID:
        return self.identity.action_id

    @property
    def immutable_spec_sha256(self) -> str:
        return canonical_sha256(self.immutable_spec)


@dataclasses.dataclass(frozen=True)
class ActionRecord:
    """Validated durable action row."""

    action_id: uuid.UUID
    domain: str
    resource_type: str
    resource_identity: str
    desired_generation: int
    action_type: str
    immutable_spec: JsonObject
    immutable_spec_sha256: str
    kernel_state: KernelState
    current_attempt: int
    next_attempt_at: datetime.datetime | None
    last_result: JsonObject | None
    last_result_sha256: str | None
    terminal_disposition: str | None
    revision: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    terminal_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class AttemptRecord:
    """Validated durable evidence for one action attempt."""

    action_id: uuid.UUID
    attempt: int
    request_id: str
    request_input_sha256: str
    provider_operation_id: str | None
    mutation_boundary: MutationBoundary
    provider_io_boundary: ProviderIOBoundary
    provider_progress: JsonObject | None
    provider_progress_sha256: str | None
    provider_progress_revision: int
    typed_outcome: JsonObject | None
    typed_outcome_sha256: str | None
    request_terminal_state: str | None
    admitted_at: datetime.datetime
    updated_at: datetime.datetime
    settled_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class AttemptExecutionFence:
    """Fresh request-claim identity supplied to typed progress validation."""

    request_id: str
    execution_generation: int
    claim_token: uuid.UUID
    worker_instance_id: uuid.UUID
    controller_generation: int | None


@dataclasses.dataclass(frozen=True)
class ActionCandidate:
    """Nonlocking discovery result safe to pass to a retryable operation."""

    action_id: uuid.UUID
    revision: int
    attempt: int
    next_attempt_at: datetime.datetime | None = None
    request_id: str | None = None


def _deadline_text(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        converted = datetime.datetime.fromtimestamp(value,
                                                    datetime.timezone.utc)
    except (OverflowError, OSError, TypeError, ValueError) as e:
        raise ValueError(
            'precondition_deadline is not a valid timestamp.') from e
    return converted.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


@dataclasses.dataclass(frozen=True)
class ActionRequestInput:
    """Canonical immutable input commitment for one API request attempt."""

    action_id: uuid.UUID
    attempt: int
    request_id: str
    value: JsonObject
    sha256: str

    @classmethod
    def from_request(cls, action_id: uuid.UUID | str, attempt: int,
                     request: 'requests_lib.Request') -> 'ActionRequestInput':
        parsed_action_id = _uuid(action_id, name='action_id')
        expected_request_id = request_id_for_attempt(parsed_action_id, attempt)
        if request.request_id != expected_request_id:
            raise ValueError(
                f'Attempt {attempt} requires request ID '
                f'{expected_request_id}, got {request.request_id}.')
        if request.status is not requests_lib.RequestStatus.PENDING:
            raise ValueError('An action request must be pristine PENDING.')
        if not request.should_enqueue:
            raise ValueError('An action request must have should_enqueue=true.')
        runtime_values = {
            'return_value': request.return_value,
            'error': request.error,
            'pid': request.pid,
            'status_msg': request.status_msg,
            'finished_at': request.finished_at,
            'claim_token': request.claim_token,
            'worker_instance_id': request.worker_instance_id,
            'controller_generation': request.controller_generation,
            'lease_expires_at': request.lease_expires_at,
            'heartbeat_at': request.heartbeat_at,
            'cancel_requested_at': request.cancel_requested_at,
            'cancel_acknowledged_at': request.cancel_acknowledged_at,
            'interrupted_reason': request.interrupted_reason,
            'terminal_cause': request.terminal_cause,
        }
        noninitial = [
            key for key, value in runtime_values.items() if value is not None
        ]
        if noninitial:
            raise ValueError('An action request has noninitial runtime fields: '
                             f'{", ".join(noninitial)}.')
        if request.should_retry or request.execution_generation != 0:
            raise ValueError(
                'An action request has noninitial retry/claim state.')
        if request.ignore_return_value or request.retryable:
            raise ValueError('V1 action requests are non-retryable and retain '
                             'their result.')
        if (request.precondition_type is not None or
                request.precondition_payload is not None or
                request.precondition_deadline is not None):
            raise ValueError('V1 action requests do not support preconditions.')

        registration = request_registry.registration_for_handler(
            request.entrypoint)
        if registration.execution_class is not (
                request_registry.ExecutionClass.NORMAL):
            raise ValueError('V1 action requests must use the normal executor.')
        if registration.replay_policy is not request_registry.ReplayPolicy.NEVER:
            raise ValueError('V1 action requests must use ReplayPolicy.NEVER.')
        durable_values = request.durable_values()
        value: JsonObject = {
            'version': 1,
            'action_id': str(parsed_action_id),
            'attempt': attempt,
            'request_id': request.request_id,
            'name': durable_values['name'],
            'handler_name': durable_values['handler_name'],
            'payload_type': durable_values['payload_type'],
            'payload_format': durable_values['payload_format'],
            'payload_version': durable_values['payload_version'],
            'producer_version': durable_values['producer_version'],
            'payload_json': durable_values['payload_json'],
            'execution_class': durable_values['execution_class'],
            'cluster_name': durable_values['cluster_name'],
            'schedule_type': durable_values['schedule_type'],
            'user_id': durable_values['user_id'],
            'file_mounts_blob_id': durable_values['file_mounts_blob_id'],
            'ignore_return_value': durable_values['ignore_return_value'],
            'retryable': durable_values['retryable'],
            'precondition_type': request.precondition_type,
            'precondition_payload': request.precondition_payload,
            'precondition_deadline': _deadline_text(
                request.precondition_deadline),
            'initial_status': requests_lib.RequestStatus.PENDING.value,
            'should_enqueue': True,
            'queue_priority': 0,
        }
        normalized = _canonical_object(value, name='request_input')
        return cls(parsed_action_id, attempt, request.request_id, normalized,
                   canonical_sha256(normalized))

    def validate(self) -> None:
        parsed_action_id = _uuid(self.action_id, name='action_id')
        if not isinstance(self.action_id, uuid.UUID):
            raise ValueError('action_id is not canonical.')
        if self.request_id != request_id_for_attempt(parsed_action_id,
                                                     self.attempt):
            raise ValueError('request_id does not match action attempt.')
        normalized = _canonical_object(self.value, name='request_input')
        if self.value != normalized:
            raise ValueError('request_input is not canonical.')
        expected_keys = {
            'version', 'action_id', 'attempt', 'request_id', 'name',
            'handler_name', 'payload_type', 'payload_format', 'payload_version',
            'producer_version', 'payload_json', 'execution_class',
            'cluster_name', 'schedule_type', 'user_id', 'file_mounts_blob_id',
            'ignore_return_value', 'retryable', 'precondition_type',
            'precondition_payload', 'precondition_deadline', 'initial_status',
            'should_enqueue', 'queue_priority'
        }
        if set(normalized) != expected_keys:
            raise ValueError('request_input has unknown or missing fields.')
        if (normalized['version'] != 1 or
                isinstance(normalized['version'], bool)):
            raise ValueError('request_input version must be integer 1.')
        if normalized['action_id'] != str(parsed_action_id):
            raise ValueError(
                'request_input action_id does not match its owner.')
        if (not isinstance(normalized['attempt'], int) or
                isinstance(normalized['attempt'], bool) or
                normalized['attempt'] != self.attempt):
            raise ValueError('request_input attempt does not match its owner.')
        if normalized['request_id'] != self.request_id:
            raise ValueError(
                'request_input request_id does not match its owner.')

        text_fields = ('name', 'handler_name', 'payload_type',
                       'producer_version', 'schedule_type', 'user_id')
        for field in text_fields:
            if not isinstance(normalized[field], str):
                raise ValueError(f'request_input {field} must be text.')
        for field in ('cluster_name', 'file_mounts_blob_id'):
            if (normalized[field] is not None and
                    not isinstance(normalized[field], str)):
                raise ValueError(f'request_input {field} must be text or null.')
        if not isinstance(normalized['payload_json'], dict):
            raise ValueError('request_input payload_json must be an object.')
        if normalized['payload_format'] != requests_lib.DURABLE_PAYLOAD_FORMAT:
            raise ValueError('request_input payload_format is unsupported.')
        if (not isinstance(normalized['payload_version'], int) or
                isinstance(normalized['payload_version'], bool) or
                normalized['payload_version']
                != requests_lib.DURABLE_PAYLOAD_VERSION):
            raise ValueError('request_input payload_version is unsupported.')
        try:
            schedule_type = requests_lib.ScheduleType(
                normalized['schedule_type'])
        except ValueError as e:
            raise ValueError(
                'request_input schedule_type is unsupported.') from e
        if schedule_type.value != normalized['schedule_type']:
            raise ValueError('request_input schedule_type is not canonical.')
        registration = request_registry.resolve_handler(
            normalized['handler_name'])
        if registration.name != normalized['handler_name']:
            raise ValueError('request_input handler_name is not canonical.')
        if (registration.execution_class
                is not request_registry.ExecutionClass.NORMAL or
                normalized['execution_class']
                != request_registry.ExecutionClass.NORMAL.value):
            raise ValueError('request_input must use the normal executor.')
        if registration.replay_policy is not request_registry.ReplayPolicy.NEVER:
            raise ValueError('request_input must use ReplayPolicy.NEVER.')
        if (normalized['ignore_return_value'] is not False or
                normalized['retryable'] is not False):
            raise ValueError(
                'request_input must retain a non-retryable result.')
        if (normalized['precondition_type'] is not None or
                normalized['precondition_payload'] is not None or
                normalized['precondition_deadline'] is not None):
            raise ValueError('request_input cannot have a precondition.')
        if (normalized['initial_status']
                != requests_lib.RequestStatus.PENDING.value or
                normalized['should_enqueue'] is not True or
                not isinstance(normalized['queue_priority'], int) or
                isinstance(normalized['queue_priority'], bool) or
                normalized['queue_priority'] != 0):
            raise ValueError('request_input has invalid initial queue state.')
        if self.sha256 != canonical_sha256(normalized):
            raise ValueError('request_input SHA-256 does not match its bytes.')


@dataclasses.dataclass(frozen=True)
class MaterializationResult:
    """Committed outcome of one materialization or lost-ACK adoption."""

    action: ActionRecord
    attempt: AttemptRecord | None
    created: bool = False
    adopted: bool = False
    blocked: bool = False


@dataclasses.dataclass(frozen=True)
class ActionReduction:
    """Bounded reducer decision persisted with terminal request evidence."""

    kernel_state: KernelState
    typed_outcome: Mapping[str, Any]
    result: Mapping[str, Any]
    retry_after_seconds: int | None = None
    terminal_disposition: str | None = None

    def normalized(self) -> 'ActionReduction':
        if self.kernel_state not in (KernelState.READY, KernelState.BLOCKED,
                                     KernelState.TERMINAL):
            raise ValueError('A reduction must target READY, BLOCKED, or '
                             'TERMINAL.')
        outcome = _canonical_object(self.typed_outcome, name='typed_outcome')
        result = _canonical_object(self.result, name='result')
        retry_after = self.retry_after_seconds
        disposition = self.terminal_disposition
        if self.kernel_state is KernelState.READY:
            if (not isinstance(retry_after, int) or
                    isinstance(retry_after, bool) or retry_after < 0):
                raise ValueError('READY reduction requires a nonnegative '
                                 'integer retry delay.')
            if disposition is not None:
                raise ValueError('READY reduction cannot be terminal.')
        elif retry_after is not None:
            raise ValueError('Only READY reduction accepts a retry delay.')
        if self.kernel_state is KernelState.TERMINAL:
            if disposition is None:
                raise ValueError('TERMINAL reduction requires a disposition.')
            disposition = _bounded_text(disposition,
                                        name='terminal_disposition',
                                        maximum_bytes=64)
        elif disposition is not None:
            raise ValueError('Only TERMINAL reduction accepts a disposition.')
        return ActionReduction(self.kernel_state, outcome, result, retry_after,
                               disposition)


@dataclasses.dataclass(frozen=True)
class ReductionContext:
    """Locked lineage, request evidence, and DB time for first reduction."""

    terminal_request: requests_lib.Request
    database_now: datetime.datetime
    predecessor_attempt: AttemptRecord | None = None


class ProviderProgressContract(Protocol):
    """Domain-owned closed validation behind the generic API006 journal.

    Implementations are pure: they may parse and compare the immutable action,
    predecessor, attempt, execution fence, and proposed value, but must not do
    I/O or mutate durable state.
    """

    def retry_seed(self, action: ActionRecord,
                   predecessor: AttemptRecord) -> Mapping[str, Any] | None:
        """Derive the only legal attempt-local seed from a predecessor."""

    def validate_attempt_snapshot(
        self,
        action: ActionRecord,
        predecessor: AttemptRecord | None,
        attempt: AttemptRecord,
        execution_fence: AttemptExecutionFence | None,
    ) -> None:
        """Validate one persisted attempt and its predecessor lineage."""

    def validate_progress_transition(
        self,
        action: ActionRecord,
        predecessor: AttemptRecord | None,
        attempt: AttemptRecord,
        execution_fence: AttemptExecutionFence,
        proposed_progress: JsonObject,
    ) -> None:
        """Validate a claim-fenced progress transition or exact replay."""

    def validate_reduction(
        self,
        action: ActionRecord,
        predecessor: AttemptRecord | None,
        attempt: AttemptRecord,
        reduction: ActionReduction,
        context: ReductionContext,
    ) -> None:
        """Validate a first terminal reduction, including retry authority."""


@dataclasses.dataclass(frozen=True)
class ReductionResult:
    """Persisted result returned by first reduction or replay adoption."""

    action: ActionRecord
    attempt: AttemptRecord
    replayed: bool


Reducer = Callable[[
    'sqlalchemy.engine.Connection', ActionRecord, AttemptRecord,
    ReductionContext
], ActionReduction]
