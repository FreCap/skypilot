"""PostgreSQL persistence for immutable Serve039 authority history.

This module owns only class-17 execution-authority lineage and authoritative
attempt terminal selectors.  Callers must supply one already-open consolidated
PostgreSQL transaction after every earlier lock class has been acquired.  The
writer never checks out, commits, rolls back, or reaches back to an earlier
relation.

Within class 17 the fixed phase order is:

1. lineage rows by ``(action_id, attempt, execution_generation)``; then
2. terminal selectors by ``(action_id, attempt)``.

Existing lineage named by a selector is key-share locked in phase one.  A
terminalizer never creates missing lineage.  Every insert is insert-or-exact-
adopt; an unequal conflict or malformed retained row is corruption.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import ipaddress
import re
from typing import Any, TypeAlias
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_actions
from sky.server.requests import resource_actions as kernel_actions

_UTC = datetime.timezone.utc
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_MAX_TEXT_BYTES = 1024

AUTHORITY_HISTORY_LOCK_CLASS_V2 = 17
AUTHORITY_HISTORY_PHASE_ORDER_V2 = (
    m4_schema.EXECUTION_AUTHORITY_LINEAGE.name,
    m4_schema.ATTEMPT_TERMINAL_AUTHORITY.name,
)


class AuthorityHistoryError(RuntimeError):
    """Base failure for the immutable Serve039 authority-history store."""


class AuthorityHistoryCorruption(AuthorityHistoryError):
    """A retained row or same-key conflict violates the closed contract."""


class AuthorityHistoryTransactionError(AuthorityHistoryError):
    """The caller did not provide the required PostgreSQL transaction."""


class PolicyAdmissionStateV2(str, enum.Enum):
    OPEN = 'OPEN'
    DRAINING = 'DRAINING'


class RequestTerminalStateV2(str, enum.Enum):
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class TerminalAuthorityDispositionV2(str, enum.Enum):
    NO_SUCCESSFUL_CLAIM_START = 'NO_SUCCESSFUL_CLAIM_START'
    LINEAGE = 'LINEAGE'


class TerminalCauseV2(str, enum.Enum):
    HANDLER_RETURN = 'HANDLER_RETURN'
    REQUEST_FAILED = 'REQUEST_FAILED'
    REQUEST_CANCELLED = 'REQUEST_CANCELLED'
    CLAIM_START_NOT_REPRESENTABLE = 'CLAIM_START_NOT_REPRESENTABLE'
    CLAIM_REAUTHORIZATION_FAILED = 'CLAIM_REAUTHORIZATION_FAILED'
    TERMINAL_BEFORE_CLAIM_START = 'TERMINAL_BEFORE_CLAIM_START'


JsonObject: TypeAlias = dict[str, Any]


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an exact JSON object.')
    if any(type(key) is not str for key in value) or set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    return resource_actions.CanonicalJsonObject(value).canonical_value()


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        parsed = value
    elif type(value) is str:
        try:
            parsed = uuid.UUID(value)
        except ValueError as error:
            raise ValueError(f'{name} must be a canonical UUID.') from error
        if str(parsed) != value:
            raise ValueError(f'{name} must be a canonical UUID.')
    else:
        raise TypeError(f'{name} must be a UUID.')
    if parsed.variant != uuid.RFC_4122 or parsed.version is None:
        raise ValueError(f'{name} must be an RFC 4122 UUID.')
    return parsed


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be a lowercase SHA-256 digest.')
    return value


def _text(value: Any,
          *,
          name: str,
          maximum_bytes: int = _MAX_TEXT_BYTES) -> str:
    if type(value) is not str or value != value.strip():
        raise ValueError(f'{name} must be canonical nonempty text.')
    try:
        size = len(value.encode('utf-8'))
    except UnicodeEncodeError as error:
        raise ValueError(f'{name} must be valid UTF-8 text.') from error
    if not 1 <= size <= maximum_bytes:
        raise ValueError(f'{name} is outside its UTF-8 byte bound.')
    return value


def _positive_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f'{name} must be a positive integer.')
    return value


def _zero_or_one(value: Any, *, name: str) -> int:
    if type(value) is not int or value not in (0, 1):
        raise ValueError(f'{name} must be integer zero or one.')
    return value


def _enum_value(enum_type: type[enum.Enum], value: Any, *, name: str) -> Any:
    if type(value) is enum_type:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        parsed = enum_type(value)
    except ValueError as error:
        raise ValueError(f'{name} is unsupported.') from error
    if parsed.value != value:
        raise ValueError(f'{name} is not canonical.')
    return parsed


def _timestamp(value: Any, *, name: str) -> str:
    if type(value) is str:
        return authority.datetime_to_timestamp(authority.timestamp_to_datetime(
            value, name=name),
                                               name=name)
    return authority.datetime_to_timestamp(value, name=name)


def _timestamp_datetime(value: str, *, name: str) -> datetime.datetime:
    return authority.timestamp_to_datetime(value, name=name)


def _canonical_child(value: Any, *,
                     name: str) -> resource_actions.CanonicalJsonObject:
    try:
        child = resource_actions.CanonicalJsonObject(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f'{name} must be a bounded canonical object.') from error
    return child


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionClaimHandoffFenceV2(authority.CanonicalContract):
    """Closed claim-time handoff/cold-recovery fence projection."""

    version: int
    cohort_id: str
    cohort_revision: int
    registration_set_revision: int
    nonterminal_handoff_id: None
    completed_cold_recovery_id: uuid.UUID | None
    checked_at: str

    _KEYS = frozenset({
        'version', 'cohort_id', 'cohort_revision', 'registration_set_revision',
        'nonterminal_handoff_id', 'completed_cold_recovery_id', 'checked_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('handoff fence version must be integer 2.')
        object.__setattr__(self, 'cohort_id',
                           _text(self.cohort_id, name='handoff.cohort_id'))
        object.__setattr__(
            self, 'cohort_revision',
            _positive_integer(self.cohort_revision,
                              name='handoff.cohort_revision'))
        object.__setattr__(
            self, 'registration_set_revision',
            _positive_integer(self.registration_set_revision,
                              name='handoff.registration_set_revision'))
        if self.cohort_revision != self.registration_set_revision:
            raise ValueError(
                'handoff cohort and registration revisions differ.')
        if self.nonterminal_handoff_id is not None:
            raise ValueError(
                'claim handoff fence requires no nonterminal handoff.')
        if self.completed_cold_recovery_id is not None:
            object.__setattr__(
                self, 'completed_cold_recovery_id',
                _uuid(self.completed_cold_recovery_id,
                      name='handoff.completed_cold_recovery_id'))
        object.__setattr__(
            self, 'checked_at',
            _timestamp(self.checked_at, name='handoff.checked_at'))

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderResourceActionClaimHandoffFenceV2:
        raw = _closed_object(value, name='claim handoff fence', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'cohort_id': self.cohort_id,
            'cohort_revision': self.cohort_revision,
            'registration_set_revision': self.registration_set_revision,
            'nonterminal_handoff_id': None,
            'completed_cold_recovery_id':
                (None if self.completed_cold_recovery_id is None else str(
                    self.completed_cold_recovery_id)),
            'checked_at': self.checked_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionReferenceSnapshotV2(authority.CanonicalContract):
    """Exact ACTION_ACTIVE reference consumed by an authoritative claim."""

    version: int
    decision_id: uuid.UUID
    cohort_id: str
    service_hash: uuid.UUID
    replica_incarnation: uuid.UUID
    desired_generation: int
    action_kind: kernel_actions.ActionKind
    controller_owner_fence: str
    lifecycle_epoch: int
    preparation_capability_sha256: str
    reference_state: str
    revision: int

    _KEYS = frozenset({
        'version', 'decision_id', 'cohort_id', 'service_hash',
        'replica_incarnation', 'desired_generation', 'action_kind',
        'controller_owner_fence', 'lifecycle_epoch',
        'preparation_capability_sha256', 'reference_state', 'revision'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('reference version must be integer 2.')
        for field in ('decision_id', 'service_hash', 'replica_incarnation'):
            object.__setattr__(
                self, field,
                _uuid(getattr(self, field), name=f'reference.{field}'))
        object.__setattr__(self, 'cohort_id',
                           _text(self.cohort_id, name='reference.cohort_id'))
        object.__setattr__(
            self, 'desired_generation',
            _positive_integer(self.desired_generation,
                              name='reference.desired_generation'))
        object.__setattr__(
            self, 'action_kind',
            _enum_value(kernel_actions.ActionKind,
                        self.action_kind,
                        name='reference.action_kind'))
        object.__setattr__(
            self, 'controller_owner_fence',
            _text(self.controller_owner_fence,
                  name='reference.controller_owner_fence'))
        object.__setattr__(
            self, 'lifecycle_epoch',
            _positive_integer(self.lifecycle_epoch,
                              name='reference.lifecycle_epoch'))
        object.__setattr__(
            self, 'preparation_capability_sha256',
            _sha256(self.preparation_capability_sha256,
                    name='reference.preparation_capability_sha256'))
        if self.reference_state != 'ACTION_ACTIVE':
            raise ValueError('authoritative reference must be ACTION_ACTIVE.')
        object.__setattr__(
            self, 'revision',
            _positive_integer(self.revision, name='reference.revision'))

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderResourceActionReferenceSnapshotV2:
        raw = _closed_object(value, name='action reference', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'decision_id': str(self.decision_id),
            'cohort_id': self.cohort_id,
            'service_hash': str(self.service_hash),
            'replica_incarnation': str(self.replica_incarnation),
            'desired_generation': self.desired_generation,
            'action_kind': self.action_kind.value,
            'controller_owner_fence': self.controller_owner_fence,
            'lifecycle_epoch': self.lifecycle_epoch,
            'preparation_capability_sha256': self.preparation_capability_sha256,
            'reference_state': 'ACTION_ACTIVE',
            'revision': self.revision,
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionApiInstanceSnapshotV2(authority.CanonicalContract):
    """Exact ready authority-worker process snapshot at claim start."""

    version: int
    instance_id: uuid.UUID
    authority_worker_instance_id: uuid.UUID
    role: str
    pod_name: str
    pod_uid: uuid.UUID
    pod_ip: str
    server_version: str
    started_at: str
    heartbeat_at: str
    draining_at: None
    ready: bool
    health_detail: resource_actions.CanonicalJsonObject
    supported_handlers: tuple[str, ...]
    supported_payload_versions: resource_actions.CanonicalJsonObject

    _KEYS = frozenset({
        'version', 'instance_id', 'authority_worker_instance_id', 'role',
        'pod_name', 'pod_uid', 'pod_ip', 'server_version', 'started_at',
        'heartbeat_at', 'draining_at', 'ready', 'health_detail',
        'supported_handlers', 'supported_payload_versions'
    })
    _HEALTH_KEYS = frozenset({
        'phase', 'boot_nonce', 'authority_worker_instance_id',
        'execution_owner_sha256', 'pool_generation'
    })
    _HANDLERS = (
        'serve_resource_action_down',
        'serve_resource_action_launch',
        'serve_shadow_candidate_down',
        'serve_shadow_candidate_launch',
    )
    _PAYLOAD_VERSIONS = {
        'pydantic-json': {
            'minimum': 1,
            'maximum': 1,
        }
    }

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('API-instance version must be integer 2.')
        object.__setattr__(self, 'instance_id',
                           _uuid(self.instance_id, name='api.instance_id'))
        object.__setattr__(
            self, 'authority_worker_instance_id',
            _uuid(self.authority_worker_instance_id,
                  name='api.authority_worker_instance_id'))
        if self.role != 'authority-worker':
            raise ValueError('API instance must have authority-worker role.')
        object.__setattr__(self, 'pod_name',
                           _text(self.pod_name, name='api.pod_name'))
        object.__setattr__(self, 'pod_uid',
                           _uuid(self.pod_uid, name='api.pod_uid'))
        if self.pod_uid != self.authority_worker_instance_id:
            raise ValueError('API stable worker and Pod UID differ.')
        if type(self.pod_ip) is not str:
            raise TypeError('api.pod_ip must be text.')
        try:
            canonical_ip = str(ipaddress.ip_address(self.pod_ip))
        except ValueError as error:
            raise ValueError('api.pod_ip is invalid.') from error
        if canonical_ip != self.pod_ip:
            raise ValueError('api.pod_ip is not canonical.')
        object.__setattr__(
            self, 'server_version',
            _text(self.server_version, name='api.server_version'))
        object.__setattr__(self, 'started_at',
                           _timestamp(self.started_at, name='api.started_at'))
        object.__setattr__(
            self, 'heartbeat_at',
            _timestamp(self.heartbeat_at, name='api.heartbeat_at'))
        if (_timestamp_datetime(self.heartbeat_at, name='api.heartbeat_at')
                < _timestamp_datetime(self.started_at, name='api.started_at')):
            raise ValueError('API heartbeat predates process start.')
        if self.draining_at is not None or type(
                self.ready) is not bool or not self.ready:
            raise ValueError(
                'claim API instance must be ready and nondraining.')
        health = self.health_detail
        if type(health) is not resource_actions.CanonicalJsonObject:
            health = _canonical_child(health, name='api.health_detail')
            object.__setattr__(self, 'health_detail', health)
        health_value = _closed_object(health.canonical_value(),
                                      name='api.health_detail',
                                      keys=self._HEALTH_KEYS)
        if (health_value['phase'] != 'authority-ready-v2' or
                _uuid(health_value['authority_worker_instance_id'],
                      name='api.health stable worker')
                != self.authority_worker_instance_id or
                _positive_integer(health_value['pool_generation'],
                                  name='api.health pool generation') < 1):
            raise ValueError('API ready health detail is crossed.')
        _uuid(health_value['boot_nonce'], name='api.health boot nonce')
        _sha256(health_value['execution_owner_sha256'],
                name='api.health execution owner hash')
        if type(self.supported_handlers) is not tuple:
            raise TypeError('API supported handlers must be a tuple.')
        if self.supported_handlers != self._HANDLERS:
            raise ValueError('API supported handler inventory is not exact.')
        payloads = self.supported_payload_versions
        if type(payloads) is not resource_actions.CanonicalJsonObject:
            payloads = _canonical_child(payloads,
                                        name='api.supported_payload_versions')
            object.__setattr__(self, 'supported_payload_versions', payloads)
        if payloads.canonical_value() != self._PAYLOAD_VERSIONS:
            raise ValueError('API payload-version inventory is not exact.')

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderResourceActionApiInstanceSnapshotV2:
        raw = _closed_object(value,
                             name='authority API instance',
                             keys=cls._KEYS)
        handlers = raw['supported_handlers']
        if type(handlers) is not list:
            raise TypeError('API supported handlers must be a list.')
        return cls(
            version=raw['version'],
            instance_id=raw['instance_id'],
            authority_worker_instance_id=raw['authority_worker_instance_id'],
            role=raw['role'],
            pod_name=raw['pod_name'],
            pod_uid=raw['pod_uid'],
            pod_ip=raw['pod_ip'],
            server_version=raw['server_version'],
            started_at=raw['started_at'],
            heartbeat_at=raw['heartbeat_at'],
            draining_at=raw['draining_at'],
            ready=raw['ready'],
            health_detail=_canonical_child(raw['health_detail'],
                                           name='api.health_detail'),
            supported_handlers=tuple(handlers),
            supported_payload_versions=_canonical_child(
                raw['supported_payload_versions'],
                name='api.supported_payload_versions'))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'instance_id': str(self.instance_id),
            'authority_worker_instance_id': str(
                self.authority_worker_instance_id),
            'role': 'authority-worker',
            'pod_name': self.pod_name,
            'pod_uid': str(self.pod_uid),
            'pod_ip': self.pod_ip,
            'server_version': self.server_version,
            'started_at': self.started_at,
            'heartbeat_at': self.heartbeat_at,
            'draining_at': None,
            'ready': True,
            'health_detail': self.health_detail.canonical_value(),
            'supported_handlers': list(self.supported_handlers),
            'supported_payload_versions':
                self.supported_payload_versions.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRequestClaimSnapshotV2(authority.CanonicalContract):
    """Exact generation-one private request claim projection."""

    version: int
    request_id: uuid.UUID
    status: str
    request_execution_generation: int
    authority_worker_instance_id: uuid.UUID
    worker_instance_id: uuid.UUID
    claim_token_sha256: str
    controller_generation: None
    lease_expires_at: str
    heartbeat_at: str
    cancel_requested_at: None
    cancel_acknowledged_at: None
    delivery_state: str
    claim_generation: int
    queue_priority: int

    _KEYS = frozenset({
        'version', 'request_id', 'status', 'request_execution_generation',
        'authority_worker_instance_id', 'worker_instance_id',
        'claim_token_sha256', 'controller_generation', 'lease_expires_at',
        'heartbeat_at', 'cancel_requested_at', 'cancel_acknowledged_at',
        'delivery_state', 'claim_generation', 'queue_priority'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('request-claim version must be integer 2.')
        object.__setattr__(self, 'request_id',
                           _uuid(self.request_id, name='claim.request_id'))
        if self.status != 'RUNNING':
            raise ValueError('request claim must be RUNNING.')
        if self.request_execution_generation != 1:
            raise ValueError('request claim generation must equal one.')
        for field in ('authority_worker_instance_id', 'worker_instance_id'):
            object.__setattr__(
                self, field, _uuid(getattr(self, field), name=f'claim.{field}'))
        if self.authority_worker_instance_id == self.worker_instance_id:
            raise ValueError('request stable and process workers must differ.')
        object.__setattr__(
            self, 'claim_token_sha256',
            _sha256(self.claim_token_sha256, name='claim.token hash'))
        if self.controller_generation is not None:
            raise ValueError(
                'authority request claim has no controller generation.')
        object.__setattr__(
            self, 'lease_expires_at',
            _timestamp(self.lease_expires_at, name='claim.lease_expires_at'))
        object.__setattr__(
            self, 'heartbeat_at',
            _timestamp(self.heartbeat_at, name='claim.heartbeat_at'))
        if (self.cancel_requested_at is not None or
                self.cancel_acknowledged_at is not None):
            raise ValueError('claim-start request must not be cancelled.')
        if (self.delivery_state != 'claimed' or self.claim_generation != 1 or
                self.queue_priority != 0):
            raise ValueError('request claim queue shape is not exact.')

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderResourceActionRequestClaimSnapshotV2:
        return cls(
            **_closed_object(value, name='request claim', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'request_id': str(self.request_id),
            'status': 'RUNNING',
            'request_execution_generation': 1,
            'authority_worker_instance_id': str(
                self.authority_worker_instance_id),
            'worker_instance_id': str(self.worker_instance_id),
            'claim_token_sha256': self.claim_token_sha256,
            'controller_generation': None,
            'lease_expires_at': self.lease_expires_at,
            'heartbeat_at': self.heartbeat_at,
            'cancel_requested_at': None,
            'cancel_acknowledged_at': None,
            'delivery_state': 'claimed',
            'claim_generation': 1,
            'queue_priority': 0,
        }


def _accepted_membership_from_value(
    value: Any,) -> authority.ProviderAuthorityWorkerAcceptedMembershipV2:
    keys = frozenset({
        'version', 'registration', 'registration_set_revision',
        'registration_set_sha256', 'lease'
    })
    raw = _closed_object(value, name='accepted membership', keys=keys)
    return authority.ProviderAuthorityWorkerAcceptedMembershipV2(
        version=raw['version'],
        registration=(
            authority.ProviderAuthorityWorkerRegistrationV2.from_value(
                raw['registration'])),
        registration_set_revision=raw['registration_set_revision'],
        registration_set_sha256=raw['registration_set_sha256'],
        lease=authority.ProviderAuthorityWorkerLeaseV1.from_value(raw['lease']))


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionDispatchMembershipV2(authority.CanonicalContract):
    """Complete typed accepted claim-membership preimage."""

    version: int
    registration_set: authority.ProviderAuthorityWorkerRegistrationSetV2
    accepted_membership: authority.ProviderAuthorityWorkerAcceptedMembershipV2
    handoff_fence: ProviderResourceActionClaimHandoffFenceV2
    reference: ProviderResourceActionReferenceSnapshotV2
    api_instance: ProviderResourceActionApiInstanceSnapshotV2
    request_claim: ProviderResourceActionRequestClaimSnapshotV2

    _KEYS = frozenset({
        'version', 'registration_set', 'accepted_membership', 'handoff_fence',
        'reference', 'api_instance', 'request_claim'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('dispatch membership version must be integer 2.')
        if type(self.registration_set) is not (
                authority.ProviderAuthorityWorkerRegistrationSetV2):
            raise TypeError('dispatch registration set is invalid.')
        if type(self.accepted_membership) is not (
                authority.ProviderAuthorityWorkerAcceptedMembershipV2):
            raise TypeError('dispatch accepted membership is invalid.')
        for value, expected, name in (
            (self.handoff_fence, ProviderResourceActionClaimHandoffFenceV2,
             'handoff fence'), (self.reference,
                                ProviderResourceActionReferenceSnapshotV2,
                                'reference'),
            (self.api_instance, ProviderResourceActionApiInstanceSnapshotV2,
             'API instance'), (self.request_claim,
                               ProviderResourceActionRequestClaimSnapshotV2,
                               'request claim')):
            if type(value) is not expected:
                raise TypeError(f'dispatch {name} is invalid.')
        registration = self.accepted_membership.registration
        matches = tuple(item for item in self.registration_set.workers
                        if item.canonical_bytes == registration.canonical_bytes)
        if len(matches) != 1:
            raise ValueError(
                'accepted member is not unique in registration set.')
        if (self.accepted_membership.registration_set_revision
                != self.registration_set.revision or
                self.accepted_membership.registration_set_sha256
                != self.registration_set.sha256 or
                self.handoff_fence.registration_set_revision
                != self.registration_set.revision):
            raise ValueError('dispatch registration-set evidence differs.')
        stable_id = registration.worker_instance_id
        process_id = self.api_instance.instance_id
        if (self.accepted_membership.lease.worker_instance_id != stable_id or
                self.api_instance.authority_worker_instance_id != stable_id or
                self.request_claim.authority_worker_instance_id != stable_id or
                self.request_claim.worker_instance_id != process_id):
            raise ValueError(
                'dispatch stable/process worker identities differ.')
        if (self.handoff_fence.cohort_id != self.reference.cohort_id or
                self.request_claim.request_id is None):
            raise ValueError('dispatch cohort/request evidence differs.')

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderResourceActionDispatchMembershipV2:
        raw = _closed_object(value, name='dispatch membership', keys=cls._KEYS)
        return cls(
            version=raw['version'],
            registration_set=(
                authority.ProviderAuthorityWorkerRegistrationSetV2.from_value(
                    raw['registration_set'])),
            accepted_membership=_accepted_membership_from_value(
                raw['accepted_membership']),
            handoff_fence=ProviderResourceActionClaimHandoffFenceV2.from_value(
                raw['handoff_fence']),
            reference=ProviderResourceActionReferenceSnapshotV2.from_value(
                raw['reference']),
            api_instance=ProviderResourceActionApiInstanceSnapshotV2.from_value(
                raw['api_instance']),
            request_claim=ProviderResourceActionRequestClaimSnapshotV2.
            from_value(raw['request_claim']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'registration_set': self.registration_set.canonical_value(),
            'accepted_membership': self.accepted_membership.canonical_value(),
            'handoff_fence': self.handoff_fence.canonical_value(),
            'reference': self.reference.canonical_value(),
            'api_instance': self.api_instance.canonical_value(),
            'request_claim': self.request_claim.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderExecutionAuthorityProofV2(authority.CanonicalContract):
    """Closed action execution-readiness proof stored in lineage."""

    version: int
    schema_heads: authority.AuthoritySchemaHeadsV2
    service_hash: uuid.UUID
    policy_epoch: uuid.UUID
    policy_sha256: str
    authority_binding_sha256: str
    policy_admission_state: PolicyAdmissionStateV2
    policy_admission_revision: int
    action_id: uuid.UUID
    action_kind: kernel_actions.ActionKind
    immutable_spec_sha256: str
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2
    registration_set_sha256: str
    cohort_id: str
    deployment_uid: str
    reference_revision: int
    api_instance_started_at: str
    api_instance_heartbeat_at: str
    preflight_request_sha256: str
    preflight_response_sha256: str
    representability_case_inventory_sha256: str

    _KEYS = frozenset({
        'version', 'schema_heads', 'service_hash', 'policy_epoch',
        'policy_sha256', 'authority_binding_sha256', 'policy_admission_state',
        'policy_admission_revision', 'action_id', 'action_kind',
        'immutable_spec_sha256', 'resolved_cohort', 'registration_set_sha256',
        'cohort_id', 'deployment_uid', 'reference_revision',
        'api_instance_started_at', 'api_instance_heartbeat_at',
        'preflight_request_sha256', 'preflight_response_sha256',
        'representability_case_inventory_sha256'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('execution authority version must be integer 2.')
        if type(self.schema_heads) is not authority.AuthoritySchemaHeadsV2:
            raise TypeError('execution authority schema heads are invalid.')
        for field in ('service_hash', 'policy_epoch', 'action_id'):
            object.__setattr__(
                self, field,
                _uuid(getattr(self, field),
                      name=f'execution_authority.{field}'))
        for field in ('policy_sha256', 'authority_binding_sha256',
                      'immutable_spec_sha256', 'registration_set_sha256',
                      'preflight_request_sha256', 'preflight_response_sha256',
                      'representability_case_inventory_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field),
                        name=f'execution_authority.{field}'))
        object.__setattr__(
            self, 'policy_admission_state',
            _enum_value(PolicyAdmissionStateV2,
                        self.policy_admission_state,
                        name='execution_authority.policy_admission_state'))
        object.__setattr__(
            self, 'policy_admission_revision',
            _positive_integer(self.policy_admission_revision,
                              name='execution_authority.policy revision'))
        object.__setattr__(
            self, 'action_kind',
            _enum_value(kernel_actions.ActionKind,
                        self.action_kind,
                        name='execution_authority.action_kind'))
        if type(self.resolved_cohort
               ) is not authority.ProviderAuthorityWorkerCohortV2:
            raise TypeError('execution authority resolved cohort is invalid.')
        object.__setattr__(
            self, 'cohort_id',
            _text(self.cohort_id, name='execution_authority.cohort_id'))
        object.__setattr__(
            self, 'deployment_uid',
            _text(self.deployment_uid,
                  name='execution_authority.deployment_uid'))
        if (self.cohort_id != self.resolved_cohort.cohort_id or
                self.deployment_uid != self.resolved_cohort.deployment_uid):
            raise ValueError('execution authority cohort identity differs.')
        object.__setattr__(
            self, 'reference_revision',
            _positive_integer(self.reference_revision,
                              name='execution_authority.reference_revision'))
        for field in ('api_instance_started_at', 'api_instance_heartbeat_at'):
            object.__setattr__(
                self, field,
                _timestamp(getattr(self, field),
                           name=f'execution_authority.{field}'))
        if (_timestamp_datetime(self.api_instance_heartbeat_at,
                                name='execution_authority.heartbeat')
                < _timestamp_datetime(self.api_instance_started_at,
                                      name='execution_authority.started')):
            raise ValueError('execution-authority heartbeat predates start.')

    @classmethod
    def from_value(cls, value: Any) -> ProviderExecutionAuthorityProofV2:
        raw = _closed_object(value,
                             name='execution authority proof',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   schema_heads=authority.AuthoritySchemaHeadsV2.from_value(
                       raw['schema_heads']),
                   service_hash=raw['service_hash'],
                   policy_epoch=raw['policy_epoch'],
                   policy_sha256=raw['policy_sha256'],
                   authority_binding_sha256=raw['authority_binding_sha256'],
                   policy_admission_state=raw['policy_admission_state'],
                   policy_admission_revision=raw['policy_admission_revision'],
                   action_id=raw['action_id'],
                   action_kind=raw['action_kind'],
                   immutable_spec_sha256=raw['immutable_spec_sha256'],
                   resolved_cohort=authority.ProviderAuthorityWorkerCohortV2.
                   from_value(raw['resolved_cohort']),
                   registration_set_sha256=raw['registration_set_sha256'],
                   cohort_id=raw['cohort_id'],
                   deployment_uid=raw['deployment_uid'],
                   reference_revision=raw['reference_revision'],
                   api_instance_started_at=raw['api_instance_started_at'],
                   api_instance_heartbeat_at=raw['api_instance_heartbeat_at'],
                   preflight_request_sha256=raw['preflight_request_sha256'],
                   preflight_response_sha256=raw['preflight_response_sha256'],
                   representability_case_inventory_sha256=raw[
                       'representability_case_inventory_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'schema_heads': self.schema_heads.canonical_value(),
            'service_hash': str(self.service_hash),
            'policy_epoch': str(self.policy_epoch),
            'policy_sha256': self.policy_sha256,
            'authority_binding_sha256': self.authority_binding_sha256,
            'policy_admission_state': self.policy_admission_state.value,
            'policy_admission_revision': self.policy_admission_revision,
            'action_id': str(self.action_id),
            'action_kind': self.action_kind.value,
            'immutable_spec_sha256': self.immutable_spec_sha256,
            'resolved_cohort': self.resolved_cohort.canonical_value(),
            'registration_set_sha256': self.registration_set_sha256,
            'cohort_id': self.cohort_id,
            'deployment_uid': self.deployment_uid,
            'reference_revision': self.reference_revision,
            'api_instance_started_at': self.api_instance_started_at,
            'api_instance_heartbeat_at': self.api_instance_heartbeat_at,
            'preflight_request_sha256': self.preflight_request_sha256,
            'preflight_response_sha256': self.preflight_response_sha256,
            'representability_case_inventory_sha256':
                self.representability_case_inventory_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionExecutionAuthorityLineageV2(
        authority.CanonicalContract):
    """Complete immutable Serve039 generation-one lineage row."""

    version: int
    action_id: uuid.UUID
    attempt: int
    request_id: uuid.UUID
    request_input_sha256: str
    request_execution_generation: int
    authority_worker_instance_id: uuid.UUID
    worker_instance_id: uuid.UUID
    claim_token_sha256: str
    controller_generation: None
    service_hash: uuid.UUID
    policy_epoch: uuid.UUID
    policy_sha256: str
    authority_binding_sha256: str
    policy_admission_state: PolicyAdmissionStateV2
    policy_admission_revision: int
    cohort_id: str
    cohort_revision: int
    registration_set_revision: int
    worker_lease_revision: int
    reference_revision: int
    api_instance_started_at: str
    api_instance_heartbeat_at: str
    dispatch_membership: ProviderResourceActionDispatchMembershipV2
    dispatch_membership_sha256: str
    execution_authority: ProviderExecutionAuthorityProofV2
    execution_authority_sha256: str
    authorized_at: str

    _KEYS = frozenset({
        'version', 'action_id', 'attempt', 'request_id', 'request_input_sha256',
        'request_execution_generation', 'authority_worker_instance_id',
        'worker_instance_id', 'claim_token_sha256', 'controller_generation',
        'service_hash', 'policy_epoch', 'policy_sha256',
        'authority_binding_sha256', 'policy_admission_state',
        'policy_admission_revision', 'cohort_id', 'cohort_revision',
        'registration_set_revision', 'worker_lease_revision',
        'reference_revision', 'api_instance_started_at',
        'api_instance_heartbeat_at', 'dispatch_membership',
        'dispatch_membership_sha256', 'execution_authority',
        'execution_authority_sha256', 'authorized_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('lineage version must be integer 2.')
        for field in ('action_id', 'request_id', 'authority_worker_instance_id',
                      'worker_instance_id', 'service_hash', 'policy_epoch'):
            object.__setattr__(
                self, field, _uuid(getattr(self, field),
                                   name=f'lineage.{field}'))
        if self.authority_worker_instance_id == self.worker_instance_id:
            raise ValueError('lineage stable and process workers must differ.')
        object.__setattr__(
            self, 'attempt',
            _positive_integer(self.attempt, name='lineage.attempt'))
        if self.request_execution_generation != 1:
            raise ValueError('lineage execution generation must equal one.')
        if self.controller_generation is not None:
            raise ValueError('lineage controller generation must be null.')
        for field in ('request_input_sha256', 'claim_token_sha256',
                      'policy_sha256', 'authority_binding_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'lineage.{field}'))
        object.__setattr__(
            self, 'policy_admission_state',
            _enum_value(PolicyAdmissionStateV2,
                        self.policy_admission_state,
                        name='lineage.policy_admission_state'))
        for field in ('policy_admission_revision', 'cohort_revision',
                      'registration_set_revision', 'worker_lease_revision',
                      'reference_revision'):
            object.__setattr__(
                self, field,
                _positive_integer(getattr(self, field),
                                  name=f'lineage.{field}'))
        if self.registration_set_revision != self.cohort_revision:
            raise ValueError(
                'lineage cohort and registration revisions differ.')
        object.__setattr__(self, 'cohort_id',
                           _text(self.cohort_id, name='lineage.cohort_id'))
        for field in ('api_instance_started_at', 'api_instance_heartbeat_at',
                      'authorized_at'):
            object.__setattr__(
                self, field,
                _timestamp(getattr(self, field), name=f'lineage.{field}'))
        started = _timestamp_datetime(self.api_instance_started_at,
                                      name='lineage.api start')
        heartbeat = _timestamp_datetime(self.api_instance_heartbeat_at,
                                        name='lineage.api heartbeat')
        authorized = _timestamp_datetime(self.authorized_at,
                                         name='lineage.authorized_at')
        if heartbeat < started or authorized < started:
            raise ValueError('lineage timestamps violate process ordering.')
        if type(self.dispatch_membership) is not (
                ProviderResourceActionDispatchMembershipV2):
            raise TypeError('lineage dispatch membership is invalid.')
        if type(self.execution_authority
               ) is not ProviderExecutionAuthorityProofV2:
            raise TypeError('lineage execution authority is invalid.')
        if self.dispatch_membership_sha256 != self.dispatch_membership.sha256:
            raise ValueError('lineage dispatch-membership digest differs.')
        if self.execution_authority_sha256 != self.execution_authority.sha256:
            raise ValueError('lineage execution-authority digest differs.')
        object.__setattr__(
            self, 'dispatch_membership_sha256',
            _sha256(self.dispatch_membership_sha256,
                    name='lineage.dispatch_membership_sha256'))
        object.__setattr__(
            self, 'execution_authority_sha256',
            _sha256(self.execution_authority_sha256,
                    name='lineage.execution_authority_sha256'))
        self._validate_nested_bindings()

    @property
    def key(self) -> tuple[uuid.UUID, int, int]:
        return (self.action_id, self.attempt, self.request_execution_generation)

    @property
    def sort_key(self) -> tuple[bytes, int, int]:
        return (self.action_id.bytes, self.attempt,
                self.request_execution_generation)

    def _validate_nested_bindings(self) -> None:
        membership = self.dispatch_membership
        proof = self.execution_authority
        registration_set = membership.registration_set
        accepted = membership.accepted_membership
        reference = membership.reference
        api_instance = membership.api_instance
        claim = membership.request_claim
        handoff = membership.handoff_fence
        if (registration_set.revision != self.registration_set_revision or
                accepted.registration_set_revision
                != self.registration_set_revision or
                accepted.registration_set_sha256 != registration_set.sha256 or
                accepted.lease.revision != self.worker_lease_revision or
                handoff.cohort_id != self.cohort_id or
                handoff.cohort_revision != self.cohort_revision or
                reference.decision_id != self.action_id or
                reference.cohort_id != self.cohort_id or
                reference.service_hash != self.service_hash or
                reference.revision != self.reference_revision or
                api_instance.instance_id != self.worker_instance_id or
                api_instance.authority_worker_instance_id
                != self.authority_worker_instance_id or
                api_instance.started_at != self.api_instance_started_at or
                api_instance.heartbeat_at != self.api_instance_heartbeat_at or
                claim.request_id != self.request_id or
                claim.request_execution_generation
                != self.request_execution_generation or
                claim.authority_worker_instance_id
                != self.authority_worker_instance_id or
                claim.worker_instance_id != self.worker_instance_id or
                claim.claim_token_sha256 != self.claim_token_sha256):
            raise ValueError('lineage dispatch membership crosses row scalars.')
        registration_set.validate_for_cohort(proof.resolved_cohort)
        proof_bindings = (
            proof.action_id == self.action_id,
            proof.action_kind is reference.action_kind,
            proof.service_hash == self.service_hash,
            proof.policy_epoch == self.policy_epoch,
            proof.policy_sha256 == self.policy_sha256,
            proof.authority_binding_sha256 == self.authority_binding_sha256,
            proof.policy_admission_state is self.policy_admission_state,
            proof.policy_admission_revision == self.policy_admission_revision,
            proof.registration_set_sha256 == registration_set.sha256,
            proof.cohort_id == self.cohort_id,
            proof.reference_revision == self.reference_revision,
            proof.api_instance_started_at == self.api_instance_started_at,
            proof.api_instance_heartbeat_at == self.api_instance_heartbeat_at,
        )
        if not all(proof_bindings):
            raise ValueError('lineage execution authority crosses row scalars.')

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderResourceActionExecutionAuthorityLineageV2:
        raw = _closed_object(value,
                             name='execution authority lineage',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            action_id=raw['action_id'],
            attempt=raw['attempt'],
            request_id=raw['request_id'],
            request_input_sha256=raw['request_input_sha256'],
            request_execution_generation=raw['request_execution_generation'],
            authority_worker_instance_id=raw['authority_worker_instance_id'],
            worker_instance_id=raw['worker_instance_id'],
            claim_token_sha256=raw['claim_token_sha256'],
            controller_generation=raw['controller_generation'],
            service_hash=raw['service_hash'],
            policy_epoch=raw['policy_epoch'],
            policy_sha256=raw['policy_sha256'],
            authority_binding_sha256=raw['authority_binding_sha256'],
            policy_admission_state=raw['policy_admission_state'],
            policy_admission_revision=raw['policy_admission_revision'],
            cohort_id=raw['cohort_id'],
            cohort_revision=raw['cohort_revision'],
            registration_set_revision=raw['registration_set_revision'],
            worker_lease_revision=raw['worker_lease_revision'],
            reference_revision=raw['reference_revision'],
            api_instance_started_at=raw['api_instance_started_at'],
            api_instance_heartbeat_at=raw['api_instance_heartbeat_at'],
            dispatch_membership=(
                ProviderResourceActionDispatchMembershipV2.from_value(
                    raw['dispatch_membership'])),
            dispatch_membership_sha256=raw['dispatch_membership_sha256'],
            execution_authority=ProviderExecutionAuthorityProofV2.from_value(
                raw['execution_authority']),
            execution_authority_sha256=raw['execution_authority_sha256'],
            authorized_at=raw['authorized_at'])

    @classmethod
    def from_row(
        cls, row: Mapping[str, Any]
    ) -> ProviderResourceActionExecutionAuthorityLineageV2:
        expected = cls._KEYS - {'version'}
        if type(row) not in (dict, sqlalchemy.engine.RowMapping):
            row = dict(row)
        if set(row) != expected:
            raise ValueError('lineage row has unknown or missing columns.')
        value = dict(row)
        for field in ('api_instance_started_at', 'api_instance_heartbeat_at',
                      'authorized_at'):
            value[field] = _timestamp(value[field], name=f'lineage row {field}')
        value['version'] = 2
        return cls.from_value(value)

    def row_values(self) -> JsonObject:
        value = self.canonical_value()
        del value['version']
        for field in ('action_id', 'authority_worker_instance_id',
                      'worker_instance_id', 'policy_epoch'):
            value[field] = uuid.UUID(value[field])
        for field in ('api_instance_started_at', 'api_instance_heartbeat_at',
                      'authorized_at'):
            value[field] = _timestamp_datetime(value[field],
                                               name=f'lineage.{field}')
        return value

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'action_id': str(self.action_id),
            'attempt': self.attempt,
            'request_id': str(self.request_id),
            'request_input_sha256': self.request_input_sha256,
            'request_execution_generation': 1,
            'authority_worker_instance_id': str(
                self.authority_worker_instance_id),
            'worker_instance_id': str(self.worker_instance_id),
            'claim_token_sha256': self.claim_token_sha256,
            'controller_generation': None,
            'service_hash': str(self.service_hash),
            'policy_epoch': str(self.policy_epoch),
            'policy_sha256': self.policy_sha256,
            'authority_binding_sha256': self.authority_binding_sha256,
            'policy_admission_state': self.policy_admission_state.value,
            'policy_admission_revision': self.policy_admission_revision,
            'cohort_id': self.cohort_id,
            'cohort_revision': self.cohort_revision,
            'registration_set_revision': self.registration_set_revision,
            'worker_lease_revision': self.worker_lease_revision,
            'reference_revision': self.reference_revision,
            'api_instance_started_at': self.api_instance_started_at,
            'api_instance_heartbeat_at': self.api_instance_heartbeat_at,
            'dispatch_membership': self.dispatch_membership.canonical_value(),
            'dispatch_membership_sha256': self.dispatch_membership_sha256,
            'execution_authority': self.execution_authority.canonical_value(),
            'execution_authority_sha256': self.execution_authority_sha256,
            'authorized_at': self.authorized_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionAttemptTerminalAuthoritySelectorV2(
        authority.CanonicalContract):
    """Immutable request-GC-safe terminal authority selector."""

    version: int
    action_id: uuid.UUID
    attempt: int
    request_id: uuid.UUID
    request_input_sha256: str
    request_terminal_state: RequestTerminalStateV2
    request_execution_generation: int
    authority_worker_instance_id: uuid.UUID | None
    worker_instance_id: uuid.UUID | None
    handler_name: str
    authority_disposition: TerminalAuthorityDispositionV2
    lineage_generation: int | None
    terminal_cause: TerminalCauseV2
    request_finished_at: str

    _KEYS = frozenset({
        'version', 'action_id', 'attempt', 'request_id', 'request_input_sha256',
        'request_terminal_state', 'request_execution_generation',
        'authority_worker_instance_id', 'worker_instance_id', 'handler_name',
        'authority_disposition', 'lineage_generation', 'terminal_cause',
        'request_finished_at'
    })
    _HANDLERS = frozenset(
        {'serve_resource_action_launch', 'serve_resource_action_down'})

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('terminal selector version must be integer 2.')
        for field in ('action_id', 'request_id'):
            object.__setattr__(
                self, field,
                _uuid(getattr(self, field), name=f'selector.{field}'))
        object.__setattr__(
            self, 'attempt',
            _positive_integer(self.attempt, name='selector.attempt'))
        object.__setattr__(
            self, 'request_input_sha256',
            _sha256(self.request_input_sha256,
                    name='selector.request_input_sha256'))
        object.__setattr__(
            self, 'request_terminal_state',
            _enum_value(RequestTerminalStateV2,
                        self.request_terminal_state,
                        name='selector.request_terminal_state'))
        object.__setattr__(
            self, 'request_execution_generation',
            _zero_or_one(self.request_execution_generation,
                         name='selector.request_execution_generation'))
        for field in ('authority_worker_instance_id', 'worker_instance_id'):
            if getattr(self, field) is not None:
                object.__setattr__(
                    self, field,
                    _uuid(getattr(self, field), name=f'selector.{field}'))
        if ((self.authority_worker_instance_id is None)
                != (self.worker_instance_id is None)):
            raise ValueError('selector worker identities must be pair-null.')
        if (self.authority_worker_instance_id is not None and
                self.authority_worker_instance_id == self.worker_instance_id):
            raise ValueError('selector stable and process workers must differ.')
        if self.handler_name not in self._HANDLERS:
            raise ValueError(
                'selector handler is not a private action handler.')
        object.__setattr__(
            self, 'authority_disposition',
            _enum_value(TerminalAuthorityDispositionV2,
                        self.authority_disposition,
                        name='selector.authority_disposition'))
        if self.lineage_generation is not None:
            object.__setattr__(
                self, 'lineage_generation',
                _positive_integer(self.lineage_generation,
                                  name='selector.lineage_generation'))
        object.__setattr__(
            self, 'terminal_cause',
            _enum_value(TerminalCauseV2,
                        self.terminal_cause,
                        name='selector.terminal_cause'))
        object.__setattr__(
            self, 'request_finished_at',
            _timestamp(self.request_finished_at,
                       name='selector.request_finished_at'))
        self._validate_matrix()

    @property
    def key(self) -> tuple[uuid.UUID, int]:
        return self.action_id, self.attempt

    @property
    def sort_key(self) -> tuple[bytes, int]:
        return self.action_id.bytes, self.attempt

    @property
    def lineage_key(self) -> tuple[uuid.UUID, int, int] | None:
        if self.lineage_generation is None:
            return None
        return self.action_id, self.attempt, self.lineage_generation

    def _validate_matrix(self) -> None:
        state = self.request_terminal_state
        disposition = self.authority_disposition
        cause = self.terminal_cause
        generation = self.request_execution_generation
        workers_present = self.authority_worker_instance_id is not None
        if disposition is TerminalAuthorityDispositionV2.LINEAGE:
            if (generation != 1 or not workers_present or
                    self.lineage_generation != 1):
                raise ValueError(
                    'LINEAGE selector has invalid generation/worker shape.')
            allowed = {
                TerminalCauseV2.HANDLER_RETURN:
                    RequestTerminalStateV2.SUCCEEDED,
                TerminalCauseV2.REQUEST_FAILED: RequestTerminalStateV2.FAILED,
                TerminalCauseV2.REQUEST_CANCELLED:
                    RequestTerminalStateV2.CANCELLED,
                TerminalCauseV2.CLAIM_REAUTHORIZATION_FAILED:
                    RequestTerminalStateV2.FAILED,
            }
            if allowed.get(cause) is not state:
                raise ValueError(
                    'LINEAGE selector terminal tuple is unsupported.')
            return
        if self.lineage_generation is not None:
            raise ValueError('no-claim-start selector must not name lineage.')
        if generation == 0 and workers_present:
            raise ValueError('generation-zero selector must have null workers.')
        if generation == 1 and not workers_present:
            raise ValueError(
                'generation-one selector must retain both workers.')
        if cause is TerminalCauseV2.CLAIM_START_NOT_REPRESENTABLE:
            if generation != 1 or state is not RequestTerminalStateV2.FAILED:
                raise ValueError('claim-start rejection selector is invalid.')
            return
        if (cause is not TerminalCauseV2.TERMINAL_BEFORE_CLAIM_START or
                state not in (RequestTerminalStateV2.FAILED,
                              RequestTerminalStateV2.CANCELLED)):
            raise ValueError(
                'no-claim-start selector terminal tuple is unsupported.')

    @classmethod
    def from_value(
            cls, value: Any
    ) -> ProviderResourceActionAttemptTerminalAuthoritySelectorV2:
        return cls(**_closed_object(
            value, name='terminal authority selector', keys=cls._KEYS))

    @classmethod
    def from_row(
        cls, row: Mapping[str, Any]
    ) -> ProviderResourceActionAttemptTerminalAuthoritySelectorV2:
        expected = cls._KEYS - {'version'}
        if type(row) not in (dict, sqlalchemy.engine.RowMapping):
            row = dict(row)
        if set(row) != expected:
            raise ValueError(
                'terminal selector row has unknown or missing columns.')
        value = dict(row)
        value['request_finished_at'] = _timestamp(
            value['request_finished_at'], name='selector row finished_at')
        value['version'] = 2
        return cls.from_value(value)

    def row_values(self) -> JsonObject:
        value = self.canonical_value()
        del value['version']
        for field in ('action_id', 'authority_worker_instance_id',
                      'worker_instance_id'):
            if value[field] is not None:
                value[field] = uuid.UUID(value[field])
        value['request_finished_at'] = _timestamp_datetime(
            value['request_finished_at'], name='selector.request_finished_at')
        return value

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'action_id': str(self.action_id),
            'attempt': self.attempt,
            'request_id': str(self.request_id),
            'request_input_sha256': self.request_input_sha256,
            'request_terminal_state': self.request_terminal_state.value,
            'request_execution_generation': self.request_execution_generation,
            'authority_worker_instance_id':
                (None if self.authority_worker_instance_id is None else str(
                    self.authority_worker_instance_id)),
            'worker_instance_id': (None if self.worker_instance_id is None else
                                   str(self.worker_instance_id)),
            'handler_name': self.handler_name,
            'authority_disposition': self.authority_disposition.value,
            'lineage_generation': self.lineage_generation,
            'terminal_cause': self.terminal_cause.value,
            'request_finished_at': self.request_finished_at,
        }


@dataclasses.dataclass(frozen=True)
class PersistedExecutionAuthorityLineageV2:
    lineage: ProviderResourceActionExecutionAuthorityLineageV2
    adopted: bool


@dataclasses.dataclass(frozen=True)
class PersistedAttemptTerminalAuthoritySelectorV2:
    selector: ProviderResourceActionAttemptTerminalAuthoritySelectorV2
    adopted: bool


@dataclasses.dataclass(frozen=True)
class AuthorityHistoryPersistenceResultV2:
    lineages: tuple[PersistedExecutionAuthorityLineageV2, ...]
    terminal_selectors: tuple[PersistedAttemptTerminalAuthoritySelectorV2, ...]


def _require_transaction(connection: sqlalchemy.engine.Connection) -> None:
    if not isinstance(connection, sqlalchemy.engine.Connection):
        raise TypeError('authority history requires a SQLAlchemy connection.')
    if connection.dialect.name != 'postgresql':
        raise AuthorityHistoryTransactionError(
            'Serve039 authority history is PostgreSQL-only.')
    if not connection.in_transaction():
        raise AuthorityHistoryTransactionError(
            'Serve039 authority history requires an existing transaction.')


def _lineage_predicates(key: tuple[uuid.UUID, int, int],) -> tuple[Any, ...]:
    action_id, attempt, generation = key
    table = m4_schema.EXECUTION_AUTHORITY_LINEAGE
    return (table.c.action_id == action_id, table.c.attempt == attempt,
            table.c.request_execution_generation == generation)


def _selector_predicates(key: tuple[uuid.UUID, int]) -> tuple[Any, ...]:
    action_id, attempt = key
    table = m4_schema.ATTEMPT_TERMINAL_AUTHORITY
    return table.c.action_id == action_id, table.c.attempt == attempt


def _parse_lineage_row(
    row: Mapping[str,
                 Any],) -> ProviderResourceActionExecutionAuthorityLineageV2:
    try:
        return ProviderResourceActionExecutionAuthorityLineageV2.from_row(row)
    except (TypeError, ValueError) as error:
        raise AuthorityHistoryCorruption(
            'Stored execution-authority lineage is malformed.') from error


def _parse_selector_row(
    row: Mapping[str, Any],
) -> ProviderResourceActionAttemptTerminalAuthoritySelectorV2:
    try:
        return ProviderResourceActionAttemptTerminalAuthoritySelectorV2.from_row(
            row)
    except (TypeError, ValueError) as error:
        raise AuthorityHistoryCorruption(
            'Stored terminal-authority selector is malformed.') from error


def _read_lineage(
    connection: sqlalchemy.engine.Connection,
    key: tuple[uuid.UUID, int, int],
    *,
    key_share: bool,
) -> ProviderResourceActionExecutionAuthorityLineageV2 | None:
    table = m4_schema.EXECUTION_AUTHORITY_LINEAGE
    statement = sqlalchemy.select(table).where(*_lineage_predicates(key))
    if key_share:
        statement = statement.with_for_update(read=True, key_share=True)
    row = connection.execute(statement).mappings().one_or_none()
    return None if row is None else _parse_lineage_row(row)


def read_execution_authority_lineage_v2(
    connection: sqlalchemy.engine.Connection,
    action_id: uuid.UUID,
    attempt: int,
    request_execution_generation: int,
    *,
    key_share: bool = False,
) -> ProviderResourceActionExecutionAuthorityLineageV2 | None:
    """Read one immutable lineage, optionally taking its class-17 key share."""

    _require_transaction(connection)
    key = (_uuid(action_id, name='lineage read action_id'),
           _positive_integer(attempt, name='lineage read attempt'),
           _positive_integer(request_execution_generation,
                             name='lineage read generation'))
    return _read_lineage(connection, key, key_share=key_share)


def read_attempt_terminal_authority_selector_v2(
    connection: sqlalchemy.engine.Connection,
    action_id: uuid.UUID,
    attempt: int,
) -> ProviderResourceActionAttemptTerminalAuthoritySelectorV2 | None:
    """Read one append-only terminal selector without acquiring a row lock."""

    _require_transaction(connection)
    key = (_uuid(action_id, name='selector read action_id'),
           _positive_integer(attempt, name='selector read attempt'))
    table = m4_schema.ATTEMPT_TERMINAL_AUTHORITY
    row = connection.execute(
        sqlalchemy.select(table).where(
            *_selector_predicates(key))).mappings().one_or_none()
    return None if row is None else _parse_selector_row(row)


def _insert_or_exact_adopt_lineage(
    connection: sqlalchemy.engine.Connection,
    candidate: ProviderResourceActionExecutionAuthorityLineageV2,
) -> PersistedExecutionAuthorityLineageV2:
    table = m4_schema.EXECUTION_AUTHORITY_LINEAGE
    statement = (postgresql.insert(table).values(
        **candidate.row_values()).on_conflict_do_nothing().returning(*table.c))
    inserted = connection.execute(statement).mappings().one_or_none()
    if inserted is not None:
        inserted_lineage = _parse_lineage_row(inserted)
        if inserted_lineage.canonical_bytes != candidate.canonical_bytes:
            raise AuthorityHistoryCorruption(
                'Inserted lineage differs from its typed candidate.')
        return PersistedExecutionAuthorityLineageV2(inserted_lineage,
                                                    adopted=False)
    stored = _read_lineage(connection, candidate.key, key_share=True)
    if stored is None:
        raise AuthorityHistoryCorruption(
            'Lineage insert conflicted on a different unique identity.')
    if stored.canonical_bytes != candidate.canonical_bytes:
        raise AuthorityHistoryCorruption(
            'Existing lineage differs from the exact replay candidate.')
    return PersistedExecutionAuthorityLineageV2(stored, adopted=True)


def _validate_selector_lineage(
    connection: sqlalchemy.engine.Connection,
    selector: ProviderResourceActionAttemptTerminalAuthoritySelectorV2,
) -> None:
    lineage_key = selector.lineage_key
    if lineage_key is None:
        if selector.request_execution_generation == 1:
            unexpected = _read_lineage(
                connection, (selector.action_id, selector.attempt, 1),
                key_share=True)
            if unexpected is not None:
                raise AuthorityHistoryCorruption(
                    'No-claim-start selector conflicts with existing lineage.')
        return
    lineage = _read_lineage(connection, lineage_key, key_share=True)
    if lineage is None:
        raise AuthorityHistoryCorruption(
            'LINEAGE selector names missing execution authority.')
    if (lineage.request_id != selector.request_id or
            lineage.request_input_sha256 != selector.request_input_sha256 or
            lineage.authority_worker_instance_id
            != selector.authority_worker_instance_id or
            lineage.worker_instance_id != selector.worker_instance_id):
        raise AuthorityHistoryCorruption(
            'Terminal selector crosses its named lineage.')
    expected_handler = ('serve_resource_action_launch'
                        if lineage.execution_authority.action_kind
                        is kernel_actions.ActionKind.LAUNCH else
                        'serve_resource_action_down')
    if selector.handler_name != expected_handler:
        raise AuthorityHistoryCorruption(
            'Terminal selector handler crosses lineage action kind.')


def _insert_or_exact_adopt_selector(
    connection: sqlalchemy.engine.Connection,
    candidate: ProviderResourceActionAttemptTerminalAuthoritySelectorV2,
) -> PersistedAttemptTerminalAuthoritySelectorV2:
    _validate_selector_lineage(connection, candidate)
    table = m4_schema.ATTEMPT_TERMINAL_AUTHORITY
    statement = (postgresql.insert(table).values(
        **candidate.row_values()).on_conflict_do_nothing().returning(*table.c))
    inserted = connection.execute(statement).mappings().one_or_none()
    if inserted is not None:
        stored = _parse_selector_row(inserted)
        if stored.canonical_bytes != candidate.canonical_bytes:
            raise AuthorityHistoryCorruption(
                'Inserted terminal selector differs from its candidate.')
        return PersistedAttemptTerminalAuthoritySelectorV2(stored,
                                                           adopted=False)
    statement = (sqlalchemy.select(table).where(
        *_selector_predicates(candidate.key)).with_for_update(read=True,
                                                              key_share=True))
    row = connection.execute(statement).mappings().one_or_none()
    if row is None:
        raise AuthorityHistoryCorruption(
            'Terminal-selector insert conflicted on a different request.')
    stored = _parse_selector_row(row)
    if stored.canonical_bytes != candidate.canonical_bytes:
        raise AuthorityHistoryCorruption(
            'Existing terminal selector differs from the replay candidate.')
    return PersistedAttemptTerminalAuthoritySelectorV2(stored, adopted=True)


def persist_authority_history_v2(
    connection: sqlalchemy.engine.Connection,
    *,
    lineages: tuple[ProviderResourceActionExecutionAuthorityLineageV2,
                    ...] = (),
    terminal_selectors: tuple[
        ProviderResourceActionAttemptTerminalAuthoritySelectorV2, ...] = (),
) -> AuthorityHistoryPersistenceResultV2:
    """Insert or exactly adopt one sorted class-17 authority-history batch.

    The caller owns the transaction and all earlier locks.  Inputs must already
    be in canonical key order; accepting an unordered set here would obscure a
    caller lock-order bug.  Lineage is always visited before selectors.
    """

    _require_transaction(connection)
    if (type(lineages) is not tuple or any(
            type(item) is not ProviderResourceActionExecutionAuthorityLineageV2
            for item in lineages)):
        raise TypeError('authority-history lineages must be an exact tuple.')
    if (type(terminal_selectors) is not tuple or any(
            type(item)
            is not ProviderResourceActionAttemptTerminalAuthoritySelectorV2
            for item in terminal_selectors)):
        raise TypeError('terminal selectors must be an exact tuple.')
    lineage_keys = tuple(item.sort_key for item in lineages)
    selector_keys = tuple(item.sort_key for item in terminal_selectors)
    if lineage_keys != tuple(sorted(set(lineage_keys))):
        raise ValueError('lineage batch must be sorted and key-distinct.')
    if selector_keys != tuple(sorted(set(selector_keys))):
        raise ValueError('selector batch must be sorted and key-distinct.')
    persisted_lineages = tuple(
        _insert_or_exact_adopt_lineage(connection, candidate)
        for candidate in lineages)
    persisted_selectors = tuple(
        _insert_or_exact_adopt_selector(connection, candidate)
        for candidate in terminal_selectors)
    return AuthorityHistoryPersistenceResultV2(
        lineages=persisted_lineages, terminal_selectors=persisted_selectors)


__all__ = [
    'AUTHORITY_HISTORY_LOCK_CLASS_V2',
    'AUTHORITY_HISTORY_PHASE_ORDER_V2',
    'AuthorityHistoryCorruption',
    'AuthorityHistoryError',
    'AuthorityHistoryPersistenceResultV2',
    'AuthorityHistoryTransactionError',
    'PersistedAttemptTerminalAuthoritySelectorV2',
    'PersistedExecutionAuthorityLineageV2',
    'PolicyAdmissionStateV2',
    'ProviderExecutionAuthorityProofV2',
    'ProviderResourceActionApiInstanceSnapshotV2',
    'ProviderResourceActionAttemptTerminalAuthoritySelectorV2',
    'ProviderResourceActionClaimHandoffFenceV2',
    'ProviderResourceActionDispatchMembershipV2',
    'ProviderResourceActionExecutionAuthorityLineageV2',
    'ProviderResourceActionReferenceSnapshotV2',
    'ProviderResourceActionRequestClaimSnapshotV2',
    'RequestTerminalStateV2',
    'TerminalAuthorityDispositionV2',
    'TerminalCauseV2',
    'persist_authority_history_v2',
    'read_attempt_terminal_authority_selector_v2',
    'read_execution_authority_lineage_v2',
]
