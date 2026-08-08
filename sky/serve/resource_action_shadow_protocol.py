"""Closed, pure protocol leaves for native-V2 private shadow execution.

This module contains the shadow values whose complete validation is independent
of database state and of the not-yet-landed shadow execution-history reducer.
It deliberately exposes no permissive JSON wrapper: every accepted object has
an exact key set, a bounded canonical encoding, and a concrete Python type.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import re
from typing import Any, ClassVar, TypeAlias
import unicodedata
import uuid

from sky.serve import resource_action_progress
from sky.serve import resource_actions

_MAX_CANONICAL_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_POSTGRES_BIGINT = 2**63 - 1
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')

JsonObject = dict[str, Any]


class ShadowProtocolContract:
    """Canonical helpers shared by the closed shadow contracts."""

    def canonical_value(self) -> JsonObject:
        raise NotImplementedError

    @property
    def canonical_bytes(self) -> bytes:
        encoded = resource_actions.canonical_json_bytes(self.canonical_value())
        if len(encoded) > _MAX_CANONICAL_BYTES:
            raise ValueError(
                f'{type(self).__name__} exceeds {_MAX_CANONICAL_BYTES} bytes.')
        return encoded

    @property
    def sha256(self) -> str:
        return resource_actions.canonical_sha256(self.canonical_value())


def _closed_object(value: object, *, name: str,
                   keys: frozenset[str]) -> JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    encoded = resource_actions.canonical_json_bytes(value)
    if len(encoded) > _MAX_CANONICAL_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_CANONICAL_BYTES} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if normalized != value:
        raise ValueError(f'{name} is not canonical.')
    return normalized


def _literal(value: object, allowed: tuple[str, ...], *, name: str) -> str:
    if type(value) is not str or value not in allowed:
        raise ValueError(f'{name} is unsupported.')
    return value


def _text(value: object, *, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        size = len(value.encode('utf-8'))
    except UnicodeEncodeError as error:
        raise ValueError(f'{name} must be valid UTF-8 text.') from error
    if (size == 0 or size > _MAX_TEXT_BYTES or '\x00' in value or
            unicodedata.normalize('NFC', value) != value):
        raise ValueError(
            f'{name} must be 1..{_MAX_TEXT_BYTES} canonical UTF-8 bytes.')
    return value


def _uuid(value: object, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be a UUID or canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f'{name} must be a UUID.') from error
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _sha256(value: object, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _timestamp(value: object, *, name: str) -> str:
    if type(value) is not str or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be canonical UTC timestamp text.')
    try:
        datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as error:
        raise ValueError(f'{name} must be a valid UTC timestamp.') from error
    return value


def _positive_integer(value: object, *, name: str) -> int:
    if type(value) is not int or not 1 <= value <= _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a positive signed-int64 integer.')
    return value


def _nonnegative_integer(value: object, *, name: str) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a nonnegative signed-int64 integer.')
    return value


def _version(value: object, expected: int, *, name: str) -> int:
    if type(value) is not int or value != expected:
        raise ValueError(f'{name} must be integer {expected}.')
    return value


def _optional_uuid_value(value: uuid.UUID | None) -> str | None:
    return None if value is None else str(value)


@dataclasses.dataclass(frozen=True)
class ProviderShadowLaunchEffectClaimV1(ShadowProtocolContract):
    """Private-shadow launch origin; action-shaped origins cannot decode."""

    version: int
    decision_id: uuid.UUID
    request_sequence: int
    logical_attempt: int
    request_role: str
    request_id: uuid.UUID
    request_execution_generation: int
    worker_attestation: resource_action_progress.ProviderAuthorityWorkerAttemptAttestationV1
    worker_attestation_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'decision_id', 'request_sequence', 'logical_attempt',
        'request_role', 'request_id', 'request_execution_generation',
        'worker_attestation', 'worker_attestation_sha256'
    })

    def __post_init__(self) -> None:
        _version(self.version, 1, name='shadow launch claim version')
        if type(self.decision_id) is not uuid.UUID:
            raise TypeError('shadow launch claim decision_id must be a UUID.')
        _positive_integer(self.request_sequence,
                          name='shadow launch claim request_sequence')
        _positive_integer(self.logical_attempt,
                          name='shadow launch claim logical_attempt')
        _literal(self.request_role, ('PRIMARY_LAUNCH',),
                 name='shadow launch claim request_role')
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('shadow launch claim request_id must be a UUID.')
        _version(self.request_execution_generation,
                 1,
                 name='shadow launch claim execution generation')
        if type(self.worker_attestation) is not (
                resource_action_progress.
                ProviderAuthorityWorkerAttemptAttestationV1):
            raise TypeError('shadow launch claim attestation has invalid type.')
        digest = _sha256(self.worker_attestation_sha256,
                         name='shadow launch claim attestation hash')
        if digest != self.worker_attestation.sha256:
            raise ValueError('shadow launch claim attestation hash mismatch.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: object) -> ProviderShadowLaunchEffectClaimV1:
        raw = _closed_object(value,
                             name='shadow launch effect claim',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            decision_id=_uuid(raw['decision_id'], name='claim.decision_id'),
            request_sequence=raw['request_sequence'],
            logical_attempt=raw['logical_attempt'],
            request_role=raw['request_role'],
            request_id=_uuid(raw['request_id'], name='claim.request_id'),
            request_execution_generation=raw['request_execution_generation'],
            worker_attestation=(
                resource_action_progress.
                ProviderAuthorityWorkerAttemptAttestationV1.from_value(
                    raw['worker_attestation'])),
            worker_attestation_sha256=raw['worker_attestation_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'decision_id': str(self.decision_id),
            'request_sequence': self.request_sequence,
            'logical_attempt': self.logical_attempt,
            'request_role': 'PRIMARY_LAUNCH',
            'request_id': str(self.request_id),
            'request_execution_generation': 1,
            'worker_attestation': self.worker_attestation.canonical_value(),
            'worker_attestation_sha256': self.worker_attestation_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ServeShadowCandidateRequestFallbackEvidenceV1(ShadowProtocolContract):
    """Bounded reducer evidence when no strict private return is available."""

    version: int
    decision_id: uuid.UUID
    request_sequence: int
    request_id: uuid.UUID
    request_terminal_state: str
    fallback_reason: str
    terminal_history_sha256: str
    journal_class: str
    provider_io_boundary: str
    provider_progress_revision: int
    provider_progress_sha256: str | None
    provider_operation_id: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'decision_id', 'request_sequence', 'request_id',
        'request_terminal_state', 'fallback_reason', 'terminal_history_sha256',
        'journal_class', 'provider_io_boundary', 'provider_progress_revision',
        'provider_progress_sha256', 'provider_operation_id'
    })

    def __post_init__(self) -> None:
        _version(self.version, 1, name='shadow fallback evidence version')
        if type(self.decision_id) is not uuid.UUID:
            raise TypeError('shadow fallback decision_id must be a UUID.')
        _positive_integer(self.request_sequence,
                          name='shadow fallback request_sequence')
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('shadow fallback request_id must be a UUID.')
        _literal(self.request_terminal_state,
                 ('SUCCEEDED', 'FAILED', 'CANCELLED'),
                 name='shadow fallback terminal state')
        _literal(
            self.fallback_reason,
            ('missing_handler_return', 'request_failed', 'request_cancelled'),
            name='shadow fallback reason')
        _sha256(self.terminal_history_sha256,
                name='shadow fallback terminal history hash')
        _literal(self.journal_class, ('not_started_empty', 'valid_nonterminal',
                                      'valid_succeeded', 'invalid'),
                 name='shadow fallback journal class')
        _literal(self.provider_io_boundary,
                 ('NOT_STARTED', 'INTENT_COMMITTED', 'SUBMITTED_OR_AMBIGUOUS'),
                 name='shadow fallback provider I/O boundary')
        _nonnegative_integer(self.provider_progress_revision,
                             name='shadow fallback progress revision')
        if self.provider_progress_sha256 is not None:
            _sha256(self.provider_progress_sha256,
                    name='shadow fallback progress hash')
        if self.provider_operation_id is not None:
            _text(self.provider_operation_id,
                  name='shadow fallback provider operation ID')
        if ((self.provider_progress_revision == 0)
                != (self.provider_progress_sha256 is None)):
            raise ValueError('shadow fallback progress revision/hash mismatch.')
        if self.provider_io_boundary == 'NOT_STARTED' and (
                self.provider_operation_id is not None):
            raise ValueError(
                'NOT_STARTED fallback cannot have an operation ID.')
        expected_reason = {
            'FAILED': 'request_failed',
            'CANCELLED': 'request_cancelled',
        }.get(self.request_terminal_state)
        if (expected_reason is not None and
                self.fallback_reason != expected_reason):
            raise ValueError('shadow fallback reason conflicts with terminal '
                             'state.')
        if (self.request_terminal_state == 'SUCCEEDED' and
                self.fallback_reason != 'missing_handler_return'):
            raise ValueError('successful fallback requires missing return.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls,
            value: object) -> ServeShadowCandidateRequestFallbackEvidenceV1:
        raw = _closed_object(value,
                             name='shadow request fallback evidence',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   decision_id=_uuid(raw['decision_id'],
                                     name='fallback.decision_id'),
                   request_sequence=raw['request_sequence'],
                   request_id=_uuid(raw['request_id'],
                                    name='fallback.request_id'),
                   request_terminal_state=raw['request_terminal_state'],
                   fallback_reason=raw['fallback_reason'],
                   terminal_history_sha256=raw['terminal_history_sha256'],
                   journal_class=raw['journal_class'],
                   provider_io_boundary=raw['provider_io_boundary'],
                   provider_progress_revision=raw['provider_progress_revision'],
                   provider_progress_sha256=raw['provider_progress_sha256'],
                   provider_operation_id=raw['provider_operation_id'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'decision_id': str(self.decision_id),
            'request_sequence': self.request_sequence,
            'request_id': str(self.request_id),
            'request_terminal_state': self.request_terminal_state,
            'fallback_reason': self.fallback_reason,
            'terminal_history_sha256': self.terminal_history_sha256,
            'journal_class': self.journal_class,
            'provider_io_boundary': self.provider_io_boundary,
            'provider_progress_revision': self.provider_progress_revision,
            'provider_progress_sha256': self.provider_progress_sha256,
            'provider_operation_id': self.provider_operation_id,
        }


@dataclasses.dataclass(frozen=True)
class ProviderShadowAuthorityFenceCommitmentClaimV2(ShadowProtocolContract):
    """Time-free shadow member of an authority-fence commitment."""

    version: ClassVar[int] = 2
    claim_kind: str
    request_id: uuid.UUID
    handler_name: str
    decision_id: uuid.UUID
    request_sequence: int
    request_role: str
    immutable_payload_sha256: str
    request_input_sha256: str
    execution_generation: int
    claim_owner_api_instance_id: uuid.UUID
    claim_token_sha256: str
    prior_cancel_requested_at: str | None
    preterminal_history_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'claim_kind', 'request_id', 'handler_name', 'decision_id',
        'request_sequence', 'request_role', 'immutable_payload_sha256',
        'request_input_sha256', 'execution_generation',
        'claim_owner_api_instance_id', 'claim_token_sha256',
        'prior_cancel_requested_at', 'preterminal_history_sha256'
    })

    def __post_init__(self) -> None:
        _literal(self.claim_kind, ('shadow_candidate',),
                 name='fence commitment claim kind')
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('fence commitment request_id must be a UUID.')
        _literal(
            self.handler_name,
            ('serve_shadow_candidate_launch', 'serve_shadow_candidate_down'),
            name='fence commitment handler')
        if type(self.decision_id) is not uuid.UUID:
            raise TypeError('fence commitment decision_id must be a UUID.')
        _positive_integer(self.request_sequence,
                          name='fence commitment request_sequence')
        _literal(self.request_role, ('PRIMARY_LAUNCH', 'PRIMARY_DOWN'),
                 name='fence commitment request role')
        _sha256(self.immutable_payload_sha256,
                name='fence commitment immutable payload hash')
        _sha256(self.request_input_sha256,
                name='fence commitment request input hash')
        _version(self.execution_generation,
                 1,
                 name='fence commitment execution generation')
        if type(self.claim_owner_api_instance_id) is not uuid.UUID:
            raise TypeError('fence commitment owner must be a UUID.')
        _sha256(self.claim_token_sha256,
                name='fence commitment claim token hash')
        if self.prior_cancel_requested_at is not None:
            _timestamp(self.prior_cancel_requested_at,
                       name='fence commitment cancellation time')
        _sha256(self.preterminal_history_sha256,
                name='fence commitment preterminal history hash')
        expected_role = ('PRIMARY_LAUNCH'
                         if self.handler_name.endswith('_launch') else
                         'PRIMARY_DOWN')
        if self.request_role != expected_role:
            raise ValueError('fence commitment handler/role mismatch.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls,
            value: object) -> ProviderShadowAuthorityFenceCommitmentClaimV2:
        raw = _closed_object(value,
                             name='shadow fence commitment claim',
                             keys=cls._KEYS)
        return cls(claim_kind=raw['claim_kind'],
                   request_id=_uuid(raw['request_id'], name='claim.request_id'),
                   handler_name=raw['handler_name'],
                   decision_id=_uuid(raw['decision_id'],
                                     name='claim.decision_id'),
                   request_sequence=raw['request_sequence'],
                   request_role=raw['request_role'],
                   immutable_payload_sha256=raw['immutable_payload_sha256'],
                   request_input_sha256=raw['request_input_sha256'],
                   execution_generation=raw['execution_generation'],
                   claim_owner_api_instance_id=_uuid(
                       raw['claim_owner_api_instance_id'], name='claim.owner'),
                   claim_token_sha256=raw['claim_token_sha256'],
                   prior_cancel_requested_at=raw['prior_cancel_requested_at'],
                   preterminal_history_sha256=raw['preterminal_history_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'claim_kind': 'shadow_candidate',
            'request_id': str(self.request_id),
            'handler_name': self.handler_name,
            'decision_id': str(self.decision_id),
            'request_sequence': self.request_sequence,
            'request_role': self.request_role,
            'immutable_payload_sha256': self.immutable_payload_sha256,
            'request_input_sha256': self.request_input_sha256,
            'execution_generation': 1,
            'claim_owner_api_instance_id': str(self.claim_owner_api_instance_id
                                              ),
            'claim_token_sha256': self.claim_token_sha256,
            'prior_cancel_requested_at': self.prior_cancel_requested_at,
            'preterminal_history_sha256': self.preterminal_history_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityFenceCommitmentProjectionV2(ShadowProtocolContract):
    """Closed time-free authority-fence projection for terminal adoption."""

    version: int
    fence_kind: str
    operation_id: uuid.UUID
    authority_worker_instance_id: uuid.UUID
    claims: tuple[ProviderShadowAuthorityFenceCommitmentClaimV2, ...]
    origin_revoking_handoff_id: uuid.UUID | None = None
    recovery_id: uuid.UUID | None = None
    pod_uid: uuid.UUID | None = None
    prior_lease_state: str | None = None
    lease_generation: int | None = None
    prior_lease_revision: int | None = None
    terminal_lease_revision: int | None = None
    preserved_revocation_reason: str | None = None
    preserved_revocation_owner_id: uuid.UUID | None = None
    supersession_id: uuid.UUID | None = None
    cohort_id: str | None = None
    source_lease_generation: int | None = None
    source_lease_revision: int | None = None
    committed_lease_generation: int | None = None
    committed_lease_revision: int | None = None
    prior_api_instance_id: uuid.UUID | None = None
    current_api_instance_id: uuid.UUID | None = None
    prior_execution_owner_sha256: str | None = None
    current_execution_owner_sha256: str | None = None
    container_supersession_proof_sha256: str | None = None

    _STALE_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'fence_kind', 'operation_id', 'origin_revoking_handoff_id',
        'authority_worker_instance_id', 'lease_generation',
        'prior_lease_revision', 'terminal_lease_revision', 'claims'
    })
    _COLD_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'fence_kind', 'operation_id', 'recovery_id',
        'authority_worker_instance_id', 'pod_uid', 'prior_lease_state',
        'lease_generation', 'prior_lease_revision', 'terminal_lease_revision',
        'preserved_revocation_reason', 'preserved_revocation_owner_id', 'claims'
    })
    _PROCESS_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'fence_kind', 'operation_id', 'supersession_id', 'cohort_id',
        'authority_worker_instance_id', 'source_lease_generation',
        'source_lease_revision', 'committed_lease_generation',
        'committed_lease_revision', 'prior_api_instance_id',
        'current_api_instance_id', 'prior_execution_owner_sha256',
        'current_execution_owner_sha256', 'container_supersession_proof_sha256',
        'claims'
    })

    def __post_init__(self) -> None:
        _version(self.version, 2, name='fence projection version')
        _literal(self.fence_kind,
                 ('stale_owner', 'cold_recovery', 'process_supersession'),
                 name='fence projection kind')
        if type(self.operation_id) is not uuid.UUID:
            raise TypeError('fence projection operation ID must be a UUID.')
        if type(self.authority_worker_instance_id) is not uuid.UUID:
            raise TypeError('fence projection worker ID must be a UUID.')
        if type(self.claims) is not tuple or any(
                type(claim) is not ProviderShadowAuthorityFenceCommitmentClaimV2
                for claim in self.claims):
            raise TypeError('fence projection claims must be typed tuple.')
        if len(self.claims) > 64:
            raise ValueError('fence projection has more than 64 claims.')
        order = tuple((str(claim.request_id), claim.execution_generation)
                      for claim in self.claims)
        if order != tuple(sorted(order)) or len(order) != len(set(order)):
            raise ValueError('fence projection claims are not uniquely sorted.')
        if self.fence_kind == 'stale_owner':
            if any(value is None for value in (self.origin_revoking_handoff_id,
                                               self.lease_generation,
                                               self.prior_lease_revision,
                                               self.terminal_lease_revision)):
                raise ValueError('stale-owner fence fields are incomplete.')
            if self.operation_id != self.origin_revoking_handoff_id:
                raise ValueError('stale-owner operation ID mismatch.')
            self._require_unused('origin_revoking_handoff_id')
        elif self.fence_kind == 'cold_recovery':
            if any(value is None
                   for value in (self.recovery_id, self.pod_uid,
                                 self.prior_lease_state, self.lease_generation,
                                 self.prior_lease_revision,
                                 self.terminal_lease_revision)):
                raise ValueError('cold-recovery fence fields are incomplete.')
            if self.operation_id != self.recovery_id:
                raise ValueError('cold-recovery operation ID mismatch.')
            _literal(self.prior_lease_state, ('ACTIVE', 'REVOKED'),
                     name='cold-recovery prior lease state')
            if ((self.preserved_revocation_reason is None)
                    != (self.preserved_revocation_owner_id is None)):
                raise ValueError('cold-recovery revocation pair is partial.')
            if self.preserved_revocation_reason is not None:
                _literal(self.preserved_revocation_reason, ('STALE_HANDOFF',),
                         name='cold-recovery revocation reason')
            self._require_unused('recovery_id')
        else:
            if any(value is None
                   for value in (self.supersession_id, self.cohort_id,
                                 self.source_lease_generation,
                                 self.source_lease_revision,
                                 self.committed_lease_generation,
                                 self.committed_lease_revision,
                                 self.prior_api_instance_id,
                                 self.current_api_instance_id,
                                 self.prior_execution_owner_sha256,
                                 self.current_execution_owner_sha256,
                                 self.container_supersession_proof_sha256)):
                raise ValueError('process-supersession fields are incomplete.')
            _text(self.cohort_id, name='fence projection cohort ID')
            for digest in (self.prior_execution_owner_sha256,
                           self.current_execution_owner_sha256,
                           self.container_supersession_proof_sha256):
                _sha256(digest, name='fence projection digest')
            self._require_unused('supersession_id')
        for value in (self.lease_generation, self.prior_lease_revision,
                      self.terminal_lease_revision,
                      self.source_lease_generation, self.source_lease_revision,
                      self.committed_lease_generation,
                      self.committed_lease_revision):
            if value is not None:
                _positive_integer(value, name='fence projection revision')
        _ = self.canonical_bytes

    def _require_unused(self, selected: str) -> None:
        groups = {
            'origin_revoking_handoff_id':
                ('recovery_id', 'pod_uid', 'prior_lease_state',
                 'preserved_revocation_reason', 'preserved_revocation_owner_id',
                 'supersession_id', 'cohort_id', 'source_lease_generation',
                 'source_lease_revision', 'committed_lease_generation',
                 'committed_lease_revision', 'prior_api_instance_id',
                 'current_api_instance_id', 'prior_execution_owner_sha256',
                 'current_execution_owner_sha256',
                 'container_supersession_proof_sha256'),
            'recovery_id':
                ('origin_revoking_handoff_id', 'supersession_id', 'cohort_id',
                 'source_lease_generation', 'source_lease_revision',
                 'committed_lease_generation', 'committed_lease_revision',
                 'prior_api_instance_id', 'current_api_instance_id',
                 'prior_execution_owner_sha256',
                 'current_execution_owner_sha256',
                 'container_supersession_proof_sha256'),
            'supersession_id':
                ('origin_revoking_handoff_id', 'recovery_id', 'pod_uid',
                 'prior_lease_state', 'lease_generation',
                 'prior_lease_revision', 'terminal_lease_revision',
                 'preserved_revocation_reason', 'preserved_revocation_owner_id'
                ),
        }
        if any(getattr(self, name) is not None for name in groups[selected]):
            raise ValueError('fence projection contains cross-kind fields.')

    @classmethod
    def from_value(
            cls, value: object) -> ProviderAuthorityFenceCommitmentProjectionV2:
        if type(value) is not dict:
            raise TypeError('fence commitment projection must be an object.')
        kind = _literal(
            value.get('fence_kind'),
            ('stale_owner', 'cold_recovery', 'process_supersession'),
            name='fence commitment projection kind')
        keys = {
            'stale_owner': cls._STALE_KEYS,
            'cold_recovery': cls._COLD_KEYS,
            'process_supersession': cls._PROCESS_KEYS,
        }.get(kind)
        if keys is None:
            raise ValueError('fence commitment projection kind unsupported.')
        raw = _closed_object(value,
                             name='fence commitment projection',
                             keys=keys)
        common = {
            'version': raw['version'],
            'fence_kind': kind,
            'operation_id': _uuid(raw['operation_id'], name='fence.operation'),
            'authority_worker_instance_id': _uuid(
                raw['authority_worker_instance_id'], name='fence.worker'),
            'claims': tuple(
                ProviderShadowAuthorityFenceCommitmentClaimV2.from_value(item)
                for item in raw['claims']),
        }
        uuid_fields = {
            'origin_revoking_handoff_id', 'recovery_id', 'pod_uid',
            'preserved_revocation_owner_id', 'supersession_id',
            'prior_api_instance_id', 'current_api_instance_id'
        }
        for name in keys - set(common) - {'claims', 'fence_kind'}:
            value_item = raw[name]
            if name in uuid_fields and value_item is not None:
                value_item = _uuid(value_item, name=f'fence.{name}')
            common[name] = value_item
        return cls(**common)

    def canonical_value(self) -> JsonObject:
        if self.fence_kind == 'stale_owner':
            return {
                'version': 2,
                'fence_kind': self.fence_kind,
                'operation_id': str(self.operation_id),
                'origin_revoking_handoff_id': str(
                    self.origin_revoking_handoff_id),
                'authority_worker_instance_id': str(
                    self.authority_worker_instance_id),
                'lease_generation': self.lease_generation,
                'prior_lease_revision': self.prior_lease_revision,
                'terminal_lease_revision': self.terminal_lease_revision,
                'claims': [claim.canonical_value() for claim in self.claims],
            }
        if self.fence_kind == 'cold_recovery':
            return {
                'version': 2,
                'fence_kind': self.fence_kind,
                'operation_id': str(self.operation_id),
                'recovery_id': str(self.recovery_id),
                'authority_worker_instance_id': str(
                    self.authority_worker_instance_id),
                'pod_uid': str(self.pod_uid),
                'prior_lease_state': self.prior_lease_state,
                'lease_generation': self.lease_generation,
                'prior_lease_revision': self.prior_lease_revision,
                'terminal_lease_revision': self.terminal_lease_revision,
                'preserved_revocation_reason': self.preserved_revocation_reason,
                'preserved_revocation_owner_id': _optional_uuid_value(
                    self.preserved_revocation_owner_id),
                'claims': [claim.canonical_value() for claim in self.claims],
            }
        return {
            'version': 2,
            'fence_kind': self.fence_kind,
            'operation_id': str(self.operation_id),
            'supersession_id': str(self.supersession_id),
            'cohort_id': self.cohort_id,
            'authority_worker_instance_id': str(
                self.authority_worker_instance_id),
            'source_lease_generation': self.source_lease_generation,
            'source_lease_revision': self.source_lease_revision,
            'committed_lease_generation': self.committed_lease_generation,
            'committed_lease_revision': self.committed_lease_revision,
            'prior_api_instance_id': str(self.prior_api_instance_id),
            'current_api_instance_id': str(self.current_api_instance_id),
            'prior_execution_owner_sha256': self.prior_execution_owner_sha256,
            'current_execution_owner_sha256':
                self.current_execution_owner_sha256,
            'container_supersession_proof_sha256':
                self.container_supersession_proof_sha256,
            'claims': [claim.canonical_value() for claim in self.claims],
        }


@dataclasses.dataclass(frozen=True)
class ProviderShadowTerminalCommitmentV1(ShadowProtocolContract):
    """Permanent, independently bounded terminal-winner commitment."""

    version: int
    request_id: uuid.UUID
    request_input_sha256: str
    immutable_payload_sha256: str
    handler_name: str
    request_execution_generation: int
    authority_worker_instance_id: uuid.UUID | None
    worker_instance_id: uuid.UUID | None
    claim_token_sha256: str | None
    winner_kind: str
    trusted_mode: str
    terminal_state: str
    request_return_sha256: str | None
    fixed_failure_code: str | None
    prior_cancel_requested_at: str | None
    fence_operation_kind: str | None
    fence_operation_id: uuid.UUID | None
    fence_operation_commitment: ProviderAuthorityFenceCommitmentProjectionV2 | None
    fence_operation_commitment_sha256: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'request_id', 'request_input_sha256',
        'immutable_payload_sha256', 'handler_name',
        'request_execution_generation', 'authority_worker_instance_id',
        'worker_instance_id', 'claim_token_sha256', 'winner_kind',
        'trusted_mode', 'terminal_state', 'request_return_sha256',
        'fixed_failure_code', 'prior_cancel_requested_at',
        'fence_operation_kind', 'fence_operation_id',
        'fence_operation_commitment', 'fence_operation_commitment_sha256'
    })

    def __post_init__(self) -> None:
        _version(self.version, 1, name='shadow terminal commitment version')
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('terminal commitment request_id must be a UUID.')
        _sha256(self.request_input_sha256,
                name='terminal commitment request input hash')
        _sha256(self.immutable_payload_sha256,
                name='terminal commitment immutable payload hash')
        _literal(
            self.handler_name,
            ('serve_shadow_candidate_launch', 'serve_shadow_candidate_down'),
            name='terminal commitment handler')
        if self.request_execution_generation not in (0, 1):
            raise ValueError('terminal commitment generation must be 0 or 1.')
        worker_bundle = (self.authority_worker_instance_id,
                         self.worker_instance_id, self.claim_token_sha256)
        if self.request_execution_generation == 0:
            if any(value is not None for value in worker_bundle):
                raise ValueError('generation-zero terminal has worker fields.')
        else:
            if any(value is None for value in worker_bundle):
                raise ValueError('generation-one terminal lacks worker fields.')
            if (type(self.authority_worker_instance_id) is not uuid.UUID or
                    type(self.worker_instance_id) is not uuid.UUID):
                raise TypeError('terminal worker identities must be UUIDs.')
            _sha256(self.claim_token_sha256,
                    name='terminal commitment token hash')
        _literal(
            self.winner_kind,
            ('handler_return', 'post_claim_failure',
             'owner_acknowledged_cancellation', 'owner_quiesced_lease_loss',
             'terminal_before_claim_start', 'claim_start_not_representable',
             'authority_fence_cancellation'),
            name='terminal commitment winner')
        _literal(
            self.trusted_mode,
            ('PRIVATE_HANDLER_RETURN', 'PRIVATE_POST_CLAIM_FAILURE',
             'CLAIM_START_NOT_REPRESENTABLE', 'CLAIM_REAUTHORIZATION_FAILED',
             'OWNER_ACK_CANCEL', 'OWNER_QUIESCED_LEASE_LOSS',
             'TERMINAL_BEFORE_CLAIM_START', 'STALE_OWNER_FENCE',
             'COLD_RECOVERY_FENCE', 'PROCESS_SUPERSESSION_FENCE'),
            name='terminal commitment trusted mode')
        _literal(self.terminal_state, ('SUCCEEDED', 'FAILED', 'CANCELLED'),
                 name='terminal commitment state')
        if self.request_return_sha256 is not None:
            _sha256(self.request_return_sha256,
                    name='terminal commitment return hash')
        if self.fixed_failure_code is not None:
            _literal(self.fixed_failure_code,
                     ('private_handler_failed',
                      'provider_authority_reauthorization_failed',
                      'private_request_failed_before_claim',
                      'provider_authority_not_representable_at_claim'),
                     name='terminal commitment failure code')
        if self.prior_cancel_requested_at is not None:
            _timestamp(self.prior_cancel_requested_at,
                       name='terminal commitment cancellation time')
        fence_bundle = (self.fence_operation_kind, self.fence_operation_id,
                        self.fence_operation_commitment,
                        self.fence_operation_commitment_sha256)
        is_fence = self.winner_kind == 'authority_fence_cancellation'
        if is_fence != all(value is not None for value in fence_bundle):
            raise ValueError('terminal commitment fence bundle is partial.')
        if not is_fence and any(value is not None for value in fence_bundle):
            raise ValueError('non-fence terminal has fence fields.')
        if is_fence:
            _literal(self.fence_operation_kind,
                     ('stale_owner', 'cold_recovery', 'process_supersession'),
                     name='terminal commitment fence kind')
            if type(self.fence_operation_id) is not uuid.UUID:
                raise TypeError('terminal fence operation ID must be a UUID.')
            if type(self.fence_operation_commitment) is not (
                    ProviderAuthorityFenceCommitmentProjectionV2):
                raise TypeError('terminal fence commitment has invalid type.')
            if (self.fence_operation_id
                    != self.fence_operation_commitment.operation_id or
                    self.fence_operation_kind
                    != self.fence_operation_commitment.fence_kind):
                raise ValueError('terminal fence identity mismatch.')
            digest = _sha256(self.fence_operation_commitment_sha256,
                             name='terminal fence commitment hash')
            if digest != self.fence_operation_commitment.sha256:
                raise ValueError('terminal fence commitment hash mismatch.')
        self._validate_winner_matrix()
        _ = self.canonical_bytes

    def _validate_winner_matrix(self) -> None:
        expected = {
            'handler_return': ('PRIVATE_HANDLER_RETURN', 'SUCCEEDED'),
            'owner_acknowledged_cancellation':
                ('OWNER_ACK_CANCEL', 'CANCELLED'),
            'owner_quiesced_lease_loss':
                ('OWNER_QUIESCED_LEASE_LOSS', 'CANCELLED'),
            'terminal_before_claim_start':
                ('TERMINAL_BEFORE_CLAIM_START', 'CANCELLED'),
            'claim_start_not_representable':
                ('CLAIM_START_NOT_REPRESENTABLE', 'FAILED'),
        }
        pair = expected.get(self.winner_kind)
        if pair is not None and pair != (self.trusted_mode,
                                         self.terminal_state):
            raise ValueError('terminal commitment winner matrix mismatch.')
        if self.winner_kind == 'post_claim_failure':
            if (self.trusted_mode not in ('PRIVATE_POST_CLAIM_FAILURE',
                                          'CLAIM_REAUTHORIZATION_FAILED') or
                    self.terminal_state != 'FAILED'):
                raise ValueError('post-claim terminal matrix mismatch.')
        if self.winner_kind == 'authority_fence_cancellation':
            fence_operation_kind = _literal(
                self.fence_operation_kind,
                ('stale_owner', 'cold_recovery', 'process_supersession'),
                name='terminal commitment fence kind')
            expected_mode = {
                'stale_owner': 'STALE_OWNER_FENCE',
                'cold_recovery': 'COLD_RECOVERY_FENCE',
                'process_supersession': 'PROCESS_SUPERSESSION_FENCE',
            }[fence_operation_kind]
            if (self.trusted_mode != expected_mode or
                    self.terminal_state != 'CANCELLED'):
                raise ValueError('authority-fence terminal matrix mismatch.')
        has_return = self.request_return_sha256 is not None
        if has_return != (self.winner_kind == 'handler_return'):
            raise ValueError('terminal handler return hash has wrong presence.')
        if ((self.winner_kind == 'owner_acknowledged_cancellation')
                != (self.prior_cancel_requested_at is not None)):
            raise ValueError('terminal cancellation time has wrong presence.')

    @classmethod
    def from_value(cls, value: object) -> ProviderShadowTerminalCommitmentV1:
        raw = _closed_object(value,
                             name='shadow terminal commitment',
                             keys=cls._KEYS)
        fence = (None if raw['fence_operation_commitment'] is None else
                 ProviderAuthorityFenceCommitmentProjectionV2.from_value(
                     raw['fence_operation_commitment']))
        return cls(
            version=raw['version'],
            request_id=_uuid(raw['request_id'], name='terminal.request_id'),
            request_input_sha256=raw['request_input_sha256'],
            immutable_payload_sha256=raw['immutable_payload_sha256'],
            handler_name=raw['handler_name'],
            request_execution_generation=raw['request_execution_generation'],
            authority_worker_instance_id=(
                None if raw['authority_worker_instance_id'] is None else _uuid(
                    raw['authority_worker_instance_id'],
                    name='terminal.authority_worker')),
            worker_instance_id=(None if raw['worker_instance_id'] is None else
                                _uuid(raw['worker_instance_id'],
                                      name='terminal.worker')),
            claim_token_sha256=raw['claim_token_sha256'],
            winner_kind=raw['winner_kind'],
            trusted_mode=raw['trusted_mode'],
            terminal_state=raw['terminal_state'],
            request_return_sha256=raw['request_return_sha256'],
            fixed_failure_code=raw['fixed_failure_code'],
            prior_cancel_requested_at=raw['prior_cancel_requested_at'],
            fence_operation_kind=raw['fence_operation_kind'],
            fence_operation_id=(None if raw['fence_operation_id'] is None else
                                _uuid(raw['fence_operation_id'],
                                      name='terminal.fence_operation')),
            fence_operation_commitment=fence,
            fence_operation_commitment_sha256=raw[
                'fence_operation_commitment_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'request_id': str(self.request_id),
            'request_input_sha256': self.request_input_sha256,
            'immutable_payload_sha256': self.immutable_payload_sha256,
            'handler_name': self.handler_name,
            'request_execution_generation': self.request_execution_generation,
            'authority_worker_instance_id': _optional_uuid_value(
                self.authority_worker_instance_id),
            'worker_instance_id': _optional_uuid_value(self.worker_instance_id),
            'claim_token_sha256': self.claim_token_sha256,
            'winner_kind': self.winner_kind,
            'trusted_mode': self.trusted_mode,
            'terminal_state': self.terminal_state,
            'request_return_sha256': self.request_return_sha256,
            'fixed_failure_code': self.fixed_failure_code,
            'prior_cancel_requested_at': self.prior_cancel_requested_at,
            'fence_operation_kind': self.fence_operation_kind,
            'fence_operation_id': _optional_uuid_value(self.fence_operation_id),
            'fence_operation_commitment':
                (None if self.fence_operation_commitment is None else
                 self.fence_operation_commitment.canonical_value()),
            'fence_operation_commitment_sha256':
                self.fence_operation_commitment_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ProviderShadowSettlementCommitmentV1(ShadowProtocolContract):
    """Permanent settlement commitment used by graph and receipt adoption."""

    version: int
    operation_id: uuid.UUID
    decision_id: uuid.UUID
    request_sequence: int
    request_role: str
    terminal_history_sha256: str
    new_write_source_sha256: str
    settlement_projection_sha256: str
    successor_kind: str | None
    successor_decision_id: uuid.UUID | None
    successor_request_sequence: int | None
    settled_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'operation_id', 'decision_id', 'request_sequence',
        'request_role', 'terminal_history_sha256', 'new_write_source_sha256',
        'settlement_projection_sha256', 'successor_kind',
        'successor_decision_id', 'successor_request_sequence', 'settled_at'
    })

    def __post_init__(self) -> None:
        _version(self.version, 1, name='shadow settlement commitment version')
        for name, value in (('operation_id', self.operation_id),
                            ('decision_id', self.decision_id)):
            if type(value) is not uuid.UUID:
                raise TypeError(f'settlement commitment {name} must be UUID.')
        _positive_integer(self.request_sequence,
                          name='settlement request_sequence')
        _literal(self.request_role, ('PRIMARY_LAUNCH', 'PRIMARY_DOWN'),
                 name='settlement request role')
        for name, digest in (('terminal history', self.terminal_history_sha256),
                             ('new-write source', self.new_write_source_sha256),
                             ('projection', self.settlement_projection_sha256)):
            _sha256(digest, name=f'settlement {name} hash')
        if self.successor_kind is not None:
            _literal(self.successor_kind,
                     ('retry_same_plan', 'observe_same_plan', 'partial_down'),
                     name='settlement successor kind')
        successor_bundle = (self.successor_decision_id,
                            self.successor_request_sequence)
        if (self.successor_kind
                is None) != all(value is None for value in successor_bundle):
            raise ValueError('settlement successor identity is partial.')
        if self.successor_kind is not None:
            if type(self.successor_decision_id) is not uuid.UUID:
                raise TypeError('settlement successor decision must be UUID.')
            _positive_integer(self.successor_request_sequence,
                              name='settlement successor sequence')
        _timestamp(self.settled_at, name='settlement time')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: object) -> ProviderShadowSettlementCommitmentV1:
        raw = _closed_object(value,
                             name='shadow settlement commitment',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            operation_id=_uuid(raw['operation_id'],
                               name='settlement.operation_id'),
            decision_id=_uuid(raw['decision_id'],
                              name='settlement.decision_id'),
            request_sequence=raw['request_sequence'],
            request_role=raw['request_role'],
            terminal_history_sha256=raw['terminal_history_sha256'],
            new_write_source_sha256=raw['new_write_source_sha256'],
            settlement_projection_sha256=raw['settlement_projection_sha256'],
            successor_kind=raw['successor_kind'],
            successor_decision_id=(None if raw['successor_decision_id'] is None
                                   else _uuid(raw['successor_decision_id'],
                                              name='settlement.successor')),
            successor_request_sequence=raw['successor_request_sequence'],
            settled_at=raw['settled_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'operation_id': str(self.operation_id),
            'decision_id': str(self.decision_id),
            'request_sequence': self.request_sequence,
            'request_role': self.request_role,
            'terminal_history_sha256': self.terminal_history_sha256,
            'new_write_source_sha256': self.new_write_source_sha256,
            'settlement_projection_sha256': self.settlement_projection_sha256,
            'successor_kind': self.successor_kind,
            'successor_decision_id': _optional_uuid_value(
                self.successor_decision_id),
            'successor_request_sequence': self.successor_request_sequence,
            'settled_at': self.settled_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderShadowSettlementReceiptV1(ShadowProtocolContract):
    """Hash-paired permanent receipt for a settlement commitment."""

    commitment: ProviderShadowSettlementCommitmentV1
    commitment_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'commitment', 'commitment_sha256'})

    def __post_init__(self) -> None:
        if type(self.commitment) is not ProviderShadowSettlementCommitmentV1:
            raise TypeError('shadow settlement receipt commitment invalid.')
        digest = _sha256(self.commitment_sha256,
                         name='settlement receipt commitment hash')
        if digest != self.commitment.sha256:
            raise ValueError('settlement receipt commitment hash mismatch.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: object) -> ProviderShadowSettlementReceiptV1:
        raw = _closed_object(value,
                             name='shadow settlement receipt',
                             keys=cls._KEYS)
        return cls(commitment=ProviderShadowSettlementCommitmentV1.from_value(
            raw['commitment']),
                   commitment_sha256=raw['commitment_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'commitment': self.commitment.canonical_value(),
            'commitment_sha256': self.commitment_sha256,
        }


def build_shadow_settlement_receipt(
    commitment: ProviderShadowSettlementCommitmentV1,
) -> ProviderShadowSettlementReceiptV1:
    """Build the exact hash-paired receipt from its sole trusted source."""

    if type(commitment) is not ProviderShadowSettlementCommitmentV1:
        raise TypeError('settlement receipt source has invalid type.')
    return ProviderShadowSettlementReceiptV1(
        commitment=commitment, commitment_sha256=commitment.sha256)


@dataclasses.dataclass(frozen=True)
class ProviderShadowSuccessorKeyAbsenceV2(ShadowProtocolContract):
    """Exact locked absence of every same-parent successor key."""

    child: bool
    execution_history: bool
    private_correlation: bool
    deterministic_request: bool
    deterministic_queue: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'child', 'execution_history', 'private_correlation',
        'deterministic_request', 'deterministic_queue'
    })

    def __post_init__(self) -> None:
        if any(
                type(value) is not bool or value is not True
                for value in (self.child, self.execution_history,
                              self.private_correlation,
                              self.deterministic_request,
                              self.deterministic_queue)):
            raise ValueError('shadow successor absence requires literal true.')

    @classmethod
    def from_value(cls, value: object) -> ProviderShadowSuccessorKeyAbsenceV2:
        raw = _closed_object(value,
                             name='shadow successor key absence',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'child': True,
            'execution_history': True,
            'private_correlation': True,
            'deterministic_request': True,
            'deterministic_queue': True,
        }


ShadowTerminalCommitment: TypeAlias = ProviderShadowTerminalCommitmentV1
ShadowSettlementCommitment: TypeAlias = ProviderShadowSettlementCommitmentV1
