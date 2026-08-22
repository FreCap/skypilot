"""Durable PostgreSQL repository for Serve physical-pool observations.

This module is intentionally inert: no controller, broker, planner, or status
path imports it yet.  It provides the canonical typed observation boundary for
the sequenced reserved-fill path without creating a second capacity cache.
"""

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import enum
import hashlib
import hmac
import json
import math
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.engine import RowMapping

from sky.adaptors import common as adaptors_common
from sky.serve import pool_capacity_observation_schema as observation_schema
from sky.serve import reserved_fill_projection_authority
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import serve_state_schema

# Both modules import this repository through their controller paths.  Keep
# these runtime-only authority adapters lazy so module initialization retains
# one acyclic path while activation still reuses their canonical decoders.
reserved_capacity = adaptors_common.LazyImport('sky.serve.reserved_capacity')
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')

_MAX_BIGINT = 2**63 - 1
_MAX_COUNT = _MAX_BIGINT
_MAX_BLACKOUT_DETAIL_BYTES = 4096
_AUTHORITY_SCHEMA_VERSION = 3
_ROW_KEY_PREFIX = '$skypilot-pool-observation-v2:'
MAX_OBSERVATION_COHORT_POOLS = 8


class PoolCapacityObservationError(RuntimeError):
    """Base error for the physical-pool observation repository."""


class ObservationProtocolInactiveError(PoolCapacityObservationError):
    """The durable physical-pool broker protocol is not version 2."""


class ObservationLeaseBusyError(PoolCapacityObservationError):
    """The pool has a live writer or is not due for another observation."""

    def __init__(self, pool_key: str, generation: int, lease_expires_at: float):
        super().__init__(
            f'Physical pool {pool_key!r} observation generation {generation} '
            f'is leased until database time {lease_expires_at}.')
        self.pool_key = pool_key
        self.generation = generation
        self.lease_expires_at = lease_expires_at


class StaleObservationWriterError(PoolCapacityObservationError):
    """A lease holder cannot publish over a successor or after expiry."""


class ObservationRepositoryCorruptionError(PoolCapacityObservationError):
    """Durable observation or sequencer state violates its closed shape."""


class ReconciliationGateConflictError(PoolCapacityObservationError):
    """The requested one-way gate transition lost its generation fence."""


class ReconciliationGateState(str, enum.Enum):
    """Closed fleet-wide reconciliation implementation selector."""

    LEGACY_ACTIVE = observation_schema.LEGACY_ACTIVE
    SEQUENCED_ACTIVE = observation_schema.SEQUENCED_ACTIVE


class PoolCapacityBlackoutReason(str, enum.Enum):
    """Closed reasons for a completed measurement blackout."""

    PROVIDER_ERROR = 'PROVIDER_ERROR'
    TIMEOUT = 'TIMEOUT'
    PERMISSION_DENIED = 'PERMISSION_DENIED'
    PHYSICAL_IDENTITY_UNAVAILABLE = 'PHYSICAL_IDENTITY_UNAVAILABLE'
    PHYSICAL_IDENTITY_MISMATCH = 'PHYSICAL_IDENTITY_MISMATCH'
    MALFORMED_RESPONSE = 'MALFORMED_RESPONSE'


def _require_exact_nonnegative_int(value: Any, name: str) -> int:
    if (isinstance(value, bool) or not isinstance(value, int) or value < 0 or
            value > _MAX_COUNT):
        raise ValueError(f'{name} must be a nonnegative 64-bit integer.')
    return value


def _require_positive_finite(value: Any, name: str) -> float:
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not math.isfinite(float(value)) or float(value) <= 0):
        raise ValueError(f'{name} must be a positive finite number.')
    return float(value)


def _bounded_text(value: Any, name: str, maximum_bytes: int) -> str:
    if (not isinstance(value, str) or not value or
            len(value.encode('utf-8')) > maximum_bytes):
        raise ValueError(
            f'{name} must be nonempty and at most {maximum_bytes} bytes.')
    return value


def _canonical_accelerator_names(values: Any) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError('accelerator_names must be a sequence of names.')
    try:
        raw_values = tuple(values)
    except TypeError as exc:
        raise ValueError(
            'accelerator_names must be a sequence of names.') from exc
    if not raw_values or len(raw_values) > 64:
        raise ValueError('accelerator_names must contain between 1 and 64 '
                         'names.')
    normalized: list[str] = []
    for raw_name in raw_values:
        name = _bounded_text(raw_name, 'accelerator name', 256).casefold()
        normalized.append(name)
    if len(set(normalized)) != len(normalized):
        raise ValueError('accelerator_names contains a case-folded duplicate.')
    return tuple(sorted(normalized))


def _parse_physical_pool_key(pool_key: Any) -> tuple[str, tuple[str, ...]]:
    key = _bounded_text(pool_key, 'pool_key', 4096)
    try:
        decoded = json.loads(key)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(
            'pool_key must be a canonical protocol-v2 key.') from exc
    if (not isinstance(decoded, list) or len(decoded) != 3 or
            decoded[0] != 'v2'):
        raise ValueError('pool_key must be a protocol-v2 physical-pool key.')
    physical_cluster_uid = _bounded_text(decoded[1], 'physical_cluster_uid',
                                         1024)
    encoded_names = decoded[2]
    if isinstance(encoded_names, str):
        raw_names = (encoded_names,)
    elif isinstance(encoded_names, list):
        raw_names = tuple(encoded_names)
    else:
        raise ValueError('pool_key has an invalid accelerator set.')
    return physical_cluster_uid, _canonical_accelerator_names(raw_names)


@dataclasses.dataclass(frozen=True)
class PoolCapacitySuccess:
    """Exact aggregate and per-card raw free-GPU measurement.

    Replica width belongs to an authenticated service claim, not to physical
    observation authority.  Persisting raw GPUs lets the broker choose one
    uniform winning width before converting the evidence into launch slots.
    """

    free_gpus: int
    free_gpus_by_accelerator: tuple[tuple[str, int], ...]
    present_accelerator_names: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_exact_nonnegative_int(self.free_gpus, 'free_gpus')
        if not isinstance(self.free_gpus_by_accelerator, tuple):
            raise ValueError('free_gpus_by_accelerator must be immutable.')
        names: list[str] = []
        total = 0
        for item in self.free_gpus_by_accelerator:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError('Each accelerator count must be a pair.')
            raw_name, raw_count = item
            name = _bounded_text(raw_name, 'accelerator name', 256)
            if name != name.casefold():
                raise ValueError('Accelerator names must be case-folded.')
            count = _require_exact_nonnegative_int(raw_count,
                                                   'accelerator free GPUs')
            names.append(name)
            total += count
        if not names:
            raise ValueError('A success payload needs exact-card counts.')
        if tuple(names) != tuple(sorted(names)) or len(
                set(names)) != len(names):
            raise ValueError('Exact-card counts must have unique sorted names.')
        if total != self.free_gpus:
            raise ValueError('Exact-card counts must sum to free_gpus.')
        if (not isinstance(self.present_accelerator_names, tuple) or
                len(self.present_accelerator_names) > 64):
            raise ValueError(
                'present_accelerator_names must be an immutable tuple.')
        present_names: list[str] = []
        for raw_name in self.present_accelerator_names:
            name = _bounded_text(raw_name, 'present accelerator name', 256)
            if name != name.casefold():
                raise ValueError(
                    'Present accelerator names must be case-folded.')
            present_names.append(name)
        present = tuple(present_names)
        if (present != tuple(sorted(present)) or
                len(set(present)) != len(present)):
            raise ValueError(
                'present_accelerator_names must be canonical and immutable.')
        if not set(present).issubset(names):
            raise ValueError('Present accelerators must be part of the exact '
                             'requested card set.')

    @classmethod
    def from_counts(
        cls,
        free_gpus: int,
        free_gpus_by_accelerator: Mapping[str, int],
        *,
        present_accelerator_names: tuple[str, ...] | None = None,
    ) -> 'PoolCapacitySuccess':
        if not isinstance(free_gpus_by_accelerator, Mapping):
            raise ValueError('free_gpus_by_accelerator must be a mapping.')
        normalized: dict[str, int] = {}
        for raw_name, count in free_gpus_by_accelerator.items():
            name = _bounded_text(raw_name, 'accelerator name', 256).casefold()
            if name in normalized:
                raise ValueError('Exact-card counts contain a folded '
                                 'duplicate.')
            normalized[name] = count
        canonical_counts = tuple(sorted(normalized.items()))
        present = (tuple(
            name for name, _ in canonical_counts) if present_accelerator_names
                   is None else present_accelerator_names)
        return cls(free_gpus=free_gpus,
                   free_gpus_by_accelerator=canonical_counts,
                   present_accelerator_names=present)

    def canonical_value(self) -> dict[str, Any]:
        return {
            'kind': 'success',
            'free_gpus': self.free_gpus,
            'free_gpus_by_accelerator': dict(self.free_gpus_by_accelerator),
            'present_accelerator_names': list(self.present_accelerator_names),
        }

    def slot_counts(
        self,
        gpus_per_replica: int,
    ) -> tuple[tuple[str, int], ...]:
        """Convert raw exact-card GPUs once under one authenticated width."""
        if (isinstance(gpus_per_replica, bool) or
                not isinstance(gpus_per_replica, int) or gpus_per_replica <= 0):
            raise ValueError('gpus_per_replica must be a positive integer.')
        return tuple((name, free_gpus // gpus_per_replica)
                     for name, free_gpus in self.free_gpus_by_accelerator)


@dataclasses.dataclass(frozen=True)
class PoolCapacityBlackout:
    """Explicit failure evidence that grants no capacity authority."""

    reason: PoolCapacityBlackoutReason
    detail: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.reason, PoolCapacityBlackoutReason):
            raise ValueError('reason must be a PoolCapacityBlackoutReason.')
        if self.detail is not None:
            if not isinstance(self.detail, str):
                raise ValueError('blackout detail must be text or None.')
            if len(self.detail.encode('utf-8')) > _MAX_BLACKOUT_DETAIL_BYTES:
                raise ValueError('blackout detail exceeds its byte bound.')

    def canonical_value(self) -> dict[str, Any]:
        return {
            'kind': 'blackout',
            'reason': self.reason.value,
            'detail': self.detail,
        }


PoolCapacityPayload = PoolCapacitySuccess | PoolCapacityBlackout


@dataclasses.dataclass(frozen=True)
class ReconciliationGate:
    """Immutable snapshot of the fleet-wide authorized generation."""

    state: ReconciliationGateState
    generation: int
    reclaim_policy_identity: (
        reserved_fill_reclaim_attestation.ReclaimPolicyIdentity | None) = None
    reclaim_activation_receipt: (
        reserved_fill_reclaim_attestation.ReclaimActivationReceipt |
        None) = None
    reclaim_authorized_at: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, ReconciliationGateState):
            raise ValueError('state must be a ReconciliationGateState.')
        _require_exact_nonnegative_int(self.generation, 'generation')
        if self.state == ReconciliationGateState.LEGACY_ACTIVE:
            if (self.reclaim_policy_identity is not None or
                    self.reclaim_activation_receipt is not None or
                    self.reclaim_authorized_at is not None):
                raise ValueError(
                    'A legacy reconciliation gate cannot carry reclaim '
                    'authorization.')
        else:
            if not isinstance(
                    self.reclaim_policy_identity,
                    reserved_fill_reclaim_attestation.ReclaimPolicyIdentity):
                raise ValueError(
                    'A sequenced reconciliation gate requires reclaim policy '
                    'identity.')
            if not isinstance(
                    self.reclaim_activation_receipt,
                    reserved_fill_reclaim_attestation.ReclaimActivationReceipt):
                raise ValueError(
                    'A sequenced reconciliation gate requires an exact '
                    'activation receipt.')
            if (self.reclaim_activation_receipt.identity
                    != self.reclaim_policy_identity):
                raise ValueError('Reclaim receipt identity does not match the '
                                 'sequenced gate identity.')
            if (isinstance(self.reclaim_authorized_at, bool) or
                    not isinstance(self.reclaim_authorized_at, (int, float)) or
                    not math.isfinite(float(self.reclaim_authorized_at)) or
                    self.reclaim_authorized_at < 0):
                raise ValueError(
                    'A sequenced reconciliation gate requires a finite '
                    'authorization timestamp.')

    @property
    def sequenced_active(self) -> bool:
        return self.state == ReconciliationGateState.SEQUENCED_ACTIVE


@dataclasses.dataclass(frozen=True)
class ReconciliationAuthorizationResult:
    """Outcome of one exact activation or active reauthorization."""

    changed: bool
    gate: ReconciliationGate

    def __post_init__(self) -> None:
        if type(self.changed) is not bool:
            raise ValueError('changed must be a boolean.')
        if not isinstance(self.gate, ReconciliationGate):
            raise ValueError('gate must be a ReconciliationGate.')


@dataclasses.dataclass(frozen=True)
class PoolCapacityObservationRequest:
    """Immutable database-facing identity for one observation cohort edge."""

    pool_key: str
    physical_cluster_uid: str
    accelerator_names: tuple[str, ...]
    access_contexts: tuple[str, ...]

    def __post_init__(self) -> None:
        key_uid, key_names = _parse_physical_pool_key(self.pool_key)
        uid = _bounded_text(self.physical_cluster_uid, 'physical_cluster_uid',
                            1024)
        names = _canonical_accelerator_names(self.accelerator_names)
        if (not isinstance(self.access_contexts, tuple) or
                not self.access_contexts or len(self.access_contexts) > 8 or
                len(set(self.access_contexts)) != len(self.access_contexts)):
            raise ValueError('Observation request access_contexts must contain '
                             'between 1 and 8 unique routes.')
        for context in self.access_contexts:
            _bounded_text(context, 'access_context', 1024)
        if key_uid != uid or key_names != names:
            raise ValueError('Observation request identity does not match '
                             'pool_key.')
        object.__setattr__(self, 'accelerator_names', names)

    @property
    def access_context(self) -> str:
        """Return the bootstrap route persisted on the in-progress row."""
        return self.access_contexts[0]


@dataclasses.dataclass(frozen=True)
class PoolCapacityObservationLease:
    """Immutable ownership token returned by cohort admission."""

    row_key: str
    pool_key: str
    physical_cluster_uid: str
    accelerator_names: tuple[str, ...]
    access_context: str
    access_contexts: tuple[str, ...]
    observation_generation: int
    lease_token: uuid.UUID
    lease_expires_at: float
    observation_sequence: int
    ordinary_admission_sequence: int
    materialization_sequence: int
    observed_at: float
    valid_until: float


@dataclasses.dataclass(frozen=True)
class PoolCapacityObservation:
    """One immutable, digest-verified completed observation generation.

    ``access_context`` records which Kubernetes alias acquired the evidence.
    It is authenticated provenance, but it is deliberately not part of the
    protocol-v2 physical-pool identity.  Consumers authorize placement through
    their own context-to-physical-UID claim edge.
    """

    pool_key: str
    physical_cluster_uid: str
    accelerator_names: tuple[str, ...]
    access_context: str
    observation_generation: int
    lease_token: uuid.UUID
    lease_expires_at: float
    observation_sequence: int
    ordinary_admission_sequence: int
    materialization_sequence: int
    payload: PoolCapacityPayload
    payload_sha256: str
    observed_at: float
    completed_at: float
    valid_until: float
    published_at: float

    def is_authoritative_at(self, database_time: float) -> bool:
        """Whether this success is usable at the supplied database time."""
        if (isinstance(database_time, bool) or
                not isinstance(database_time, (int, float)) or
                not math.isfinite(float(database_time))):
            raise ValueError('database_time must be finite.')
        return (isinstance(self.payload, PoolCapacitySuccess) and
                self.published_at <= float(database_time) <= self.valid_until)


def _row_key(pool_key: str, generation: int) -> str:
    pool_digest = hashlib.sha256(pool_key.encode('utf-8')).hexdigest()
    return f'{_ROW_KEY_PREFIX}{pool_digest}:{generation}'


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _authority_material(*, row_key: str, pool_key: str,
                        physical_cluster_uid: str,
                        accelerator_names: tuple[str, ...], access_context: str,
                        observation_generation: int, lease_token: uuid.UUID,
                        lease_expires_at: float, observation_sequence: int,
                        ordinary_admission_sequence: int,
                        materialization_sequence: int,
                        payload: PoolCapacityPayload, observed_at: float,
                        completed_at: float, valid_until: float,
                        published_at: float) -> dict[str, Any]:
    return {
        'schema_version': _AUTHORITY_SCHEMA_VERSION,
        'legacy_projection': {
            'context': row_key,
            'snapshot_time': observed_at,
            'completed_at': completed_at,
            'availability': None,
        },
        'identity': {
            'pool_key': pool_key,
            'physical_cluster_uid': physical_cluster_uid,
            'accelerator_names': list(accelerator_names),
            'access_context': access_context,
            'observation_generation': observation_generation,
        },
        'lease': {
            'lease_token': str(lease_token),
            'lease_expires_at': lease_expires_at,
            'observation_sequence': observation_sequence,
            'ordinary_admission_sequence': ordinary_admission_sequence,
            'materialization_sequence': materialization_sequence,
        },
        'status': ('SUCCESS'
                   if isinstance(payload, PoolCapacitySuccess) else 'BLACKOUT'),
        'payload': payload.canonical_value(),
        'timestamps': {
            'observed_at': observed_at,
            'completed_at': completed_at,
            'valid_until': valid_until,
            'published_at': published_at,
        },
    }


def _authority_sha256(**kwargs: Any) -> str:
    material = _authority_material(**kwargs)
    return hashlib.sha256(_canonical_json_bytes(material)).hexdigest()


def _decode_payload(raw_payload: Any,
                    status: Any) -> PoolCapacityPayload | None:
    if not isinstance(raw_payload, dict):
        return None
    if status == observation_schema.SUCCESS:
        if set(raw_payload) != {
                'kind', 'free_gpus', 'free_gpus_by_accelerator',
                'present_accelerator_names'
        } or raw_payload.get('kind') != 'success':
            return None
        counts = raw_payload.get('free_gpus_by_accelerator')
        present = raw_payload.get('present_accelerator_names')
        if not isinstance(counts, dict) or type(present) is not list:
            return None
        try:
            return PoolCapacitySuccess.from_counts(
                raw_payload['free_gpus'],
                counts,
                present_accelerator_names=(tuple(present)))
        except (KeyError, TypeError, ValueError):
            return None
    if status == observation_schema.BLACKOUT:
        if set(raw_payload) != {'kind', 'reason', 'detail'
                               } or raw_payload.get('kind') != 'blackout':
            return None
        try:
            reason = PoolCapacityBlackoutReason(raw_payload['reason'])
            return PoolCapacityBlackout(reason=reason,
                                        detail=raw_payload['detail'])
        except (KeyError, TypeError, ValueError):
            return None
    return None


def _finite_float(value: Any) -> float | None:
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not math.isfinite(float(value))):
        return None
    return float(value)


def _decode_reconciliation_gate(row: RowMapping) -> ReconciliationGate:
    try:
        state = ReconciliationGateState(row['reconciliation_gate_state'])
    except (KeyError, TypeError, ValueError) as exc:
        raise ObservationRepositoryCorruptionError(
            'Reserved-fill reconciliation gate state is malformed.') from exc
    generation = row.get('reconciliation_gate_generation')
    if (isinstance(generation, bool) or not isinstance(generation, int) or
            generation < 0 or generation > _MAX_BIGINT):
        raise ObservationRepositoryCorruptionError(
            'Reserved-fill reconciliation gate generation is malformed.')
    if state == ReconciliationGateState.LEGACY_ACTIVE:
        reclaim_columns = (
            row.get('reclaim_fleet_bundle_sha256'),
            row.get('reclaim_policy_revision'),
            row.get('reclaim_provider_inventory_sha256'),
            row.get('reclaim_claim_scope_count'),
            row.get('reclaim_claim_scope_sha256'),
            row.get('reclaim_evidence_sha256'),
            row.get('reclaim_authorized_at'),
        )
        if any(value is not None for value in reclaim_columns):
            raise ObservationRepositoryCorruptionError(
                'Legacy reconciliation gate carries reclaim authorization.')
        return ReconciliationGate(state=state, generation=generation)
    try:
        receipt = _reclaim_authorization_receipt(row)
        return ReconciliationGate(
            state=state,
            generation=generation,
            reclaim_policy_identity=receipt.identity,
            reclaim_activation_receipt=receipt,
            reclaim_authorized_at=row.get('reclaim_authorized_at'))
    except (ObservationRepositoryCorruptionError, TypeError, ValueError) as exc:
        raise ObservationRepositoryCorruptionError(
            'Sequenced reconciliation gate reclaim authorization is '
            'malformed.') from exc


def _reclaim_authorization_receipt(
    row: RowMapping,
) -> reserved_fill_reclaim_attestation.ReclaimActivationReceipt:
    """Decode the exact writer and reclaim receipt stored in one row."""
    try:
        return reserved_fill_reclaim_attestation.ReclaimActivationReceipt(
            identity=reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
                fleet_bundle_sha256=row['reclaim_fleet_bundle_sha256'],
                policy_revision=row['reclaim_policy_revision'],
                provider_inventory_sha256=(
                    row['reclaim_provider_inventory_sha256'])),
            claim_scope_count=row['reclaim_claim_scope_count'],
            claim_scope_sha256=row['reclaim_claim_scope_sha256'],
            evidence_sha256=row['reclaim_evidence_sha256'],
            writer_image_digest=row['image_digest'],
            writer_deployment_generation=row['deployment_generation'],
            writer_deployment_uid=row['deployment_uid'],
            writer_pod_inventory_count=row['pod_inventory_count'],
            writer_pod_inventory_sha256=row['pod_inventory_sha256'])
    except (KeyError, TypeError, ValueError) as error:
        raise ObservationRepositoryCorruptionError(
            'Persisted reclaim authorization receipt is malformed.') from error


def _decode_completed_row(
        row: Mapping[str, Any]) -> PoolCapacityObservation | None:
    """Fail-closed decode of one completed authority row."""
    try:
        pool_key = row['pool_key']
        physical_cluster_uid = row['physical_cluster_uid']
        accelerator_names = _canonical_accelerator_names(
            row['accelerator_names'])
        key_uid, key_names = _parse_physical_pool_key(pool_key)
        if key_uid != physical_cluster_uid or key_names != accelerator_names:
            return None
        access_context = _bounded_text(row['access_context'], 'access_context',
                                       1024)
        generation = row['observation_generation']
        sequence = row['observation_sequence']
        ordinary_sequence = row['ordinary_admission_sequence']
        materialization_sequence = row['materialization_sequence']
        if (isinstance(generation, bool) or not isinstance(generation, int) or
                generation <= 0 or generation > _MAX_BIGINT or
                isinstance(sequence, bool) or not isinstance(sequence, int) or
                sequence < 0 or sequence > _MAX_BIGINT or
                isinstance(ordinary_sequence, bool) or
                not isinstance(ordinary_sequence, int) or
                ordinary_sequence < 0 or ordinary_sequence > sequence or
                isinstance(materialization_sequence, bool) or
                not isinstance(materialization_sequence, int) or
                materialization_sequence < 0 or
                materialization_sequence > _MAX_BIGINT):
            return None
        expected_row_key = _row_key(pool_key, generation)
        if row['context'] != expected_row_key:
            return None
        lease_token = row['lease_token']
        if isinstance(lease_token, str):
            lease_token = uuid.UUID(lease_token)
        if not isinstance(lease_token, uuid.UUID):
            return None
        observed_at = _finite_float(row['observed_at'])
        completed_at = _finite_float(row['completed_at'])
        valid_until = _finite_float(row['valid_until'])
        published_at = _finite_float(row['published_at'])
        lease_expires_at = _finite_float(row['lease_expires_at'])
        snapshot_time = _finite_float(row['snapshot_time'])
        if None in (observed_at, completed_at, valid_until, published_at,
                    lease_expires_at, snapshot_time):
            return None
        assert observed_at is not None
        assert completed_at is not None
        assert valid_until is not None
        assert published_at is not None
        assert lease_expires_at is not None
        assert snapshot_time is not None
        if (row['availability'] is not None or snapshot_time != observed_at or
                lease_expires_at < observed_at or
                not observed_at <= completed_at <= published_at or
                valid_until <= observed_at):
            return None
        payload = _decode_payload(row['payload'], row['observation_status'])
        if payload is None:
            return None
        if isinstance(payload, PoolCapacitySuccess):
            payload_names = tuple(
                name for name, _ in payload.free_gpus_by_accelerator)
            if payload_names != accelerator_names:
                return None
        payload_sha256 = row['payload_sha256']
        if (not isinstance(payload_sha256, str) or len(payload_sha256) != 64 or
                payload_sha256 != payload_sha256.lower()):
            return None
        expected_sha256 = _authority_sha256(
            row_key=expected_row_key,
            pool_key=pool_key,
            physical_cluster_uid=physical_cluster_uid,
            accelerator_names=accelerator_names,
            access_context=access_context,
            observation_generation=generation,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            observation_sequence=sequence,
            ordinary_admission_sequence=ordinary_sequence,
            materialization_sequence=materialization_sequence,
            payload=payload,
            observed_at=observed_at,
            completed_at=completed_at,
            valid_until=valid_until,
            published_at=published_at,
        )
        if not hmac.compare_digest(expected_sha256, payload_sha256):
            return None
        return PoolCapacityObservation(
            pool_key=pool_key,
            physical_cluster_uid=physical_cluster_uid,
            accelerator_names=accelerator_names,
            access_context=access_context,
            observation_generation=generation,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            observation_sequence=sequence,
            ordinary_admission_sequence=ordinary_sequence,
            materialization_sequence=materialization_sequence,
            payload=payload,
            payload_sha256=payload_sha256,
            observed_at=observed_at,
            completed_at=completed_at,
            valid_until=valid_until,
            published_at=published_at,
        )
    except (KeyError, TypeError, ValueError):
        return None


def decode_completed_observation(
    row: Mapping[str, Any],) -> PoolCapacityObservation | None:
    """Decode one completed row at an authority-use boundary.

    This is the shared fail-closed decoder for repository reads and atomic
    broker publication.  Callers must additionally check freshness at the
    database time sampled inside their transaction before spending a success.
    """
    if not isinstance(row, Mapping):
        raise ValueError('Observation row must be a mapping.')
    return _decode_completed_row(row)


class PoolCapacityObservationRepository:
    """PostgreSQL-only begin/complete/read boundary for capacity evidence."""

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine | None = None,
        *,
        token_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._engine = (serve_state_schema.get_database_engine()
                        if engine is None else engine)
        if self._engine.dialect.name != 'postgresql':
            raise ValueError(
                'PoolCapacityObservationRepository is PostgreSQL-only.')
        self._token_factory = token_factory

    @staticmethod
    def _database_now(connection: sqlalchemy.engine.Connection) -> float:
        value = connection.execute(
            sqlalchemy.text(
                'SELECT EXTRACT(EPOCH FROM '
                'clock_timestamp())::double precision')).scalar_one()
        return float(value)

    @staticmethod
    def _lock_protocol(connection: sqlalchemy.engine.Connection,) -> RowMapping:
        table = observation_schema.protocol_state_sequence_table
        row = connection.execute(
            sqlalchemy.select(table).where(
                table.c.id == 1).with_for_update()).mappings().one_or_none()
        if row is None:
            raise ObservationRepositoryCorruptionError(
                'Reserved-fill protocol singleton is absent.')
        if row['protocol_version'] != 2:
            raise ObservationProtocolInactiveError(
                'Physical-pool observations require reserved-fill protocol '
                'version 2.')
        sequence = row['zero_cost_admission_sequence']
        ordinary_sequence = row['ordinary_zero_cost_admission_sequence']
        materialization_sequence = row['zero_cost_materialization_sequence']
        if (isinstance(sequence, bool) or not isinstance(sequence, int) or
                sequence < 0 or sequence > _MAX_BIGINT or
                isinstance(ordinary_sequence, bool) or
                not isinstance(ordinary_sequence, int) or
                ordinary_sequence < 0 or ordinary_sequence > sequence or
                isinstance(materialization_sequence, bool) or
                not isinstance(materialization_sequence, int) or
                materialization_sequence < 0 or
                materialization_sequence > _MAX_BIGINT):
            raise ObservationRepositoryCorruptionError(
                'Reserved-fill admission sequences are malformed.')
        _decode_reconciliation_gate(row)
        return row

    def read_reconciliation_gate(self) -> ReconciliationGate:
        """Read the explicit fleet-wide path selector without changing it."""
        table = observation_schema.protocol_state_sequence_table
        with self._engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(table).where(
                    table.c.id == 1)).mappings().one_or_none()
        if row is None:
            raise ObservationRepositoryCorruptionError(
                'Reserved-fill protocol singleton is absent.')
        return _decode_reconciliation_gate(row)

    def lock_reconciliation_gate_for_activation(
        self,
        connection: sqlalchemy.engine.Connection,
    ) -> ReconciliationGate:
        """Lock and read the gate in the caller's activation transaction."""
        if connection.engine is not self._engine:
            raise ValueError('Activation connection belongs to another '
                             'PostgreSQL engine.')
        return _decode_reconciliation_gate(self._lock_protocol(connection))

    @staticmethod
    def _activation_claim_scope_in_connection(
        connection: sqlalchemy.engine.Connection,
        *,
        lock: bool,
    ) -> tuple[reserved_fill_reclaim_attestation.ReservedContextClaim, ...]:
        """Reconstruct every selectable edge from its current version."""
        if lock:
            connection.execute(
                sqlalchemy.text(
                    'LOCK TABLE reserved_fill_service_claim_sets, '
                    'reserved_fill_pool_claims IN SHARE ROW EXCLUSIVE MODE'))
        protocol_table = serve_state_schema.reserved_fill_protocol_state_table
        claim_generation = connection.execute(
            sqlalchemy.select(protocol_table.c.claim_generation).where(
                protocol_table.c.id == 1)).scalar_one()
        if (type(claim_generation) is not int or claim_generation < 0 or
                claim_generation > _MAX_BIGINT):
            raise ObservationRepositoryCorruptionError(
                'Reserved-fill claim generation is malformed.')
        set_table = serve_state_schema.reserved_fill_service_claim_sets_table
        edge_table = serve_state_schema.reserved_fill_pool_claims_table
        set_rows = connection.execute(
            sqlalchemy.select(set_table).where(
                set_table.c.claim_set_state ==
                'authoritative_v2')).mappings().all()
        service_names = tuple(
            sorted(str(row['service_name']) for row in set_rows))
        service_rows: dict[str, RowMapping] = {}
        if service_names:
            service_query = sqlalchemy.select(
                serve_state_schema.services_table.c.name,
                serve_state_schema.services_table.c.current_version).where(
                    serve_state_schema.services_table.c.name.in_(service_names))
            if lock:
                service_query = service_query.with_for_update(read=True)
            service_rows = {
                str(row['name']): row
                for row in connection.execute(service_query).mappings().all()
            }
        edge_rows: list[RowMapping] = []
        if service_names:
            edge_rows = list(
                connection.execute(
                    sqlalchemy.select(edge_table).where(
                        edge_table.c.service_name.in_(
                            service_names))).mappings().all())
        edges_by_service: dict[str, list[RowMapping]] = {}
        for edge in edge_rows:
            edges_by_service.setdefault(str(edge['service_name']),
                                        []).append(edge)
        claims = []
        try:
            for claim_set in set_rows:
                service_name = str(claim_set['service_name'])
                service_generation = claim_set['generation']
                service_row = service_rows.get(service_name)
                if service_row is None:
                    raise ValueError('claim set has no service owner')
                service_version = service_row['current_version']
                persisted_version = claim_set['service_version']
                if (type(service_version) is not int or service_version <= 0 or
                        persisted_version is not None and
                        persisted_version != service_version):
                    raise ValueError('claim set has no exact current version')
                version_query = sqlalchemy.select(
                    serve_state_schema.version_specs_table.c.
                    worker_placement_projections).where(
                        serve_state_schema.version_specs_table.c.service_name ==
                        service_name,
                        serve_state_schema.version_specs_table.c.version ==
                        service_version,
                        serve_state_schema.version_specs_table.c.yaml_content.
                        isnot(None),
                        serve_state_schema.version_specs_table.c.quarantined_at.
                        is_(None),
                        serve_state_schema.version_specs_table.c.retired_at.is_(
                            None))
                if lock:
                    version_query = version_query.with_for_update(read=True)
                version_row = connection.execute(
                    version_query).mappings().one_or_none()
                if version_row is None:
                    raise ValueError('claim set version is not committed')
                edges = edges_by_service.get(service_name, [])
                if (type(service_generation) is not int or
                        service_generation <= 0 or
                        service_generation > claim_generation or
                        type(claim_set['edge_count']) is not int or
                        claim_set['edge_count'] <= 0 or
                        claim_set['edge_count'] != len(edges)):
                    raise ValueError('incomplete authoritative claim set')
                for edge in edges:
                    if edge['service_generation'] != service_generation:
                        raise ValueError('mixed claim-set generations')
                    raw_names = json.loads(edge['accelerator_names'])
                    names = _canonical_accelerator_names(raw_names)
                    pool_uid, pool_names = _parse_physical_pool_key(
                        edge['pool_key'])
                    if (pool_uid != edge['physical_cluster_uid'] or
                            pool_names != names):
                        raise ValueError('claim edge identity mismatch')
                    accelerator_count = edge['gpus_per_replica']
                    if (type(accelerator_count) is not int or
                            accelerator_count <= 0):
                        raise ValueError('claim edge width is malformed')
                    projected_admissions = (
                        reserved_fill_projection_authority.
                        projected_admissions_for_edge(
                            version_row['worker_placement_projections'],
                            access_context=edge['access_context'],
                            accelerator_names=names,
                            accelerator_count=accelerator_count,
                            require_current_protocol=True))
                    expected_projection_map = (
                        reserved_fill_projection_authority.
                        projection_sha256_by_accelerator(projected_admissions))
                    raw_projection_map = edge[
                        'worker_projection_sha256_by_accelerator']
                    if isinstance(raw_projection_map, str):
                        raw_projection_map = json.loads(raw_projection_map)
                    # Legacy-active rows have no persisted version/map.  The
                    # activation proof derives them from the locked current
                    # version; sequenced rows must already carry the exact map.
                    if ((persisted_version is None) != (raw_projection_map
                                                        is None) or
                            raw_projection_map is not None and
                            raw_projection_map != expected_projection_map):
                        raise ValueError('claim projection map is stale')
                    claims.append(
                        reserved_fill_reclaim_attestation.ReservedContextClaim(
                            service_name=service_name,
                            service_version=service_version,
                            service_generation=service_generation,
                            pool_key=edge['pool_key'],
                            access_context=edge['access_context'],
                            physical_cluster_uid=edge['physical_cluster_uid'],
                            accelerator_names=names,
                            projected_admissions=projected_admissions))
            result = tuple(sorted(claims))
            if len(set(result)) != len(result):
                raise ValueError('duplicate authoritative claim edge')
            return result
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ObservationRepositoryCorruptionError(
                'The authoritative reclaim claim scope is malformed.'
            ) from error

    def read_activation_claim_scope(
        self,
    ) -> tuple[reserved_fill_reclaim_attestation.ReservedContextClaim, ...]:
        """Read the exact scope later predicate-locked by activation CAS."""
        with self._engine.connect() as connection:
            return self._activation_claim_scope_in_connection(connection,
                                                              lock=False)

    @classmethod
    def _lock_activation_claim_scope(
        cls,
        connection: sqlalchemy.engine.Connection,
    ) -> tuple[reserved_fill_reclaim_attestation.ReservedContextClaim, ...]:
        """Predicate-lock and reconstruct every selectable claim edge."""
        return cls._activation_claim_scope_in_connection(connection, lock=True)

    @staticmethod
    def _require_initial_activation_queue_drained(
        connection: sqlalchemy.engine.Connection,
        *,
        successor_gate_generation: int,
        reclaim_policy_identity: (
            reserved_fill_reclaim_attestation.ReclaimPolicyIdentity),
        claim_scope: tuple[
            reserved_fill_reclaim_attestation.ReservedContextClaim, ...],
    ) -> None:
        """Lock and reject queued effects not bound to exact v2 authority."""
        replica_table = serve_state_schema.replicas_table
        rows = connection.execute(
            sqlalchemy.select(
                replica_table.c.service_name, replica_table.c.replica_id,
                replica_table.c.replica_state_version,
                replica_table.c.replica_state, replica_table.c.status,
                replica_table.c.sky_down_status, replica_table.c.version).where(
                    replica_table.c.status.in_((
                        'PENDING',
                        'PROVISIONING',
                    ))).with_for_update()).mappings()
        blockers: list[tuple[str, int]] = []
        expected_tuple = (
            successor_gate_generation,
            reclaim_policy_identity.fleet_bundle_sha256,
            reclaim_policy_identity.policy_revision,
            reclaim_policy_identity.provider_inventory_sha256,
        )
        for row in rows:
            blocker = (str(row['service_name']), int(row['replica_id']))
            state = row['replica_state']
            if not isinstance(state, dict):
                blockers.append(blocker)
                continue
            try:
                info = serve_state.decode_replica_state_for_authority(
                    row['replica_state_version'], state)
                scalar_sky_down_status = (
                    None if info.status_property.sky_down_status is None else
                    info.status_property.sky_down_status.value)
                if (info.replica_id != row['replica_id'] or
                        info.version != row['version'] or
                        info.status.value != row['status'] or
                        scalar_sky_down_status != row['sky_down_status']):
                    raise ValueError('Queued replica scalar columns disagree.')
                if info.reserved_fill is not True:
                    continue
                # A legacy decoder can materialize new in-memory defaults.  It
                # cannot turn an old durable payload into successor authority.
                if info._version != info._VERSION:  # pylint: disable=protected-access
                    raise ValueError('Queued fill record is not current.')

                location = info.get_spot_location()
                accelerators = (None
                                if location is None else location.accelerators)
                if not isinstance(accelerators,
                                  Mapping) or len(accelerators) != 1:
                    raise ValueError('Queued fill has no exact accelerator.')
                accelerator, accelerator_count = next(iter(
                    accelerators.items()))
                durable_context = reserved_capacity.make_protocol_v2_launch_fence(
                    pool_key=info.reserved_fill_pool_key,
                    service_generation=info.reserved_fill_service_generation,
                    service_version=info.version,
                    physical_cluster_uid=(
                        info.reserved_fill_physical_cluster_uid),
                    kubernetes_context=info.reserved_fill_kubernetes_context,
                    accelerator=accelerator,
                    accelerator_count=accelerator_count,
                    reconciliation_gate_generation=(
                        info.reserved_fill_reconciliation_gate_generation),
                    reclaim_fleet_bundle_sha256=(
                        info.reserved_fill_reclaim_fleet_bundle_sha256),
                    reclaim_policy_revision=(
                        info.reserved_fill_reclaim_policy_revision),
                    reclaim_provider_inventory_sha256=(
                        info.reserved_fill_reclaim_provider_inventory_sha256),
                    worker_projection_sha256=(
                        info.reserved_fill_worker_projection_sha256))
                fence = reserved_capacity.parse_protocol_v2_launch_fence(
                    durable_context)
                if fence is None or not fence.policy_bound:
                    raise ValueError('Queued fill has no successor fence.')
                reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
                    fence, info)
                policy_tuple = (
                    fence.reconciliation_gate_generation,
                    fence.reclaim_fleet_bundle_sha256,
                    fence.reclaim_policy_revision,
                    fence.reclaim_provider_inventory_sha256,
                )
                if policy_tuple != expected_tuple or info.is_zero_cost is not True:
                    raise ValueError('Queued fill has stale successor policy.')

                matching_admissions = [
                    admission for claim in claim_scope
                    if (claim.service_name == row['service_name'] and
                        claim.service_version == fence.service_version and
                        claim.service_generation == fence.service_generation and
                        claim.pool_key == fence.pool_key and
                        claim.access_context == fence.kubernetes_context and
                        claim.physical_cluster_uid == fence.physical_cluster_uid
                        and info.reserved_fill_allocation_claim_generation ==
                        claim.service_generation)
                    for admission in claim.projected_admissions
                    if (admission.kubernetes_context == fence.kubernetes_context
                        and admission.accelerator == fence.accelerator and
                        admission.accelerator_count == fence.accelerator_count
                        and admission.worker_projection_sha256 ==
                        fence.worker_projection_sha256)
                ]
                if len(matching_admissions) != 1:
                    raise ValueError('Queued fill has no exact locked claim.')
            except Exception:  # pylint: disable=broad-except
                # Replica JSON and cross-table authority are durable inputs.
                # Any decode or proof failure must block activation without
                # exposing the raw payload in an operator-visible error.
                blockers.append(blocker)
        if blockers:
            raise ReconciliationGateConflictError(
                'Sequenced activation requires queued reserved-fill effects '
                'to drain or carry the exact current successor launch tuple; '
                'blocked '
                f'rows: {blockers!r}.')

    def read_ordinary_admission_sequence(self) -> int:
        """Return the current ordinary zero-cost admission high-water."""
        table = observation_schema.protocol_state_sequence_table
        with self._engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(table).where(
                    table.c.id == 1)).mappings().one_or_none()
        if row is None:
            raise ObservationRepositoryCorruptionError(
                'Reserved-fill protocol singleton is absent.')
        ordinary_sequence = row['ordinary_zero_cost_admission_sequence']
        global_sequence = row['zero_cost_admission_sequence']
        if (type(ordinary_sequence) is not int or ordinary_sequence < 0 or
                type(global_sequence) is not int or
                global_sequence < ordinary_sequence):
            raise ObservationRepositoryCorruptionError(
                'Reserved-fill admission sequences are malformed.')
        return ordinary_sequence

    def authorize_sequenced_reconciliation(
        self,
        *,
        expected_generation: int,
        receipt: reserved_fill_reclaim_attestation.ReclaimActivationReceipt,
        connection: sqlalchemy.engine.Connection | None = None,
    ) -> ReconciliationAuthorizationResult:
        """Authorize one exact receipt through the canonical generation CAS.

        With ``connection``, the caller owns the transaction and can compose
        the CAS with the same-session fleet advisory locks. Without it, this
        method owns the transaction for repository tests and diagnostics.
        """
        generation = _require_exact_nonnegative_int(expected_generation,
                                                    'expected_generation')
        if not isinstance(
                receipt,
                reserved_fill_reclaim_attestation.ReclaimActivationReceipt):
            raise ValueError('Authorization requires ReclaimActivationReceipt.')
        if connection is not None:
            if connection.engine is not self._engine:
                raise ValueError('Activation connection belongs to another '
                                 'PostgreSQL engine.')
            return self._authorize_sequenced_reconciliation_in_connection(
                connection, generation, receipt)
        with self._engine.begin() as owned_connection:
            return self._authorize_sequenced_reconciliation_in_connection(
                owned_connection, generation, receipt)

    def _authorize_sequenced_reconciliation_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        generation: int,
        receipt: reserved_fill_reclaim_attestation.ReclaimActivationReceipt,
    ) -> ReconciliationAuthorizationResult:
        """Shared authorization implementation on an open transaction."""
        table = observation_schema.protocol_state_sequence_table
        row = self._lock_protocol(connection)
        current = _decode_reconciliation_gate(row)

        claim_scope = self._lock_activation_claim_scope(connection)
        scope_count, scope_sha256 = (reserved_fill_reclaim_attestation.
                                     claim_scope_projection(claim_scope))
        if (scope_count != receipt.claim_scope_count or not hmac.compare_digest(
                scope_sha256, receipt.claim_scope_sha256)):
            raise ReconciliationGateConflictError(
                'Reserved-context claims changed after reclaim attestation.')

        if (current.sequenced_active and
                current.reclaim_activation_receipt == receipt):
            if generation not in (current.generation, current.generation - 1):
                raise ReconciliationGateConflictError(
                    'Exact reclaim receipt replay has an unrelated expected '
                    f'generation {generation}; current generation is '
                    f'{current.generation}.')
            return ReconciliationAuthorizationResult(False, current)
        if current.state not in (
                ReconciliationGateState.LEGACY_ACTIVE,
                ReconciliationGateState.SEQUENCED_ACTIVE,
        ):
            raise ReconciliationGateConflictError(
                'Reconciliation gate is not authorizable.')
        if current.generation != generation:
            raise ReconciliationGateConflictError(
                'Reconciliation gate generation changed from expected '
                f'{generation} to {current.generation}.')
        if generation >= _MAX_BIGINT:
            raise ObservationRepositoryCorruptionError(
                'Reconciliation gate generation is exhausted.')

        if not current.sequenced_active:
            self._require_initial_activation_queue_drained(
                connection,
                successor_gate_generation=generation + 1,
                reclaim_policy_identity=receipt.identity,
                claim_scope=claim_scope)
        else:
            # A policy or writer rotation invalidates every planner map. Old
            # durable rows remain conservative occupancy, while their carried
            # gate generation can no longer authorize a provider effect.
            allocation_table = (
                observation_schema.reserved_fill_service_allocation_table)
            connection.execute(
                sqlalchemy.update(allocation_table).values(
                    allocation_generation=0,
                    allocation_input_sha256=None,
                    allocation_claim_generation=None,
                    allocation_map=None,
                    allocation_published_at=None,
                    allocation_gate_generation=None))

        authorized_at = self._database_now(connection)
        successor = ReconciliationGate(
            state=ReconciliationGateState.SEQUENCED_ACTIVE,
            generation=generation + 1,
            reclaim_policy_identity=receipt.identity,
            reclaim_activation_receipt=receipt,
            reclaim_authorized_at=authorized_at)
        update = connection.execute(
            sqlalchemy.update(table).where(
                table.c.id == 1,
                table.c.reconciliation_gate_state == current.state.value,
                table.c.reconciliation_gate_generation == generation,
            ).values(
                reconciliation_gate_state=successor.state.value,
                reconciliation_gate_generation=successor.generation,
                reclaim_fleet_bundle_sha256=(
                    receipt.identity.fleet_bundle_sha256),
                reclaim_policy_revision=receipt.identity.policy_revision,
                reclaim_provider_inventory_sha256=(
                    receipt.identity.provider_inventory_sha256),
                reclaim_claim_scope_count=receipt.claim_scope_count,
                reclaim_claim_scope_sha256=receipt.claim_scope_sha256,
                reclaim_evidence_sha256=receipt.evidence_sha256,
                reclaim_authorized_at=authorized_at,
                image_digest=receipt.writer_image_digest,
                deployment_generation=receipt.writer_deployment_generation,
                deployment_uid=receipt.writer_deployment_uid,
                pod_inventory_count=receipt.writer_pod_inventory_count,
                pod_inventory_sha256=receipt.writer_pod_inventory_sha256,
            ))
        if update.rowcount != 1:
            raise ReconciliationGateConflictError(
                'Reconciliation gate lost its conditional authorization.')
        return ReconciliationAuthorizationResult(True, successor)

    @staticmethod
    def _latest_row(
        connection: sqlalchemy.engine.Connection,
        pool_key: str,
        *,
        for_update: bool,
    ) -> RowMapping | None:
        table = observation_schema.demand_capacity_observations_v2_table
        query = (sqlalchemy.select(table).where(
            table.c.pool_key == pool_key).order_by(
                table.c.observation_generation.desc()).limit(1))
        if for_update:
            query = query.with_for_update()
        return connection.execute(query).mappings().one_or_none()

    def begin_observation(
        self,
        *,
        pool_key: str,
        physical_cluster_uid: str,
        accelerator_names: tuple[str, ...],
        access_context: str,
        lease_duration_seconds: float,
        authority_horizon_seconds: float = 180.0,
        minimum_refresh_interval_seconds: float = 0.0,
    ) -> PoolCapacityObservationLease:
        """Acquire one observation through the canonical cohort boundary."""
        request = PoolCapacityObservationRequest(
            pool_key=pool_key,
            physical_cluster_uid=physical_cluster_uid,
            accelerator_names=accelerator_names,
            access_contexts=(access_context,))
        leases = self.begin_observations(
            (request,),
            lease_duration_seconds=lease_duration_seconds,
            authority_horizon_seconds=authority_horizon_seconds,
            minimum_refresh_interval_seconds=(minimum_refresh_interval_seconds))
        if leases:
            return leases[0]
        # Preserve the single-pool API's typed contention result while routing
        # all admission through the cohort implementation. The row is only
        # diagnostic; authority remains the empty lease result above.
        with self._engine.connect() as connection:
            latest = self._latest_row(connection, pool_key, for_update=False)
        if latest is None:
            raise ObservationRepositoryCorruptionError(
                'Skipped observation has no latest pool generation.')
        generation = latest['observation_generation']
        if latest['observation_status'] == observation_schema.IN_PROGRESS:
            unavailable_until = _finite_float(latest['lease_expires_at'])
        else:
            observed_at = _finite_float(latest['observed_at'])
            unavailable_until = (None if observed_at is None else observed_at +
                                 float(minimum_refresh_interval_seconds))
        if (type(generation) is not int or generation <= 0 or
                unavailable_until is None):
            raise ObservationRepositoryCorruptionError(
                'Skipped observation authority is malformed.')
        raise ObservationLeaseBusyError(pool_key, generation, unavailable_until)

    def begin_observations(
        self,
        requests: tuple[PoolCapacityObservationRequest, ...],
        *,
        lease_duration_seconds: float,
        authority_horizon_seconds: float = 180.0,
        minimum_refresh_interval_seconds: float = 0.0,
    ) -> tuple[PoolCapacityObservationLease, ...]:
        """Atomically acquire a coherent independently available cohort.

        Every pool in a service-wide allocation map must observe the same
        ordinary-admission prefix.  Locking the sequencer once and admitting
        every due, unleased pool in one transaction makes that prefix
        attainable even when multiple controllers share individual pools.
        Busy/not-due pools are skipped independently and retain their prior
        completed evidence; healthy siblings cannot be starved by a shared
        pool whose fixed-rate observer consistently wins first.

        ``observation_sequence`` is the current all-zero-cost admission
        high-water, ``ordinary_admission_sequence`` is the ordinary-demand
        high-water, and ``materialization_sequence`` is the first-successful-
        launch high-water at query start. Starting a query advances none of
        these event counters.
        """
        if (type(requests) is not tuple or not requests or
                any(not isinstance(request, PoolCapacityObservationRequest)
                    for request in requests)):
            raise ValueError('Observation requests must be a nonempty '
                             'immutable request tuple.')
        if len(requests) > MAX_OBSERVATION_COHORT_POOLS:
            raise ValueError('Observation cohort exceeds the bounded pool '
                             f'count {MAX_OBSERVATION_COHORT_POOLS}.')
        pool_keys = [request.pool_key for request in requests]
        if len(set(pool_keys)) != len(pool_keys):
            raise ValueError('An observation cohort cannot repeat a pool key.')
        lease_duration = _require_positive_finite(lease_duration_seconds,
                                                  'lease_duration_seconds')
        authority_horizon = _require_positive_finite(
            authority_horizon_seconds, 'authority_horizon_seconds')
        if (isinstance(minimum_refresh_interval_seconds, bool) or
                not isinstance(minimum_refresh_interval_seconds,
                               (int, float)) or
                not math.isfinite(float(minimum_refresh_interval_seconds)) or
                float(minimum_refresh_interval_seconds) < 0):
            raise ValueError('minimum_refresh_interval_seconds must be a '
                             'finite nonnegative number.')
        minimum_refresh_interval = float(minimum_refresh_interval_seconds)
        lease_tokens = tuple(self._token_factory() for _ in requests)
        if any(not isinstance(token, uuid.UUID) for token in lease_tokens):
            raise ValueError('token_factory must return a UUID.')

        observations = observation_schema.demand_capacity_observations_v2_table
        leases_by_key: dict[str, PoolCapacityObservationLease] = {}
        with self._engine.begin() as connection:
            protocol_row = self._lock_protocol(connection)
            now = self._database_now(connection)
            generations: dict[str, int] = {}
            # Canonical lock order prevents overlapping cohorts from crossing
            # their pool-row locks after the shared protocol mutex.
            for request in sorted(requests, key=lambda item: item.pool_key):
                latest = self._latest_row(connection,
                                          request.pool_key,
                                          for_update=True)
                generation = 1
                if latest is not None:
                    latest_generation = latest['observation_generation']
                    if (isinstance(latest_generation, bool) or
                            not isinstance(latest_generation, int) or
                            latest_generation <= 0 or
                            latest_generation >= _MAX_BIGINT):
                        raise ObservationRepositoryCorruptionError(
                            'Latest observation generation is malformed or '
                            'exhausted.')
                    available = True
                    if latest['observation_status'] == (
                            observation_schema.IN_PROGRESS):
                        expires_at = _finite_float(latest['lease_expires_at'])
                        if expires_at is None:
                            raise ObservationRepositoryCorruptionError(
                                'Latest observation lease expiry is '
                                'malformed.')
                        if now < expires_at:
                            available = False
                    elif latest['observation_status'] in (
                            observation_schema.SUCCESS,
                            observation_schema.BLACKOUT):
                        latest_observed_at = _finite_float(
                            latest['observed_at'])
                        if latest_observed_at is None:
                            raise ObservationRepositoryCorruptionError(
                                'Latest observation start time is malformed.')
                        next_refresh_at = (latest_observed_at +
                                           minimum_refresh_interval)
                        if now < next_refresh_at:
                            available = False
                    else:
                        raise ObservationRepositoryCorruptionError(
                            'Latest observation status is malformed.')
                    generation = latest_generation + 1
                    if not available:
                        continue
                generations[request.pool_key] = generation

            sequence = protocol_row['zero_cost_admission_sequence']
            ordinary_sequence = protocol_row[
                'ordinary_zero_cost_admission_sequence']
            materialization_sequence = protocol_row[
                'zero_cost_materialization_sequence']
            assert isinstance(sequence, int)
            assert isinstance(ordinary_sequence, int)
            assert isinstance(materialization_sequence, int)

            lease_expires_at = now + lease_duration
            valid_until = now + authority_horizon
            for request, lease_token in zip(requests, lease_tokens):
                request_generation = generations.get(request.pool_key)
                if request_generation is None:
                    continue
                row_key = _row_key(request.pool_key, request_generation)
                connection.execute(
                    sqlalchemy.insert(observations).values(
                        context=row_key,
                        snapshot_time=now,
                        completed_at=now,
                        availability=None,
                        pool_key=request.pool_key,
                        physical_cluster_uid=request.physical_cluster_uid,
                        accelerator_names=list(request.accelerator_names),
                        access_context=request.access_context,
                        observation_generation=request_generation,
                        lease_token=lease_token,
                        lease_expires_at=lease_expires_at,
                        observation_sequence=sequence,
                        ordinary_admission_sequence=ordinary_sequence,
                        materialization_sequence=materialization_sequence,
                        observation_status=observation_schema.IN_PROGRESS,
                        payload=None,
                        payload_sha256=None,
                        observed_at=now,
                        valid_until=valid_until,
                        published_at=None,
                    ))
                leases_by_key[request.pool_key] = PoolCapacityObservationLease(
                    row_key=row_key,
                    pool_key=request.pool_key,
                    physical_cluster_uid=request.physical_cluster_uid,
                    accelerator_names=request.accelerator_names,
                    access_context=request.access_context,
                    access_contexts=request.access_contexts,
                    observation_generation=request_generation,
                    lease_token=lease_token,
                    lease_expires_at=lease_expires_at,
                    observation_sequence=sequence,
                    ordinary_admission_sequence=ordinary_sequence,
                    materialization_sequence=materialization_sequence,
                    observed_at=now,
                    valid_until=valid_until,
                )
        return tuple(leases_by_key[request.pool_key]
                     for request in requests
                     if request.pool_key in leases_by_key)

    def complete_success(
        self,
        lease: PoolCapacityObservationLease,
        payload: PoolCapacitySuccess,
        *,
        access_context: str,
    ) -> PoolCapacityObservation:
        """Conditionally publish a successful exact-card measurement."""
        if not isinstance(lease, PoolCapacityObservationLease):
            raise ValueError('lease must be PoolCapacityObservationLease.')
        if not isinstance(payload, PoolCapacitySuccess):
            raise ValueError('payload must be PoolCapacitySuccess.')
        payload_names = tuple(
            name for name, _ in payload.free_gpus_by_accelerator)
        if payload_names != lease.accelerator_names:
            raise ValueError('Success payload does not cover the exact pool '
                             'accelerator set.')
        winning_access_context = _bounded_text(access_context, 'access_context',
                                               1024)
        if winning_access_context not in lease.access_contexts:
            raise ValueError('Successful observation route was not authorized '
                             'by its lease.')
        return self._complete(lease,
                              payload,
                              access_context=winning_access_context)

    def complete_blackout(
        self,
        lease: PoolCapacityObservationLease,
        payload: PoolCapacityBlackout,
    ) -> PoolCapacityObservation:
        """Conditionally publish explicit non-authoritative failure evidence."""
        if not isinstance(lease, PoolCapacityObservationLease):
            raise ValueError('lease must be PoolCapacityObservationLease.')
        if not isinstance(payload, PoolCapacityBlackout):
            raise ValueError('payload must be PoolCapacityBlackout.')
        return self._complete(lease,
                              payload,
                              access_context=lease.access_context)

    def _complete(
        self,
        lease: PoolCapacityObservationLease,
        payload: PoolCapacityPayload,
        *,
        access_context: str,
    ) -> PoolCapacityObservation:
        if not isinstance(lease, PoolCapacityObservationLease):
            raise ValueError('lease must be PoolCapacityObservationLease.')
        observations = observation_schema.demand_capacity_observations_v2_table
        status = (observation_schema.SUCCESS if isinstance(
            payload, PoolCapacitySuccess) else observation_schema.BLACKOUT)
        with self._engine.begin() as connection:
            self._lock_protocol(connection)
            latest = self._latest_row(connection,
                                      lease.pool_key,
                                      for_update=True)
            if latest is None:
                raise StaleObservationWriterError(
                    'Observation row disappeared before completion.')
            latest_generation = latest['observation_generation']
            latest_token = latest['lease_token']
            if isinstance(latest_token, str):
                try:
                    latest_token = uuid.UUID(latest_token)
                except ValueError as exc:
                    raise ObservationRepositoryCorruptionError(
                        'Latest observation lease token is malformed.') from exc
            exact_identity = (
                latest_generation == lease.observation_generation and
                latest_token == lease.lease_token and
                latest['context'] == lease.row_key and
                latest['pool_key'] == lease.pool_key and
                latest['physical_cluster_uid'] == lease.physical_cluster_uid and
                latest['observation_sequence'] == lease.observation_sequence and
                latest['ordinary_admission_sequence']
                == lease.ordinary_admission_sequence and
                latest['materialization_sequence']
                == lease.materialization_sequence and
                latest['lease_expires_at'] == lease.lease_expires_at and
                latest['observed_at'] == lease.observed_at and
                latest['snapshot_time'] == lease.observed_at and
                latest['valid_until'] == lease.valid_until)
            try:
                latest_names = _canonical_accelerator_names(
                    latest['accelerator_names'])
            except ValueError:
                latest_names = ()
            exact_identity = (exact_identity and
                              latest_names == lease.accelerator_names)
            if not exact_identity:
                raise StaleObservationWriterError(
                    'Observation lease was superseded or changed.')

            if latest[
                    'observation_status'] in observation_schema.COMPLETED_STATUSES:
                completed = _decode_completed_row(latest)
                if (completed is not None and
                        completed.access_context == access_context and
                        completed.payload.canonical_value()
                        == payload.canonical_value()):
                    return completed
                raise StaleObservationWriterError(
                    'Observation generation already has a different or '
                    'malformed completion.')
            if latest['observation_status'] != observation_schema.IN_PROGRESS:
                raise ObservationRepositoryCorruptionError(
                    'Observation has an invalid completion state.')
            if latest['access_context'] != lease.access_context:
                raise StaleObservationWriterError(
                    'Observation lease route changed before completion.')
            now = self._database_now(connection)
            lease_expires_at = _finite_float(latest['lease_expires_at'])
            if lease_expires_at is None:
                raise ObservationRepositoryCorruptionError(
                    'Observation lease expiry is malformed.')
            if now >= lease_expires_at:
                raise StaleObservationWriterError(
                    'Observation lease expired before completion.')

            observed_at = float(latest['observed_at'])
            valid_until = float(latest['valid_until'])
            payload_sha256 = _authority_sha256(
                row_key=lease.row_key,
                pool_key=lease.pool_key,
                physical_cluster_uid=lease.physical_cluster_uid,
                accelerator_names=lease.accelerator_names,
                access_context=access_context,
                observation_generation=lease.observation_generation,
                lease_token=lease.lease_token,
                lease_expires_at=lease_expires_at,
                observation_sequence=lease.observation_sequence,
                ordinary_admission_sequence=(lease.ordinary_admission_sequence),
                materialization_sequence=lease.materialization_sequence,
                payload=payload,
                observed_at=observed_at,
                completed_at=now,
                valid_until=valid_until,
                published_at=now,
            )
            update = connection.execute(
                sqlalchemy.update(observations).where(
                    observations.c.context == lease.row_key,
                    observations.c.pool_key == lease.pool_key,
                    observations.c.observation_generation ==
                    lease.observation_generation,
                    observations.c.lease_token == lease.lease_token,
                    observations.c.observation_status ==
                    observation_schema.IN_PROGRESS,
                ).values(
                    completed_at=now,
                    access_context=access_context,
                    observation_status=status,
                    payload=payload.canonical_value(),
                    payload_sha256=payload_sha256,
                    published_at=now,
                ))
            if update.rowcount != 1:
                raise StaleObservationWriterError(
                    'Observation completion lost its conditional update.')
            completed_row = dict(latest)
            completed_row.update({
                'completed_at': now,
                'access_context': access_context,
                'observation_status': status,
                'payload': payload.canonical_value(),
                'payload_sha256': payload_sha256,
                'published_at': now,
            })
            decoded = _decode_completed_row(completed_row)
            if decoded is None:
                raise ObservationRepositoryCorruptionError(
                    'Committed observation could not pass its own authority '
                    'validation.')
            return decoded

    def read_exact_completed(
        self,
        pool_key: str,
        observation_generation: int,
    ) -> PoolCapacityObservation | None:
        """Read one exact completed generation, including blackout/expired.

        Legacy, in-progress, partial, malformed, and digest-mismatched rows
        return ``None``.  Callers must use ``is_authoritative_at`` or the
        stricter ``read_latest_authoritative`` before treating a success as
        capacity authority.
        """
        _parse_physical_pool_key(pool_key)
        if (isinstance(observation_generation, bool) or
                not isinstance(observation_generation, int) or
                observation_generation <= 0):
            raise ValueError('observation_generation must be positive.')
        table = observation_schema.demand_capacity_observations_v2_table
        with self._engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(table).where(
                    table.c.pool_key == pool_key, table.c.observation_generation
                    == observation_generation)).mappings().one_or_none()
        if row is None:
            return None
        return _decode_completed_row(row)

    def read_latest_completed(
        self,
        pool_key: str,
    ) -> PoolCapacityObservation | None:
        """Return the newest completed generation without granting authority.

        This is the inspection/provenance counterpart to
        :meth:`read_latest_authoritative`.  It deliberately returns a valid
        blackout or expired success so a caller can explain why a pool is
        unavailable, but it never falls back past a malformed newest completed
        row.  Capacity-consuming callers must still use
        ``read_latest_authoritative`` or prove freshness in their own locked
        transaction.
        """
        _parse_physical_pool_key(pool_key)
        table = observation_schema.demand_capacity_observations_v2_table
        with self._engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(table).where(
                    table.c.pool_key == pool_key,
                    table.c.observation_status.in_(
                        observation_schema.COMPLETED_STATUSES),
                ).order_by(table.c.observation_generation.desc()).limit(
                    1)).mappings().one_or_none()
        if row is None:
            return None
        return _decode_completed_row(row)

    def read_latest_authoritative(
        self,
        pool_key: str,
    ) -> PoolCapacityObservation | None:
        """Return the newest completed generation only when it grants authority.

        A newer in-progress generation does not erase the previous completed
        result.  A newer completed blackout, malformed row, or expired success
        returns ``None`` and never falls back to an older success.
        """
        _parse_physical_pool_key(pool_key)
        table = observation_schema.demand_capacity_observations_v2_table
        with self._engine.connect() as connection:
            now = self._database_now(connection)
            row = connection.execute(
                sqlalchemy.select(table).where(
                    table.c.pool_key == pool_key,
                    table.c.observation_status.in_(
                        observation_schema.COMPLETED_STATUSES),
                ).order_by(table.c.observation_generation.desc()).limit(
                    1)).mappings().one_or_none()
        if row is None:
            return None
        observation = _decode_completed_row(row)
        if (observation is None or not observation.is_authoritative_at(now)):
            return None
        return observation
