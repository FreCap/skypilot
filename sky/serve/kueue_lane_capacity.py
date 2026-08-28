"""Demand accounting for Kueue-backed reserved-fill admissions.

Kueue owns scheduling policy.  This module only projects the small durable
PostgreSQL admission state into the two capacity questions its callers need:

* does this intent supply current demand; and
* does it debit assigned GPU capacity?

It performs no Kubernetes/provider I/O and never commits independently of the
caller's transaction.
"""
from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any
import uuid

import sqlalchemy

from sky.adaptors import common as adaptors_common
from sky.serve import kueue_lane_lineage
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation_schema

zero_cost_actuation = adaptors_common.LazyImport(
    'sky.serve.zero_cost_actuation')
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')

_SERVICES = serve_state_schema.services_table
_LIFECYCLES = serve_state_schema.service_lifecycle_fences_table
_INTENTS = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_UNKNOWN_ADMISSION = object()
_PENDING_INTENT_STATES = frozenset({'GRANTED', 'ACTUATING', 'RETRYABLE'})


class KueueAdmissionCapacityError(RuntimeError):
    """Base error for Kueue admission capacity accounting."""


class KueueAdmissionCapacityConflict(KueueAdmissionCapacityError):
    """The exact admission or its materialized replica cannot be proven."""


class KueueReplicaCapacityClass(str, enum.Enum):
    """Autoscaler interpretation of one exact materialized admission."""

    FRESH_WAITING = 'FRESH_WAITING'
    POLICY_ADMITTED = 'POLICY_ADMITTED'
    UNKNOWN = 'UNKNOWN'


@dataclasses.dataclass(frozen=True)
class KueueReplicaCapacitySnapshot:
    """Deeply immutable scheduler-capacity input for one replica snapshot."""

    by_replica_id: Mapping[int, KueueReplicaCapacityClass]
    unknown_shapes: frozenset[tuple[str, int]] = frozenset()
    unbounded_unknown: bool = False
    replacement_surge_replica_ids: frozenset[int] = frozenset()
    replacement_surge_shapes: frozenset[tuple[str, int]] = frozenset()
    ordinary_scheduler_replica_ids: frozenset[int] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.by_replica_id, Mapping):
            raise ValueError('Kueue capacity classes must be a mapping.')
        classes = dict(self.by_replica_id)
        if (any(not isinstance(replica_id, int) or
                isinstance(replica_id, bool) or replica_id < 0
                for replica_id in classes) or
                any(not isinstance(value, KueueReplicaCapacityClass)
                    for value in classes.values()) or
                type(self.unbounded_unknown) is not bool):
            raise ValueError('Kueue capacity classes are malformed.')

        def _shapes(values: Any, *,
                    allow_unbounded: bool) -> frozenset[tuple[str, int]]:
            try:
                raw_shapes = frozenset(values)
            except TypeError as error:
                raise ValueError('Kueue capacity shapes are malformed.') from (
                    error)
            normalized: set[tuple[str, int]] = set()
            for shape in raw_shapes:
                if (not isinstance(shape, tuple) or len(shape) != 2 or
                        not isinstance(shape[0], str) or not shape[0] or
                        type(shape[1]) is not int or
                    (shape[1] < 1 and
                     not (allow_unbounded and shape == ('*', 0)))):
                    raise ValueError('Kueue capacity shapes are malformed.')
                normalized.add((shape[0].casefold(), shape[1]))
            return frozenset(normalized)

        def _replica_ids(values: Any) -> frozenset[int]:
            try:
                result = frozenset(values)
            except TypeError as error:
                raise ValueError('Kueue capacity replica IDs are malformed.') \
                    from error
            if any(not isinstance(replica_id, int) or
                   isinstance(replica_id, bool) or replica_id < 0
                   for replica_id in result):
                raise ValueError('Kueue capacity replica IDs are malformed.')
            return result

        surge_ids = _replica_ids(self.replacement_surge_replica_ids)
        ordinary_ids = _replica_ids(self.ordinary_scheduler_replica_ids)
        if ordinary_ids & set(classes) or surge_ids - set(classes):
            raise ValueError('Kueue capacity ownership is contradictory.')
        object.__setattr__(self, 'by_replica_id', MappingProxyType(classes))
        object.__setattr__(self, 'unknown_shapes',
                           _shapes(self.unknown_shapes, allow_unbounded=True))
        object.__setattr__(
            self, 'replacement_surge_shapes',
            _shapes(self.replacement_surge_shapes, allow_unbounded=False))
        object.__setattr__(self, 'replacement_surge_replica_ids', surge_ids)
        object.__setattr__(self, 'ordinary_scheduler_replica_ids', ordinary_ids)

    @property
    def has_unknown(self) -> bool:
        return self.unbounded_unknown or bool(self.unknown_shapes)


@dataclasses.dataclass(frozen=True)
class ZeroCostReplacementSurgeDecision:
    """Result of the fixed one-physical-Pod replacement exception."""

    allowed: bool
    uses_surge: bool


def replacement_compatibility_sha256(
    *,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    capacity_unit: str,
    accelerator: str,
    accelerator_count: int,
    worker_projection_sha256: str,
) -> str:
    """Bind a surge lease to one exact-shape replacement contract.

    A configured worker catalog proves only that each shape can run the
    service.  It does not prove cross-card request dominance.  Serve057
    therefore permits the above-ceiling replacement exception only for paid
    capacity with this exact ``(accelerator, accelerator_count)`` shape.  The
    candidate's immutable worker projection is included so mutable request
    profiles, an empty profile set, or a later catalog cannot widen that
    authority.
    """
    if not isinstance(service_hash, str) or not service_hash:
        raise ValueError('service_hash must be nonempty.')
    for value, field in ((service_lifecycle_epoch, 'service_lifecycle_epoch'),
                         (service_version, 'service_version')):
        if (not isinstance(value, int) or isinstance(value, bool) or value < 1):
            raise ValueError(f'{field} must be positive.')
    if capacity_unit not in ('physical', 'logical'):
        raise ValueError('capacity_unit is invalid.')
    if (not isinstance(accelerator, str) or not accelerator or
            accelerator != accelerator.casefold()):
        raise ValueError('accelerator must be canonical lowercase.')
    if (not isinstance(accelerator_count, int) or
            isinstance(accelerator_count, bool) or accelerator_count < 1):
        raise ValueError('accelerator_count must be positive.')
    if (not isinstance(worker_projection_sha256, str) or
            _SHA256_RE.fullmatch(worker_projection_sha256) is None):
        raise ValueError('worker_projection_sha256 must be lowercase SHA-256.')
    payload = {
        'protocol': 'serve057-exact-shape-replacement-v1',
        'service_hash': service_hash,
        'service_lifecycle_epoch': service_lifecycle_epoch,
        'service_version': service_version,
        'capacity_unit': capacity_unit,
        'reserved_shape': {
            'accelerator': accelerator,
            'accelerator_count': accelerator_count,
            'worker_projection_sha256': worker_projection_sha256,
        },
        # This explicit directional edge prevents a digest over only the
        # candidate identity from being misread as cross-card authority.
        'replaceable_paid_shape': {
            'accelerator': accelerator,
            'accelerator_count': accelerator_count,
        },
    }
    canonical = json.dumps(payload,
                           sort_keys=True,
                           separators=(',', ':'),
                           ensure_ascii=False,
                           allow_nan=False)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def decide_zero_cost_replacement_surge(
    *,
    max_capacity: int,
    physical_capacity_debit: int,
    candidate_capacity: int,
    candidate_is_kueue: bool,
    compatible_live_paid_capacity: int,
    surge_lease_active: bool = False,
) -> ZeroCostReplacementSurgeDecision:
    """Apply the normal ceiling followed by one physical replacement token.

    ``physical_capacity_debit`` includes every provider-possible replica and
    every live unmaterialized intent, including a fresh waiting Pod.  Capacity
    is measured in the service's normal unit, but an overflow candidate always
    consumes exactly one *physical* token: an eight-GPU Pod is one surge just as
    a one-GPU Pod is.  Once the debit is already above the normal ceiling, that
    token is occupied and cannot chain.
    """
    for value, field in ((max_capacity, 'max_capacity'),
                         (physical_capacity_debit, 'physical_capacity_debit'),
                         (candidate_capacity, 'candidate_capacity')):
        if (not isinstance(value, int) or isinstance(value, bool) or
                value < (1 if field == 'candidate_capacity' else 0)):
            raise ValueError(f'{field} has an invalid capacity.')
    if type(candidate_is_kueue) is not bool:
        raise ValueError('candidate_is_kueue must be boolean.')
    if (not isinstance(compatible_live_paid_capacity, int) or
            isinstance(compatible_live_paid_capacity, bool) or
            compatible_live_paid_capacity < 0):
        raise ValueError(
            'compatible_live_paid_capacity has an invalid capacity.')
    if type(surge_lease_active) is not bool:
        raise ValueError('surge_lease_active must be boolean.')

    projected_capacity = physical_capacity_debit + candidate_capacity
    if projected_capacity <= max_capacity:
        return ZeroCostReplacementSurgeDecision(True, False)
    overflow = projected_capacity - max_capacity
    if (not surge_lease_active and physical_capacity_debit <= max_capacity and
            candidate_is_kueue and compatible_live_paid_capacity >= overflow):
        return ZeroCostReplacementSurgeDecision(True, True)
    return ZeroCostReplacementSurgeDecision(False, False)


@dataclasses.dataclass(frozen=True)
class KueueAdmissionCapacityProjection:
    """One locked projection of all admission rows affecting a service.

    Ordinary-scheduler ownership is retained as positive immutable evidence;
    admission-row absence is never itself proof of that path.  The projection
    deliberately returns ``False`` from the Kueue lookups only for a fresh,
    exact ``POD_WAITING`` receipt. Every ambiguous state remains assigned and
    sets ``has_unknown``. Bounded unknowns suppress only their exact shape;
    malformed shape identity fails the whole service closed.
    """

    rows: tuple[Any, ...]
    row_by_intent_key: Mapping[str, Any]
    planned_capacity_by_intent_key: Mapping[str, int]
    demand_supply_intent_keys: frozenset[str]
    assigned_gpu_intent_keys: frozenset[str]
    fresh_waiting_intent_keys: frozenset[str]
    admitted_intent_keys: frozenset[str]
    fresh_waiting_replica_record_ids: frozenset[tuple[int, uuid.UUID]]
    admitted_replica_record_ids: frozenset[tuple[int, uuid.UUID]]
    ordinary_scheduler_intent_keys: frozenset[str]
    unknown_intent_keys: frozenset[str]
    unknown_domains: tuple[str, ...]
    unknown_shapes: frozenset[tuple[str, int]]
    unbounded_unknown: bool
    replacement_surge_intent_keys: frozenset[str]
    replacement_surge_shapes: frozenset[tuple[str, int]]
    replacement_surge_replica_record_ids: frozenset[tuple[int, uuid.UUID]]
    now: datetime.datetime

    def demand_supply_for_intent(self, intent_key: str | None) -> bool | None:
        """Return False only for fresh waiting; admitted is demand supply."""
        if intent_key is None or intent_key not in self.row_by_intent_key:
            return None
        return intent_key in self.demand_supply_intent_keys

    def assigned_gpu_for_intent(self, intent_key: str | None) -> bool | None:
        """Return False only for fresh waiting; ambiguity debits capacity."""
        if intent_key is None or intent_key not in self.row_by_intent_key:
            return None
        return intent_key in self.assigned_gpu_intent_keys

    def uses_ordinary_scheduler(self, intent_key: str | None) -> bool:
        """Return whether immutable version state proves the non-Kueue path."""
        return (isinstance(intent_key, str) and
                intent_key in self.ordinary_scheduler_intent_keys)

    @property
    def has_unknown(self) -> bool:
        return self.unbounded_unknown or bool(self.unknown_domains)


def replica_capacity_snapshot_from_projection(
    replica_infos: list[Any] | tuple[Any, ...],
    projection: KueueAdmissionCapacityProjection,
) -> KueueReplicaCapacitySnapshot:
    """Classify exact replica rows from the same locked admission projection."""
    if not isinstance(projection, KueueAdmissionCapacityProjection):
        raise TypeError('projection must be a Kueue admission projection.')
    reserved_infos = [
        info for info in replica_infos if
        isinstance(getattr(info, 'reserved_fill_intent_idempotency_key', None),
                   str) and bool(info.reserved_fill_intent_idempotency_key)
    ]
    classes: dict[int, KueueReplicaCapacityClass] = {}
    ordinary_replica_ids: set[int] = set()
    for info in reserved_infos:
        intent_key = info.reserved_fill_intent_idempotency_key
        if intent_key not in projection.row_by_intent_key:
            if projection.uses_ordinary_scheduler(intent_key):
                ordinary_replica_ids.add(int(info.replica_id))
            # Preserve the historical no-override behavior for rows outside
            # the Kueue projection.  A genuinely expected-but-missing Kueue
            # admission is represented by the projection's UNKNOWN sentinel
            # and therefore does not take this branch.
            continue
        try:
            record = (int(info.replica_id),
                      uuid.UUID(str(info.replica_record_id)))
        except (TypeError, ValueError, AttributeError):
            classes[int(info.replica_id)] = KueueReplicaCapacityClass.UNKNOWN
            continue
        if record in projection.fresh_waiting_replica_record_ids:
            value = KueueReplicaCapacityClass.FRESH_WAITING
        elif record in projection.admitted_replica_record_ids:
            value = KueueReplicaCapacityClass.POLICY_ADMITTED
        else:
            value = KueueReplicaCapacityClass.UNKNOWN
        classes[int(info.replica_id)] = value
    surge_replica_ids = {
        replica_id for replica_id, record_id in
        projection.replacement_surge_replica_record_ids if any(
            int(info.replica_id) == replica_id and
            str(getattr(info, 'replica_record_id', '')) == str(record_id)
            for info in reserved_infos)
    }
    return KueueReplicaCapacitySnapshot(
        by_replica_id=classes,
        unknown_shapes=projection.unknown_shapes,
        unbounded_unknown=projection.unbounded_unknown,
        replacement_surge_replica_ids=frozenset(surge_replica_ids),
        replacement_surge_shapes=projection.replacement_surge_shapes,
        ordinary_scheduler_replica_ids=frozenset(ordinary_replica_ids))


def _db_now(connection: sqlalchemy.engine.Connection) -> datetime.datetime:
    return connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()


def _row_domain(row: Any, intent_key: str) -> str:
    value = getattr(row, 'unresolved_domain_sha256', None)
    return str(value) if value else f'intent:{intent_key}'


def _exact_replica_record(row: Any) -> tuple[int, uuid.UUID] | None:
    replica_id = getattr(row, 'replica_id', None)
    record_id = getattr(row, 'replica_record_id', None)
    if replica_id is None or record_id is None:
        return None
    try:
        return int(replica_id), uuid.UUID(str(record_id))
    except (TypeError, ValueError, AttributeError):
        return None


def _state_value(row: Any) -> str:
    state = getattr(row, 'state', None)
    return str(state.value) if isinstance(state, enum.Enum) else str(state)


def _capacity_shape(row: Any) -> tuple[str, int] | None:
    """Return an exact immutable GPU shape from a row-like object."""
    if isinstance(row, Mapping):
        accelerator = row.get('accelerator')
        accelerator_count = row.get('accelerator_count')
    else:
        accelerator = getattr(row, 'accelerator', None)
        accelerator_count = getattr(row, 'accelerator_count', None)
    if (not isinstance(accelerator, str) or not accelerator or
            not isinstance(accelerator_count, int) or
            isinstance(accelerator_count, bool) or accelerator_count < 1):
        return None
    return accelerator.casefold(), accelerator_count


def lock_capacity_projection_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    accounting_cards: set[str],
    locked_intent_rows: tuple[Mapping[str, Any], ...],
    planned_capacity_by_intent_key: Mapping[str, int],
    capacity_unit_by_intent_key: Mapping[str, str],
    live_replica_record_ids: set[tuple[int, uuid.UUID]],
    provider_present_replica_record_ids: set[tuple[int, uuid.UUID]],
    live_intent_keys: set[str],
    read: bool = False,
) -> KueueAdmissionCapacityProjection:
    """Lock and classify admissions against immutable intent projections.

    Every live intent is reclassified from its exact version projection.  Only
    an intent proven to use the ordinary Kubernetes scheduler may omit an
    admission. Missing Kueue rows and any copied lineage mismatch remain
    conservative UNKNOWN capacity, exact-shape when that shape is provable and
    otherwise unbounded.
    """
    if not accounting_cards:
        raise KueueAdmissionCapacityConflict(
            'Kueue admission has no capacity accounting class.')
    aggregate = accounting_cards == {'*'}
    if '*' in accounting_cards and not aggregate:
        raise KueueAdmissionCapacityConflict(
            'Kueue admission mixes aggregate and exact-card accounting.')

    repository = kueue_lane_lineage.KueueAdmissionRepository()
    try:
        if read:
            rows = repository.lock_service_admissions_in_connection(
                connection, service_name, service_hash, read=True)
        else:
            rows = repository.lock_service_admissions_in_connection(
                connection, service_name, service_hash)
    except kueue_lane_lineage.KueueAdmissionError as error:
        raise KueueAdmissionCapacityConflict(
            'Kueue admission rows could not be locked.') from error
    now = _db_now(connection)

    by_intent: dict[str, Any] = {}
    demand_supply: set[str] = set()
    assigned_gpu: set[str] = set()
    fresh_waiting: set[str] = set()
    admitted: set[str] = set()
    fresh_waiting_records: set[tuple[int, uuid.UUID]] = set()
    admitted_records: set[tuple[int, uuid.UUID]] = set()
    ordinary_scheduler_intents: set[str] = set()
    unknown: set[str] = set()
    unknown_intents: set[str] = set()
    unknown_shapes: set[tuple[str, int]] = set()
    unbounded_unknown = False
    surge_intents: set[str] = set()
    surge_shapes: set[tuple[str, int]] = set()
    surge_records: set[tuple[int, uuid.UUID]] = set()

    for row in rows:
        intent_key = getattr(row, 'intent_idempotency_key', None)
        if (not isinstance(intent_key, str) or not intent_key or
                intent_key in by_intent):
            raise KueueAdmissionCapacityConflict(
                'Kueue admission rows do not map one-to-one to intents.')
        by_intent[intent_key] = row
        # Anything except a proven fresh wait must conservatively debit the
        # assigned/max capacity view, even when final paid admission will fail.
        assigned_gpu.add(intent_key)

    intents: dict[str, Mapping[str, Any]] = {}
    for intent in locked_intent_rows:
        intent_key = intent.get('intent_idempotency_key')
        if (not isinstance(intent_key, str) or not intent_key or
                intent_key in intents):
            raise KueueAdmissionCapacityConflict(
                'Locked intent rows do not map one-to-one to identities.')
        intents[intent_key] = intent

    # Callers additionally name retained replica ownership, but liveness of
    # an unmaterialized intent is PostgreSQL state, not a process-local
    # replica snapshot.  Derive it while the complete intent set is locked so
    # a missing admission cannot disappear from autoscaler retirement merely
    # because no ReplicaInfo exists yet.
    effective_live_intent_keys = set(live_intent_keys)
    for intent_key, intent in intents.items():
        state = intent.get('state')
        if state == 'COMMITTED':
            effective_live_intent_keys.add(intent_key)
        elif state in _PENDING_INTENT_STATES:
            valid_until = intent.get('valid_until')
            try:
                is_live = valid_until is not None and valid_until > now
            except TypeError:
                # A malformed liveness boundary cannot prove this intent is
                # safe to omit from final retirement accounting.
                is_live = True
            if is_live:
                effective_live_intent_keys.add(intent_key)

    identity_by_intent: dict[str,
                             kueue_lane_lineage.KueueAdmissionIdentity] = {}

    def _mark_unknown(
        intent_key: str,
        *,
        intent: Mapping[str, Any] | None,
        row: Any | None,
        identity: kueue_lane_lineage.KueueAdmissionIdentity | None = None,
    ) -> None:
        nonlocal unbounded_unknown
        assigned_gpu.add(intent_key)
        if intent_key not in by_intent:
            # Membership, rather than the value, is the conservative override
            # consumed by demand/assigned accounting below.
            by_intent[intent_key] = _UNKNOWN_ADMISSION
        unknown_intents.add(intent_key)
        if identity is not None:
            unknown.add(identity.unresolved_domain_sha256)
            unknown_shapes.add(
                (identity.accelerator, identity.accelerator_count))
            return
        unknown.add(
            _row_domain(row, intent_key
                       ) if row is not None else f'intent:{intent_key}')
        shape = _capacity_shape(intent)
        if shape is None:
            shape = _capacity_shape(row)
        if shape is None:
            unbounded_unknown = True
        else:
            unknown_shapes.add(shape)

    # Reconstruct the Kueue-vs-East decision from immutable version state for
    # every admission and every live intent.  An admission row is never itself
    # authority for selecting which scheduler path the intent used.
    for intent_key in sorted(set(by_intent) | effective_live_intent_keys):
        locked_intent = intents.get(intent_key)
        admission_row = by_intent.get(intent_key)
        if locked_intent is None:
            _mark_unknown(intent_key, intent=None, row=admission_row)
            continue
        try:
            expected_identity = (
                zero_cost_actuation.
                kueue_admission_identity_for_locked_intent_in_connection(
                    connection, locked_intent))
        except (kueue_lane_lineage.KueueAdmissionError,
                zero_cost_actuation.ZeroCostActuationError, TypeError,
                ValueError):
            _mark_unknown(intent_key, intent=locked_intent, row=admission_row)
            continue
        if expected_identity is None:
            if admission_row is not None:
                _mark_unknown(intent_key,
                              intent=locked_intent,
                              row=admission_row)
            else:
                ordinary_scheduler_intents.add(intent_key)
            continue
        if admission_row is None:
            _mark_unknown(intent_key,
                          intent=locked_intent,
                          row=None,
                          identity=expected_identity)
            continue
        try:
            checked_identity = (
                kueue_lane_lineage.validate_admission_intent_identity(
                    admission_row, locked_intent))
        except (kueue_lane_lineage.KueueAdmissionError, TypeError, ValueError):
            _mark_unknown(intent_key,
                          intent=locked_intent,
                          row=admission_row,
                          identity=expected_identity)
            continue
        if checked_identity != expected_identity:
            _mark_unknown(intent_key,
                          intent=locked_intent,
                          row=admission_row,
                          identity=expected_identity)
            continue
        identity_by_intent[intent_key] = expected_identity

    for row in rows:
        intent_key = getattr(row, 'intent_idempotency_key')
        domain = _row_domain(row, intent_key)
        if intent_key in unknown_intents:
            continue

        planned = planned_capacity_by_intent_key.get(intent_key)
        capacity_unit = capacity_unit_by_intent_key.get(intent_key)
        shape = _capacity_shape(row)
        accelerator = '' if shape is None else shape[0]
        shape_is_valid = shape is not None
        card = '*' if aggregate else accelerator
        row_version = getattr(row, 'service_version', None)
        owner_matches = (getattr(row, 'service_name', None) == service_name and
                         getattr(row, 'service_hash', None) == service_hash and
                         getattr(row, 'service_lifecycle_epoch',
                                 None) == service_lifecycle_epoch and
                         isinstance(row_version, int) and
                         not isinstance(row_version, bool) and
                         0 < row_version <= service_version and
                         getattr(row, 'planned_capacity', None) == planned and
                         getattr(row, 'capacity_unit', None) == capacity_unit
                         and capacity_unit in ('physical', 'logical') and
                         intent_key in identity_by_intent)
        if (not isinstance(planned, int) or isinstance(planned, bool) or
                planned < 1 or card not in accounting_cards or
                not owner_matches or not shape_is_valid):
            unknown.add(domain)
            unknown_intents.add(intent_key)
            if shape is not None:
                unknown_shapes.add(shape)
            else:
                unbounded_unknown = True
            continue

        assert shape is not None
        surge_units = getattr(row, 'replacement_surge_units', 0)
        surge_digest = getattr(row, 'replacement_compatibility_sha256', None)
        if (not isinstance(surge_units, int) or isinstance(surge_units, bool) or
                surge_units < 0 or
            ((surge_units == 0) != (surge_digest is None))):
            unknown.add(domain)
            unknown_intents.add(intent_key)
            unknown_shapes.add(shape)
            continue
        if surge_units > 0:
            # The durable positive lease remains a conservation barrier even
            # when its digest later proves malformed.  Digest validity gates
            # replacement authority; it cannot make the physical lease
            # disappear.
            surge_intents.add(intent_key)
            surge_shapes.add(shape)
            exact_retained_record = _exact_replica_record(row)
            if (exact_retained_record is not None and exact_retained_record
                    in provider_present_replica_record_ids):
                surge_records.add(exact_retained_record)
            capacity_unit = capacity_unit_by_intent_key.get(intent_key)
            try:
                expected_digest = replacement_compatibility_sha256(
                    service_hash=str(getattr(row, 'service_hash', '')),
                    service_lifecycle_epoch=int(
                        getattr(row, 'service_lifecycle_epoch', 0)),
                    service_version=int(getattr(row, 'service_version', 0)),
                    capacity_unit=str(capacity_unit),
                    accelerator=shape[0],
                    accelerator_count=shape[1],
                    worker_projection_sha256=str(
                        getattr(row, 'worker_projection_sha256', '')))
            except (TypeError, ValueError):
                expected_digest = None
            if surge_digest != expected_digest:
                unknown.add(domain)
                unknown_intents.add(intent_key)
                unknown_shapes.add(shape)
                continue

        exact_record = _exact_replica_record(row)
        exact_live = (exact_record is not None and
                      exact_record in live_replica_record_ids)
        state = _state_value(row)
        if state == kueue_lane_lineage.KueueAdmissionState.POD_WAITING.value:
            valid_until = getattr(row, 'valid_until', None)
            if (not exact_live or valid_until is None or valid_until <= now):
                unknown.add(domain)
                unknown_intents.add(intent_key)
                assert shape is not None
                unknown_shapes.add(shape)
                continue
            assigned_gpu.remove(intent_key)
            fresh_waiting.add(intent_key)
            assert exact_record is not None
            fresh_waiting_records.add(exact_record)
        elif state == kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED.value:
            if not exact_live:
                unknown.add(domain)
                unknown_intents.add(intent_key)
                assert shape is not None
                unknown_shapes.add(shape)
                continue
            demand_supply.add(intent_key)
            admitted.add(intent_key)
            assert exact_record is not None
            admitted_records.add(exact_record)
        elif state == kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING.value:
            unknown.add(domain)
            unknown_intents.add(intent_key)
            assert shape is not None
            unknown_shapes.add(shape)
        else:
            unknown.add(domain)
            unknown_intents.add(intent_key)
            if shape is not None:
                unknown_shapes.add(shape)

    return KueueAdmissionCapacityProjection(
        rows=tuple(rows),
        row_by_intent_key=by_intent,
        planned_capacity_by_intent_key=dict(planned_capacity_by_intent_key),
        demand_supply_intent_keys=frozenset(demand_supply),
        assigned_gpu_intent_keys=frozenset(assigned_gpu),
        fresh_waiting_intent_keys=frozenset(fresh_waiting),
        admitted_intent_keys=frozenset(admitted),
        fresh_waiting_replica_record_ids=frozenset(fresh_waiting_records),
        admitted_replica_record_ids=frozenset(admitted_records),
        ordinary_scheduler_intent_keys=frozenset(ordinary_scheduler_intents),
        unknown_intent_keys=frozenset(unknown_intents),
        unknown_domains=tuple(sorted(unknown)),
        unknown_shapes=frozenset(unknown_shapes),
        unbounded_unknown=unbounded_unknown,
        replacement_surge_intent_keys=frozenset(surge_intents),
        replacement_surge_shapes=frozenset(surge_shapes),
        replacement_surge_replica_record_ids=frozenset(surge_records),
        now=now)


def snapshot_replica_capacity_classes(
    service_name: str,
    replica_infos: list[Any],
) -> KueueReplicaCapacitySnapshot:
    """Read exact Kueue admission classes for one autoscaler snapshot.

    The method intentionally performs its PostgreSQL work before the
    controller enters routing serialization.  Final paid authority is still
    revalidated transactionally by :mod:`capacity_admission`.
    """
    reserved_infos = [
        info for info in replica_infos if
        isinstance(getattr(info, 'reserved_fill_intent_idempotency_key', None),
                   str) and bool(info.reserved_fill_intent_idempotency_key)
    ]
    repository = kueue_lane_lineage.KueueAdmissionRepository()
    try:
        engine = repository.engine
        with engine.begin() as connection:
            # This read-only snapshot shares the same prefix as Kueue
            # materialization observers.  Taking it before the all-intent
            # scan removes the historical intent -> service inversion while
            # still fencing protocol, lifecycle, and service writers.
            serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
                connection)
            lifecycle = connection.execute(
                sqlalchemy.select(_LIFECYCLES.c.epoch).where(
                    _LIFECYCLES.c.name == service_name).with_for_update(
                        read=True)).scalar_one_or_none()
            service = connection.execute(
                sqlalchemy.select(
                    _SERVICES.c.hash, _SERVICES.c.lifecycle_epoch,
                    _SERVICES.c.current_version).where(
                        _SERVICES.c.name == service_name).with_for_update(
                            read=True)).mappings().one_or_none()
            if (service is None or lifecycle is None or
                    service['lifecycle_epoch'] != lifecycle):
                raise KueueAdmissionCapacityConflict(
                    'Kueue admission service lifecycle owner is absent or '
                    'inconsistent.')
            intent_rows = connection.execute(
                sqlalchemy.select(_INTENTS).where(
                    _INTENTS.c.service_name == service_name,
                    _INTENTS.c.service_hash == service['hash']).order_by(
                        _INTENTS.c.intent_idempotency_key).with_for_update()
            ).mappings().all()
            planned_by_intent: dict[str, int] = {}
            capacity_unit_by_intent: dict[str, str] = {}
            for row in intent_rows:
                key = row['intent_idempotency_key']
                planned = row['planned_capacity']
                capacity_unit = row['capacity_unit']
                if (not isinstance(key, str) or not key or
                        not isinstance(planned, int) or
                        isinstance(planned, bool) or planned < 1 or
                        capacity_unit not in ('physical', 'logical')):
                    raise KueueAdmissionCapacityConflict(
                        'Kueue intent capacity is malformed.')
                planned_by_intent[key] = planned
                capacity_unit_by_intent[key] = capacity_unit

            live_records: set[tuple[int, uuid.UUID]] = set()
            retained_records: set[tuple[int, uuid.UUID]] = set()
            live_intent_keys: set[str] = set()
            cards: set[str] = set()
            for info in reserved_infos:
                key = info.reserved_fill_intent_idempotency_key
                live_intent_keys.add(key)
                raw_card = _replica_accelerator(info)
                if raw_card is not None:
                    cards.add(raw_card)
                try:
                    record_id = uuid.UUID(str(info.replica_record_id))
                except (TypeError, ValueError, AttributeError):
                    continue
                record = (int(info.replica_id), record_id)
                retained_records.add(record)
                if (getattr(info, 'is_terminal', False) or
                        getattr(getattr(info, 'status_property', None),
                                'is_scale_down', False)):
                    continue
                live_records.add(record)
            if not cards:
                cards = {'*'}
            projection = lock_capacity_projection_in_connection(
                connection,
                service_name=service_name,
                service_hash=str(service['hash']),
                service_lifecycle_epoch=int(service['lifecycle_epoch']),
                service_version=int(service['current_version']),
                accounting_cards=cards,
                locked_intent_rows=tuple(intent_rows),
                planned_capacity_by_intent_key=planned_by_intent,
                capacity_unit_by_intent_key=capacity_unit_by_intent,
                live_replica_record_ids=live_records,
                provider_present_replica_record_ids=retained_records,
                live_intent_keys=live_intent_keys,
                read=True)
    except (sqlalchemy.exc.SQLAlchemyError,
            kueue_lane_lineage.KueueAdmissionError) as error:
        raise KueueAdmissionCapacityConflict(
            'Kueue admission snapshot is unavailable.') from error
    return replica_capacity_snapshot_from_projection(reserved_infos, projection)


def _replica_accelerator(info: Any) -> str | None:
    """Return the exact canonical card persisted on a ReplicaInfo."""
    for raw in (getattr(info, 'location',
                        None), getattr(info, 'resources_override', None)):
        accelerators = (raw.get('accelerators')
                        if isinstance(raw, Mapping) else None)
        if isinstance(accelerators, Mapping) and len(accelerators) == 1:
            card = next(iter(accelerators))
            if isinstance(card, str) and card:
                return card.casefold()
    return None
