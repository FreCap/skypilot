"""PostgreSQL ownership for three-state Kueue reserved-fill admissions.

This module is provider-free.  Every mutating method participates in the
caller's transaction and never commits, waits for Kubernetes, or performs a
provider operation.
"""
from __future__ import annotations

from collections.abc import Mapping
import copy
import dataclasses
import datetime
import enum
import hashlib
import json
import re
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.adaptors import common as adaptors_common
from sky.serve import kueue_lane_lineage_schema
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation_schema
from sky.utils.db import db_utils

WAITING_OBSERVATION_TTL_SECONDS = 15

_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_TERMINAL_STATUSES = frozenset({'SUCCEEDED', 'FAILED', 'CANCELLED'})
_TERMINAL_PROFILE_FIELDS = ('binding_protocol_version', 'profile_kind',
                            'profile_version', 'profile_digest',
                            'capability_cohort_epoch',
                            'capability_profile_set_digest',
                            'receipt_protocol_version')
_ADMISSIONS = kueue_lane_lineage_schema.serve_kueue_admissions_table
_INTENTS = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
_REPLICAS = serve_state_schema.replicas_table
_SERVICES = serve_state_schema.services_table
# ordinary_launch_binding imports zero_cost_actuation, which imports this
# module.  Resolve the existing table only at graph-validation time.
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')
request_postgres_schema = adaptors_common.LazyImport(
    'sky.server.requests.postgres_schema')
zero_cost_actuation = adaptors_common.LazyImport(
    'sky.serve.zero_cost_actuation')
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')


class KueueAdmissionState(str, enum.Enum):
    """The complete closed state set for one durable Kueue admission."""

    INTENT_PENDING = 'INTENT_PENDING'
    POD_WAITING = 'POD_WAITING'
    POLICY_ADMITTED = 'POLICY_ADMITTED'


class KueueAdmissionError(RuntimeError):
    """Base error for durable Kueue admission authority."""


class KueueAdmissionConflict(KueueAdmissionError):
    """The exact admission or its materialized graph changed."""


class KueueAdmissionUnavailable(KueueAdmissionError):
    """The PostgreSQL-only admission repository is unavailable."""


def _require_nonempty(value: str, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{field} must be a non-empty string.')
    return value


def _require_nonnegative(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f'{field} must be a non-negative integer.')
    return value


def _require_positive(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f'{field} must be a positive integer.')
    return value


def _require_sha256(value: str, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{field} must be a lowercase SHA-256 digest.')
    return value


def _require_capacity_unit(value: str) -> str:
    if value not in ('physical', 'logical'):
        raise ValueError('capacity_unit must be physical or logical.')
    return value


def _length_prefixed(value: str) -> str:
    return f'{len(value.encode("utf-8"))}:{value}'


@dataclasses.dataclass(frozen=True)
class KueueAdmissionIdentity:
    """Exact immutable admission identity and its unresolved domain."""

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    pool_key: str
    pool_epoch: int
    physical_cluster_uid: str
    kubernetes_context: str
    accelerator: str
    accelerator_count: int
    worker_projection_sha256: str

    def validate(self) -> None:
        for field in ('service_name', 'service_hash', 'pool_key',
                      'physical_cluster_uid', 'kubernetes_context'):
            _require_nonempty(getattr(self, field), field)
        for field in ('service_lifecycle_epoch', 'service_version',
                      'pool_epoch', 'accelerator_count'):
            _require_positive(getattr(self, field), field)
        accelerator = _require_nonempty(self.accelerator, 'accelerator')
        if accelerator != accelerator.casefold():
            raise ValueError('accelerator must be canonical lowercase.')
        _require_sha256(self.worker_projection_sha256,
                        'worker_projection_sha256')

    @property
    def unresolved_domain_sha256(self) -> str:
        """Return the frozen provider-free unresolved-domain digest."""
        self.validate()
        payload = '|'.join((
            _length_prefixed(self.service_name),
            str(self.service_lifecycle_epoch),
            _length_prefixed(self.physical_cluster_uid),
            _length_prefixed(self.accelerator),
            str(self.accelerator_count),
        ))
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()


@dataclasses.dataclass(frozen=True)
class KueueAdmissionRow:
    """One intent-owned admission with monotonic materialized facts."""

    intent_idempotency_key: str
    service_name: str
    unresolved_domain_sha256: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    pool_key: str
    pool_epoch: int
    physical_cluster_uid: str
    kubernetes_context: str
    accelerator: str
    accelerator_count: int
    worker_projection_sha256: str
    capacity_unit: str
    planned_capacity: int
    state: KueueAdmissionState
    replica_id: int | None
    replica_record_id: uuid.UUID | None
    provider_cluster_generation: int | None
    association_id: uuid.UUID | None
    pod_namespace: str | None
    pod_name: str | None
    pod_uid: str | None
    pod_receipt: Mapping[str, Any] | None
    pod_receipt_sha256: str | None
    observed_at: datetime.datetime | None
    valid_until: datetime.datetime | None
    admitted_at: datetime.datetime | None
    replacement_surge_units: int
    replacement_compatibility_sha256: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> KueueAdmissionRow:
        values = {column.name: row[column.name] for column in _ADMISSIONS.c}
        values['state'] = KueueAdmissionState(values['state'])
        return cls(**values)


@dataclasses.dataclass(frozen=True)
class ProviderFreeTerminalAdmissionProof:
    """Transaction-bound proof for one provider-free terminal admission."""

    transaction_id: int
    service_name: str
    service_hash: str
    intent_idempotency_key: str
    unresolved_domain_sha256: str
    intent_updated_at: datetime.datetime
    admission_updated_at: datetime.datetime
    teardown_authorized: bool
    checked_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class MaterializedAdmissionRetirementTarget:
    """Caller-owned immutable replica identity requested for retirement."""

    replica_id: int
    replica_record_id: uuid.UUID

    def validate(self) -> None:
        _require_positive(self.replica_id, 'replica_id')
        if not isinstance(self.replica_record_id, uuid.UUID):
            raise ValueError('replica_record_id must be a UUID.')


@dataclasses.dataclass(frozen=True)
class MaterializedRetirementProofBase:
    """Common transaction-bound proof for a provider-clean Kueue graph."""

    transaction_id: int
    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    intent_idempotency_key: str
    intent_updated_at: datetime.datetime
    replica_id: int
    replica_record_id: uuid.UUID
    provider_cluster_generation: int
    association_id: uuid.UUID
    association_updated_at: datetime.datetime
    request_id: str
    request_updated_at: datetime.datetime | None
    checked_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class MaterializedAdmissionRetirementProof(MaterializedRetirementProofBase):
    """Retirement proof for a graph with one exact admission row."""

    admission_updated_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class AdmissionlessMaterializedRetirementProof(MaterializedRetirementProofBase):
    """Whole-service retirement proof for a provider-free missing admission."""


MaterializedRetirementProof = (MaterializedAdmissionRetirementProof |
                               AdmissionlessMaterializedRetirementProof)


@dataclasses.dataclass(frozen=True)
class ExactPodAbsenceProbeTarget:
    """Provider-read target frozen from one admitted materialized graph."""

    identity: KueueAdmissionIdentity
    intent_idempotency_key: str
    replica_id: int
    replica_record_id: uuid.UUID
    provider_cluster_generation: int
    association_id: uuid.UUID
    pod_namespace: str
    pod_name: str
    pod_uid: str

    def validate(self) -> None:
        if not isinstance(self.identity, KueueAdmissionIdentity):
            raise TypeError('identity must be KueueAdmissionIdentity.')
        self.identity.validate()
        _require_sha256(self.intent_idempotency_key, 'intent_idempotency_key')
        _require_positive(self.replica_id, 'replica_id')
        _require_positive(self.provider_cluster_generation,
                          'provider_cluster_generation')
        for field in ('replica_record_id', 'association_id'):
            if not isinstance(getattr(self, field), uuid.UUID):
                raise ValueError(f'{field} must be a UUID.')
        for field in ('pod_namespace', 'pod_name', 'pod_uid'):
            _require_nonempty(getattr(self, field), field)


@dataclasses.dataclass(frozen=True)
class AdmissionlessPhysicalAbsenceProbeTarget:
    """Provider-read target for one teardown-fenced missing admission."""

    identity: KueueAdmissionIdentity
    intent_idempotency_key: str
    replica_id: int
    replica_record_id: uuid.UUID
    provider_cluster_generation: int
    association_id: uuid.UUID
    cluster_name: str

    def validate(self) -> None:
        if not isinstance(self.identity, KueueAdmissionIdentity):
            raise TypeError('identity must be KueueAdmissionIdentity.')
        self.identity.validate()
        _require_sha256(self.intent_idempotency_key, 'intent_idempotency_key')
        _require_positive(self.replica_id, 'replica_id')
        if not isinstance(self.replica_record_id, uuid.UUID):
            raise ValueError('replica_record_id must be a UUID.')
        _require_positive(self.provider_cluster_generation,
                          'provider_cluster_generation')
        if not isinstance(self.association_id, uuid.UUID):
            raise ValueError('association_id must be a UUID.')
        _require_nonempty(self.cluster_name, 'cluster_name')


class PhysicalAbsenceLoadState(str, enum.Enum):
    """Closed result set for loading durable provider-absence authority."""

    NOT_APPLICABLE = 'NOT_APPLICABLE'
    NEEDS_PROBE = 'NEEDS_PROBE'
    ALREADY_PROVEN = 'ALREADY_PROVEN'


@dataclasses.dataclass(frozen=True)
class ExactPodAbsenceLoadResult:
    """Typed exact-Pod probe decision that preserves durable replay."""

    state: PhysicalAbsenceLoadState
    target: ExactPodAbsenceProbeTarget | None = None

    def validate(self) -> None:
        if not isinstance(self.state, PhysicalAbsenceLoadState):
            raise TypeError('state must be PhysicalAbsenceLoadState.')
        if self.state is PhysicalAbsenceLoadState.NEEDS_PROBE:
            if not isinstance(self.target, ExactPodAbsenceProbeTarget):
                raise ValueError('NEEDS_PROBE requires an exact target.')
            self.target.validate()
        elif self.target is not None:
            raise ValueError(f'{self.state.value} cannot carry a target.')


@dataclasses.dataclass(frozen=True)
class AdmissionlessPhysicalAbsenceLoadResult:
    """Typed provider-probe decision that preserves durable replay."""

    state: PhysicalAbsenceLoadState
    target: AdmissionlessPhysicalAbsenceProbeTarget | None = None

    def validate(self) -> None:
        if not isinstance(self.state, PhysicalAbsenceLoadState):
            raise TypeError('state must be PhysicalAbsenceLoadState.')
        if self.state is PhysicalAbsenceLoadState.NEEDS_PROBE:
            if not isinstance(self.target,
                              AdmissionlessPhysicalAbsenceProbeTarget):
                raise ValueError('NEEDS_PROBE requires an exact target.')
            self.target.validate()
        elif self.target is not None:
            raise ValueError(f'{self.state.value} cannot carry a target.')


@dataclasses.dataclass(frozen=True)
class _AdmissionlessTeardownGraph:
    """Validated database graph behind one admissionless provider probe."""

    target: AdmissionlessPhysicalAbsenceProbeTarget
    association: Mapping[str, Any]
    replica_info: Any
    service: Mapping[str, Any]
    provider_absence_state: PhysicalAbsenceLoadState


def _identity_predicates(identity: KueueAdmissionIdentity) -> tuple[Any, ...]:
    return (
        _ADMISSIONS.c.service_name == identity.service_name,
        _ADMISSIONS.c.unresolved_domain_sha256 ==
        identity.unresolved_domain_sha256,
        _ADMISSIONS.c.service_hash == identity.service_hash,
        _ADMISSIONS.c.service_lifecycle_epoch ==
        identity.service_lifecycle_epoch,
        _ADMISSIONS.c.service_version == identity.service_version,
        _ADMISSIONS.c.pool_key == identity.pool_key,
        _ADMISSIONS.c.pool_epoch == identity.pool_epoch,
        _ADMISSIONS.c.physical_cluster_uid == identity.physical_cluster_uid,
        _ADMISSIONS.c.kubernetes_context == identity.kubernetes_context,
        _ADMISSIONS.c.accelerator == identity.accelerator,
        _ADMISSIONS.c.accelerator_count == identity.accelerator_count,
        _ADMISSIONS.c.worker_projection_sha256 ==
        identity.worker_projection_sha256,
    )


def _intent_predicates(identity: KueueAdmissionIdentity,
                       intent_idempotency_key: str) -> tuple[Any, ...]:
    return (
        _INTENTS.c.intent_idempotency_key == intent_idempotency_key,
        _INTENTS.c.service_name == identity.service_name,
        _INTENTS.c.service_hash == identity.service_hash,
        _INTENTS.c.service_lifecycle_epoch == identity.service_lifecycle_epoch,
        _INTENTS.c.service_version == identity.service_version,
        _INTENTS.c.pool_key == identity.pool_key,
        _INTENTS.c.pool_epoch == identity.pool_epoch,
        _INTENTS.c.physical_cluster_uid == identity.physical_cluster_uid,
        _INTENTS.c.kubernetes_context == identity.kubernetes_context,
        sqlalchemy.func.lower(_INTENTS.c.accelerator) == identity.accelerator,
        _INTENTS.c.accelerator_count == identity.accelerator_count,
        _INTENTS.c.worker_projection_sha256 ==
        identity.worker_projection_sha256,
    )


def _require_postgres_connection(
        connection: sqlalchemy.engine.Connection) -> None:
    if (connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise KueueAdmissionUnavailable(
            'Kueue admission authority requires PostgreSQL.')


def _canonical_capacity(
    intent: Mapping[str, Any],
    identity: KueueAdmissionIdentity,
) -> tuple[str, int]:
    unit = intent['capacity_unit']
    planned = intent['planned_capacity']
    if (unit not in ('physical', 'logical') or not isinstance(planned, int) or
            isinstance(planned, bool) or planned <= 0 or
        (unit == 'physical' and planned != 1) or
        (unit == 'logical' and planned != identity.accelerator_count)):
        raise KueueAdmissionConflict(
            'The intent has no immutable configured-unit capacity width.')
    return str(unit), int(planned)


def _identity_from_admission(
        admission: Mapping[str, Any]) -> KueueAdmissionIdentity:
    """Reconstruct and validate the checked identity of a retained row."""
    identity = KueueAdmissionIdentity(
        service_name=admission['service_name'],
        service_hash=admission['service_hash'],
        service_lifecycle_epoch=admission['service_lifecycle_epoch'],
        service_version=admission['service_version'],
        pool_key=admission['pool_key'],
        pool_epoch=admission['pool_epoch'],
        physical_cluster_uid=admission['physical_cluster_uid'],
        kubernetes_context=admission['kubernetes_context'],
        accelerator=admission['accelerator'],
        accelerator_count=admission['accelerator_count'],
        worker_projection_sha256=admission['worker_projection_sha256'])
    try:
        identity.validate()
    except ValueError as error:
        raise KueueAdmissionConflict(
            'Provider-free admission identity is malformed.') from error
    if admission['unresolved_domain_sha256'] != (
            identity.unresolved_domain_sha256):
        raise KueueAdmissionConflict(
            'Provider-free admission domain identity is corrupt.')
    return identity


def _validate_admission_intent_identity(
    admission: Mapping[str, Any],
    intent: Mapping[str, Any],
) -> KueueAdmissionIdentity:
    """Require one admission and intent to describe the same authority."""
    identity = _identity_from_admission(admission)
    if (intent['intent_idempotency_key'] != admission['intent_idempotency_key']
            or intent['service_name'] != identity.service_name or
            intent['service_hash'] != identity.service_hash or
            intent['service_lifecycle_epoch']
            != identity.service_lifecycle_epoch or
            intent['service_version'] != identity.service_version or
            intent['pool_key'] != identity.pool_key or
            intent['pool_epoch'] != identity.pool_epoch or
            intent['physical_cluster_uid'] != identity.physical_cluster_uid or
            intent['kubernetes_context'] != identity.kubernetes_context or
            str(intent['accelerator']).casefold() != identity.accelerator or
            intent['accelerator_count'] != identity.accelerator_count or
            intent['worker_projection_sha256']
            != identity.worker_projection_sha256):
        raise KueueAdmissionConflict(
            'Provider-free admission and intent identities diverged.')
    capacity_unit, planned_capacity = _canonical_capacity(intent, identity)
    if (admission['capacity_unit'] != capacity_unit or
            admission['planned_capacity'] != planned_capacity):
        raise KueueAdmissionConflict(
            'Provider-free admission capacity identity is corrupt.')
    return identity


def validate_admission_intent_identity(
    admission: KueueAdmissionRow | Mapping[str, Any],
    intent: Mapping[str, Any],
) -> KueueAdmissionIdentity:
    """Validate one admission against every copied immutable intent field.

    Capacity accounting consumes the typed rows returned by the repository,
    while mutation paths consume SQLAlchemy mappings.  Keep both on this one
    canonical validator so a newly copied identity field cannot be omitted at
    a final paid or retirement boundary.
    """
    if isinstance(admission, KueueAdmissionRow):
        admission = {
            field.name: getattr(admission, field.name)
            for field in dataclasses.fields(KueueAdmissionRow)
        }
    if not isinstance(admission, Mapping) or not isinstance(intent, Mapping):
        raise TypeError('Kueue lineage validation requires mapped rows.')
    return _validate_admission_intent_identity(admission, intent)


def _replica_matches_reserved_fill_intent(
    info: Any,
    intent: Mapping[str, Any],
    identity: KueueAdmissionIdentity,
    intent_idempotency_key: str,
) -> bool:
    """Require the replica's immutable fill attribution, not launch receipts.

    ``launch_request_id`` and ``service_job_id`` belong exclusively to the
    system-recovery subdocument in ReplicaInfo.  The ordinary-launch
    association and Kueue admission are the durable request/job/Pod lineage for
    reserved fill, so duplicating those identities into ReplicaInfo would make
    an otherwise canonical RESERVED_FILL record undecodable.

    The intent snapshots the all-zero-cost observation high-water before
    atomic admission.  The replica owns the later sequence allocated by that
    admission, so the latter must be strictly newer rather than equal.
    """
    try:
        return bool(
            ordinary_launch_binding.classify_non_pool_launch_profile(info)
            is ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL
            and info.launch_request_id is None and
            info.service_job_id is None and
            info.reserved_fill_pool_key == identity.pool_key and
            info.reserved_fill_service_generation
            == intent['service_generation'] and
            info.reserved_fill_physical_cluster_uid
            == identity.physical_cluster_uid and
            info.reserved_fill_kubernetes_context == identity.kubernetes_context
            and info.reserved_fill_allocation_generation
            == intent['allocation_generation'] and
            info.reserved_fill_allocation_input_sha256
            == intent['allocation_input_sha256'] and
            info.reserved_fill_allocation_claim_generation
            == intent['allocation_claim_generation'] and
            info.reserved_fill_reconciliation_gate_generation
            == intent['reconciliation_gate_generation'] and
            info.reserved_fill_reclaim_fleet_bundle_sha256
            == intent['reclaim_fleet_bundle_sha256'] and
            info.reserved_fill_reclaim_policy_revision
            == intent['reclaim_policy_revision'] and
            info.reserved_fill_reclaim_provider_inventory_sha256
            == intent['reclaim_provider_inventory_sha256'] and
            info.reserved_fill_worker_projection_sha256
            == identity.worker_projection_sha256 and
            info.reserved_fill_observation_generation
            == intent['observation_generation'] and
            info.reserved_fill_observation_sequence
            == intent['observation_sequence'] and
            info.reserved_fill_intent_idempotency_key == intent_idempotency_key
            and type(info.zero_cost_admission_sequence) is int and
            type(intent['observation_sequence']) is int and
            info.zero_cost_admission_sequence > intent['observation_sequence'])
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def _terminal_request_matches_association(
    request: Mapping[str, Any],
    association: Mapping[str, Any],
) -> bool:
    """Return whether one retained request exactly copies terminal lineage."""
    try:
        return bool(
            request['request_id'] == association['request_id'] and
            request['ordinary_launch_association_id']
            == association['association_id'] and request['handler_name']
            == 'sky.server.requests.non_pool_launch:launch' and
            request['status'] in _TERMINAL_STATUSES and
            request['status'] == association['terminal_status'] and
            isinstance(request['terminal_cause'], str) and
            request['terminal_cause'] and
            request['terminal_cause'] == association['terminal_cause'] and
            request['finished_at'] is not None and
            request['execution_generation']
            == association['terminal_execution_generation'] and
            request['execution_quiescence_required'] is True and
            request['execution_quiesced_generation']
            == association['execution_quiesced_generation'] and
            request['execution_quiesced_at']
            == association['execution_quiesced_at'] and
            all(request[field] == association[field]
                for field in _TERMINAL_PROFILE_FIELDS))
    except (KeyError, TypeError):
        return False


def _is_provider_absent_pre_job_association(
        association: Mapping[str, Any]) -> bool:
    """Whether one settled association is the closed pre-job ABSENT shape."""
    return bool(
        association['resolution'] == 'PROJECTED' and
        association['reconciliation_outcome'] == 'PROJECTED' and
        association['effect_phase'] in {'NOT_STARTED', 'PROVIDER_IO'} and
        association['service_job_id'] is None and
        association['provider_evidence'] == 'ABSENT')


def _validate_provider_absent_pre_job_admission(
    admission: Mapping[str, Any],
    intent: Mapping[str, Any],
    identity: KueueAdmissionIdentity,
    association: Mapping[str, Any],
    *,
    replica_id: int,
    replica_record_id: uuid.UUID,
) -> None:
    """Accept only an exact materialized admission that never reached a Pod."""
    empty_fields = ('pod_namespace', 'pod_name', 'pod_uid', 'pod_receipt',
                    'pod_receipt_sha256', 'observed_at', 'valid_until',
                    'admitted_at')
    checked_identity = _validate_admission_intent_identity(admission, intent)
    if (checked_identity != identity or
            admission['state'] != KueueAdmissionState.INTENT_PENDING.value or
            admission['replica_id'] != replica_id or
            admission['replica_record_id'] != replica_record_id or
            admission['provider_cluster_generation']
            != association['launch_generation'] or
            admission['association_id'] != association['association_id'] or
            admission['replacement_surge_units'] != 0 or
            admission['replacement_compatibility_sha256'] is not None or
            any(admission[field] is not None for field in empty_fields)):
        raise KueueAdmissionConflict(
            'Provider-absent pre-job teardown found live or mismatched '
            'admission authority.')


def _validate_provider_absent_pre_job_replica(replica_info: Any) -> None:
    """Reject process state that contradicts a terminal pre-job tombstone."""
    try:
        terminal = bool(
            replica_info.zero_cost_materialization_sequence is None and
            replica_info.status_property.service_ready_now is False and
            replica_info.status in {
                serve_state.ReplicaStatus.SHUTTING_DOWN,
                serve_state.ReplicaStatus.FAILED_CLEANUP,
            })
    except (AttributeError, TypeError, ValueError):
        terminal = False
    if not terminal:
        raise KueueAdmissionConflict(
            'Provider-absent pre-job teardown has live or materialized '
            'replica state.')


def _materialized_graph_predicate(
    identity: KueueAdmissionIdentity,
    intent_idempotency_key: str,
    replica_id: int,
    replica_record_id: uuid.UUID,
    provider_cluster_generation: int,
    association_id: uuid.UUID,
) -> Any:
    association = (ordinary_launch_binding.ordinary_launch_associations_table.
                   alias('kueue_admission_association'))
    replica = _REPLICAS.alias('kueue_admission_replica')
    return sqlalchemy.exists(
        sqlalchemy.select(sqlalchemy.literal(1)).select_from(
            replica.join(
                association,
                sqlalchemy.and_(
                    replica.c.ordinary_launch_association_id ==
                    association.c.association_id,
                    replica.c.service_name == association.c.service_name,
                    replica.c.replica_id == association.c.replica_id))).
        where(
            replica.c.service_name == identity.service_name,
            replica.c.replica_id == replica_id,
            replica.c.version == identity.service_version,
            replica.c.reserved_fill_intent_idempotency_key ==
            intent_idempotency_key,
            replica.c.replica_state['replica_record_id'].as_string() == str(
                replica_record_id),
            association.c.association_id == association_id,
            association.c.service_name == identity.service_name,
            association.c.service_hash == identity.service_hash,
            association.c.service_lifecycle_epoch ==
            identity.service_lifecycle_epoch,
            association.c.service_version == identity.service_version,
            association.c.replica_id == replica_id,
            association.c.replica_record_id == replica_record_id,
            association.c.launch_generation == provider_cluster_generation,
            association.c.binding_protocol_version == 2,
            association.c.profile_kind == 'RESERVED_FILL',
            association.c.authorization_kind == 'RESERVED_FILL_ALLOCATION',
            association.c.authorization_reference ==
            f'reserved-fill:{intent_idempotency_key}',
            association.c.resolution == 'BOUND'))


def _receipt_expression(receipt: Mapping[str, Any]) -> tuple[Any, Any]:
    value = sqlalchemy.literal(copy.deepcopy(dict(receipt)),
                               type_=postgresql.JSONB)
    digest = sqlalchemy.func.encode(
        sqlalchemy.func.sha256(
            sqlalchemy.func.convert_to(sqlalchemy.cast(value, sqlalchemy.Text),
                                       'UTF8')), 'hex')
    return value, digest


def _validate_receipt(
    receipt: Mapping[str, Any],
    *,
    state: KueueAdmissionState,
    identity: KueueAdmissionIdentity,
    intent_idempotency_key: str,
    replica_record_id: uuid.UUID,
    pod_namespace: str,
    pod_name: str,
    pod_uid: str,
) -> None:
    """Validate the classifier's closed receipt at the SQL boundary."""
    if not isinstance(receipt, Mapping):
        raise ValueError('pod_receipt must be a mapping.')
    top_keys = {
        'schema_version', 'state', 'pod', 'skypilot', 'kueue', 'priority',
        'scheduler_name', 'service_account_name', 'accelerator'
    }
    if set(receipt) != top_keys or receipt.get('schema_version') != 1 or \
            receipt.get('state') != state.value:
        raise ValueError('pod_receipt is not the closed admission schema.')
    pod = receipt.get('pod')
    skypilot = receipt.get('skypilot')
    accelerator = receipt.get('accelerator')
    if (not isinstance(pod, Mapping) or not isinstance(skypilot, Mapping) or
            not isinstance(accelerator, Mapping) or
            pod.get('namespace') != pod_namespace or
            pod.get('name') != pod_name or pod.get('uid') != pod_uid or
            skypilot.get('intent_key') != intent_idempotency_key or
            skypilot.get('replica_record_uuid') != str(replica_record_id) or
            skypilot.get('pool_physical_uid') != identity.physical_cluster_uid
            or skypilot.get('worker_projection_sha256')
            != identity.worker_projection_sha256 or
            accelerator.get('name', '').casefold() != identity.accelerator or
            accelerator.get('count') != identity.accelerator_count):
        raise KueueAdmissionConflict(
            'Pod receipt changed its exact admission identity.')


def _admitted_receipt_refresh_allowed(old_receipt: Mapping[str, Any],
                                      new_receipt: Mapping[str, Any]) -> bool:
    """Allow only the documented Pending-to-Running admitted audit refresh."""
    old_pod = old_receipt.get('pod')
    new_pod = new_receipt.get('pod')
    old_kueue = old_receipt.get('kueue')
    new_kueue = new_receipt.get('kueue')
    if (not isinstance(old_pod, Mapping) or not isinstance(new_pod, Mapping) or
            not isinstance(old_kueue, Mapping) or
            not isinstance(new_kueue, Mapping)):
        return False
    old_phase = old_pod.get('phase')
    new_phase = new_pod.get('phase')
    if (old_phase not in ('Pending', 'Running') or
            new_phase not in ('Pending', 'Running') or
        (old_phase == 'Running' and new_phase != 'Running')):
        return False
    group_name = old_kueue.get('pod_group_name')
    old_workload = old_kueue.get('workload_name')
    new_workload = new_kueue.get('workload_name')
    old_topology = old_kueue.get('unconstrained_topology')
    new_topology = new_kueue.get('unconstrained_topology')
    if (not isinstance(group_name, str) or not group_name or
            new_kueue.get('pod_group_name') != group_name or
            old_workload not in (None, group_name) or
            new_workload not in (None, group_name) or
        (old_workload is not None and new_workload != old_workload) or
            old_topology not in (None, 'true') or
            new_topology not in (None, 'true') or
        (old_topology is not None and new_topology != old_topology)):
        return False
    old_immutable = copy.deepcopy(dict(old_receipt))
    new_immutable = copy.deepcopy(dict(new_receipt))
    for value in (old_immutable, new_immutable):
        value['pod'].pop('phase')
        value['kueue'].pop('workload_name')
        value['kueue'].pop('unconstrained_topology')
    return old_immutable == new_immutable


def _validate_provider_read_started_at(
    connection: sqlalchemy.engine.Connection,
    provider_read_started_at: datetime.datetime,
) -> tuple[datetime.datetime, datetime.datetime]:
    """Fence provider evidence to the database time sampled before its read."""
    if (not isinstance(provider_read_started_at, datetime.datetime) or
            provider_read_started_at.tzinfo is None or
            provider_read_started_at.utcoffset() is None):
        raise ValueError('provider_read_started_at must be timezone-aware.')
    valid_until = provider_read_started_at + datetime.timedelta(
        seconds=WAITING_OBSERVATION_TTL_SECONDS)
    database_now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    if provider_read_started_at > database_now:
        raise KueueAdmissionConflict(
            'Provider-read token is later than PostgreSQL time.')
    if database_now >= valid_until:
        raise KueueAdmissionConflict(
            'Provider evidence expired while waiting for database authority.')
    return provider_read_started_at, valid_until


def _validate_provider_publication_completed_at(
    connection: sqlalchemy.engine.Connection,
    valid_until: datetime.datetime,
) -> None:
    """Require the provider-derived publication statement to beat its TTL."""
    database_now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    if database_now >= valid_until:
        raise KueueAdmissionConflict(
            'Provider evidence expired during database publication.')


def _canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload,
                           sort_keys=True,
                           separators=(',', ':'),
                           ensure_ascii=False,
                           allow_nan=False).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()


def _exact_pod_absence_evidence(
    association: Mapping[str, Any],
    target: ExactPodAbsenceProbeTarget,
) -> tuple[dict[str, Any], str]:
    payload = {
        'schema_version': 1,
        'probe_contract': 'kueue-exact-pod-absence-v1',
        'association_id': str(target.association_id),
        'replica_record_id': str(target.replica_record_id),
        'physical_cluster_uid': target.identity.physical_cluster_uid,
        'kubernetes_context': target.identity.kubernetes_context,
        'pod': {
            'namespace': target.pod_namespace,
            'name': target.pod_name,
            'uid': target.pod_uid,
        },
        'result': 'ABSENT',
    }
    digest = _canonical_json_sha256({
        'association_id': str(target.association_id),
        'evidence': 'ABSENT',
        'payload': payload,
        'profile_digest': association['profile_digest'],
    })
    return payload, digest


def _provider_absence_publication_values(
    service: Mapping[str, Any],
    association: Mapping[str, Any],
    *,
    observed_at: datetime.datetime,
    payload: Mapping[str, Any],
    digest: str,
) -> dict[str, Any]:
    """Build one evidence write that also adopts the current service owner."""
    owner_incarnation = service.get('controller_incarnation')
    owner_epoch = service.get('controller_owner_epoch')
    if (not isinstance(owner_incarnation, uuid.UUID) or
            type(owner_epoch) is not int or owner_epoch < 1 or
            service.get('ordinary_launch_binding_mode') != 'bound' or
            service.get('ordinary_launch_binding_capable') is not True):
        raise KueueAdmissionConflict(
            'Provider absence lost the current bound service owner.')
    owner_changed = (association['owner_controller_incarnation']
                     != owner_incarnation or
                     association['owner_controller_epoch'] != owner_epoch)
    values: dict[str, Any] = {
        'provider_evidence': 'ABSENT',
        'provider_evidence_observed_at': observed_at,
        'provider_evidence_payload': dict(payload),
        'provider_evidence_digest': digest,
        'owner_controller_incarnation': owner_incarnation,
        'owner_controller_epoch': owner_epoch,
        'owner_revision': int(association['owner_revision']) + 1,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    if owner_changed:
        values['owner_transferred_at'] = sqlalchemy.func.clock_timestamp()
    return values


def _validate_admissionless_retirement_rows_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    lifecycle_epoch: int,
    service: Mapping[str, Any],
    intent: Mapping[str, Any],
    identity: KueueAdmissionIdentity,
    intent_idempotency_key: str,
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    request: Mapping[str, Any] | None,
    replica_id: int,
    replica_record_id: uuid.UUID,
) -> _AdmissionlessTeardownGraph:
    """Validate the shared rows of one admissionless retirement graph.

    This acquires no rows or locks and performs no writes or provider I/O.  The
    single-replica probe and sorted multi-replica retirement transaction own
    those operations, then use this validator so provider publication and
    final deletion cannot drift on the durable row contract.  Its frozen-profile
    authentication may perform read-only PostgreSQL lookups.  Association
    effect-phase shape remains a caller concern: a post-job graph can require a
    new provider probe, while an older pre-job graph may already own a
    provider-absence tombstone.
    """
    _require_postgres_connection(connection)
    if not all(
            isinstance(row, Mapping)
            for row in (service, intent, replica, association)):
        raise TypeError('Admissionless retirement requires mapped rows.')
    if request is not None and not isinstance(request, Mapping):
        raise TypeError('Admissionless retirement request must be mapped.')
    _require_positive(lifecycle_epoch, 'lifecycle_epoch')
    _require_positive(replica_id, 'replica_id')
    if not isinstance(replica_record_id, uuid.UUID):
        raise ValueError('replica_record_id must be a UUID.')
    identity.validate()
    intent_idempotency_key = _require_sha256(intent_idempotency_key,
                                             'intent_idempotency_key')

    if service['status'] not in ('SHUTTING_DOWN', 'FAILED_CLEANUP'):
        raise KueueAdmissionConflict(
            'A Kueue-bound protocol-v2 replica lost its admission outside '
            'whole-service teardown.')
    if (lifecycle_epoch != identity.service_lifecycle_epoch or
            service['name'] != identity.service_name or
            service['hash'] != identity.service_hash or
            service['lifecycle_epoch'] != identity.service_lifecycle_epoch or
            intent['intent_idempotency_key'] != intent_idempotency_key or
            intent['service_name'] != identity.service_name or
            intent['service_hash'] != identity.service_hash or
            intent['service_lifecycle_epoch']
            != identity.service_lifecycle_epoch or
            intent['service_version'] != identity.service_version or
            intent['state'] != 'COMMITTED' or
            intent['replica_id'] != replica_id or
            intent['replica_record_id'] != replica_record_id):
        raise KueueAdmissionConflict(
            'Admissionless teardown lost its exact committed handoff.')

    state = replica['replica_state']
    try:
        info = serve_state.decode_replica_state_for_authority(
            replica['replica_state_version'], state)
    except (AttributeError, KeyError, RuntimeError, TypeError,
            ValueError) as error:
        raise KueueAdmissionConflict(
            'Admissionless teardown found malformed replica authority.'
        ) from error
    if (not isinstance(state, Mapping) or
            replica['service_name'] != identity.service_name or
            replica['replica_id'] != replica_id or
            replica['version'] != identity.service_version or
            state.get('replica_record_id') != str(replica_record_id) or
            state.get('reserved_fill_intent_idempotency_key')
            != intent_idempotency_key or
            replica['reserved_fill_intent_idempotency_key']
            != intent_idempotency_key or
            replica['ordinary_launch_association_id'] is not None or
            info.replica_id != replica_id or
            info.replica_record_id != str(replica_record_id) or
            info.version != identity.service_version or
            info.reserved_fill is not True or info.is_zero_cost is not True or
            info.paid_capacity_pool_key is not None or
            not _replica_matches_reserved_fill_intent(info, intent, identity,
                                                      intent_idempotency_key)):
        raise KueueAdmissionConflict(
            'Admissionless teardown replica is not an exact zero-cost '
            'materialization.')

    try:
        (ordinary_launch_binding.
         validate_reserved_fill_cleanup_association_in_connection)(connection,
                                                                   service,
                                                                   replica,
                                                                   association)
    except (ordinary_launch_binding.OrdinaryLaunchBindingConflict, TypeError,
            ValueError) as error:
        raise KueueAdmissionConflict(
            'Admissionless teardown lost its frozen reserved-fill profile.'
        ) from error
    provider_generation = association['launch_generation']
    if (association['service_name'] != identity.service_name or
            association['service_workspace'] != service['workspace'] or
            association['service_hash'] != identity.service_hash or
            association['service_lifecycle_epoch']
            != identity.service_lifecycle_epoch or
            association['service_version'] != identity.service_version or
            association['replica_id'] != replica_id or
            association['replica_record_id'] != replica_record_id or
            association['binding_protocol_version'] != 2 or
            association['authorization_reference']
            != f'reserved-fill:{intent_idempotency_key}' or
            association['profile_kind'] != 'RESERVED_FILL' or
            association['paid_capacity_pool_key'] is not None or
            association['cluster_name'] != info.cluster_name or
            type(provider_generation) is not int or provider_generation < 1 or
            association['terminal_status'] not in _TERMINAL_STATUSES or
            not isinstance(association['terminal_cause'], str) or
            not association['terminal_cause'] or
            type(association['terminal_execution_generation']) is not int or
            association['terminal_execution_generation'] < 1 or
            association['execution_quiescence_required'] is not True or
            association['execution_quiesced_generation']
            != association['terminal_execution_generation'] or
            association['execution_quiesced_at'] is None or
            association['projected_at'] is None or
            association['pin_released_at'] is None or
            association['tombstone_not_before'] is None):
        raise KueueAdmissionConflict(
            'Admissionless teardown launch is not exact, terminal, and '
            'quiescent.')

    request_id = association['request_id']
    if not isinstance(request_id, str) or not request_id:
        raise KueueAdmissionConflict(
            'Admissionless teardown lost its launch request identity.')
    if (request is not None and
            not _terminal_request_matches_association(request, association)):
        raise KueueAdmissionConflict(
            'Admissionless teardown request receipt is not exact.')

    target = AdmissionlessPhysicalAbsenceProbeTarget(
        identity=identity,
        intent_idempotency_key=intent_idempotency_key,
        replica_id=replica_id,
        replica_record_id=replica_record_id,
        provider_cluster_generation=provider_generation,
        association_id=association['association_id'],
        cluster_name=info.cluster_name)
    target.validate()
    evidence = association['provider_evidence']
    if evidence == 'NOT_QUERIED':
        if (association['provider_evidence_observed_at'] is not None or
                association['provider_evidence_payload'] is not None or
                association['provider_evidence_digest'] is not None):
            raise KueueAdmissionConflict(
                'Admissionless teardown requires fresh provider absence.')
        absence_state = PhysicalAbsenceLoadState.NEEDS_PROBE
    elif evidence == 'ABSENT':
        expected_payload, expected_digest = (
            ordinary_launch_binding._reserved_fill_provider_evidence(  # pylint: disable=protected-access
                association, info,
                ordinary_launch_binding.ProviderEvidence.ABSENT))
        if (association['provider_evidence_observed_at'] is None or
                association['provider_evidence_observed_at']
                < association['execution_quiesced_at'] or
                association['provider_evidence_payload'] != expected_payload or
                association['provider_evidence_digest'] != expected_digest):
            raise KueueAdmissionConflict(
                'Admissionless teardown retained noncanonical absence.')
        absence_state = PhysicalAbsenceLoadState.ALREADY_PROVEN
    else:
        raise KueueAdmissionConflict(
            'Admissionless teardown requires fresh provider absence.')
    return _AdmissionlessTeardownGraph(target=target,
                                       association=dict(association),
                                       replica_info=info,
                                       service=dict(service),
                                       provider_absence_state=absence_state)


class KueueAdmissionRepository:
    """Transactional owner of durable Kueue admission facts."""

    def __init__(self, engine: sqlalchemy.engine.Engine | None = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        engine = self._engine or serve_state_schema.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise KueueAdmissionUnavailable(
                'Kueue admission authority requires PostgreSQL.')
        return engine

    @staticmethod
    def _lock_service_owner(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        service_hash: str,
    ) -> None:
        owner = connection.execute(
            sqlalchemy.select(_SERVICES.c.hash).where(
                _SERVICES.c.name ==
                service_name).with_for_update()).scalar_one_or_none()
        if owner != service_hash:
            raise KueueAdmissionConflict(
                'Kueue admission lost its exact service incarnation.')

    def lock_service_admissions_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        service_hash: str,
    ) -> tuple[KueueAdmissionRow, ...]:
        """Lock the service gap and every admission in canonical order."""
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_nonempty(service_hash, 'service_hash')
        self._lock_service_owner(connection, service_name, service_hash)
        rows = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                _ADMISSIONS.c.service_name == service_name).order_by(
                    _ADMISSIONS.c.unresolved_domain_sha256, _ADMISSIONS.c.
                    intent_idempotency_key).with_for_update()).mappings().all()
        if any(row['service_hash'] != service_hash for row in rows):
            raise KueueAdmissionConflict(
                'A retained admission belongs to another service lifecycle.')
        return tuple(KueueAdmissionRow.from_mapping(row) for row in rows)

    def lock_outgoing_update_holds_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        service_hash: str,
        incoming_version: int,
    ) -> tuple[KueueAdmissionRow, ...]:
        """Lock unresolved admissions owned by versions being replaced."""
        _require_positive(incoming_version, 'incoming_version')
        rows = self.lock_service_admissions_in_connection(
            connection, service_name, service_hash)
        unresolved = frozenset({
            KueueAdmissionState.INTENT_PENDING,
            KueueAdmissionState.POD_WAITING,
        })
        return tuple(row for row in rows
                     if row.service_version < incoming_version and
                     row.state in unresolved)

    def get_for_intent_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        intent_idempotency_key: str,
        *,
        for_update: bool = False,
    ) -> KueueAdmissionRow | None:
        """Read the one admission owned by an exact intent."""
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_sha256(intent_idempotency_key, 'intent_idempotency_key')
        statement = sqlalchemy.select(_ADMISSIONS).where(
            _ADMISSIONS.c.service_name == service_name,
            _ADMISSIONS.c.intent_idempotency_key == intent_idempotency_key)
        if for_update:
            statement = statement.with_for_update()
        row = connection.execute(statement).mappings().one_or_none()
        return None if row is None else KueueAdmissionRow.from_mapping(row)

    def load_exact_pod_absence_probe_target_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        service_name: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
    ) -> ExactPodAbsenceLoadResult:
        """Load one exact admitted Pod probe decision without SQL locks."""
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_positive(replica_id, 'replica_id')
        if not isinstance(replica_record_id, uuid.UUID):
            raise ValueError('replica_record_id must be a UUID.')
        admission_rows = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                _ADMISSIONS.c.service_name == service_name,
                _ADMISSIONS.c.replica_id == replica_id,
                _ADMISSIONS.c.replica_record_id == replica_record_id).order_by(
                    _ADMISSIONS.c.intent_idempotency_key)).mappings().all()
        if not admission_rows:
            result = ExactPodAbsenceLoadResult(
                PhysicalAbsenceLoadState.NOT_APPLICABLE)
            result.validate()
            return result
        if len(admission_rows) != 1:
            raise KueueAdmissionConflict(
                'Replica owns multiple Kueue admission identities.')
        admission = admission_rows[0]
        if admission['state'] == KueueAdmissionState.INTENT_PENDING.value:
            # A pending admission has no Pod identity to observe.  The
            # provider-free loader below revalidates the complete graph and
            # accepts only a terminal pre-job ABSENT tombstone.
            result = ExactPodAbsenceLoadResult(
                PhysicalAbsenceLoadState.NOT_APPLICABLE)
            result.validate()
            return result
        identity = _identity_from_admission(admission)
        intent_key = str(admission['intent_idempotency_key'])
        intent = connection.execute(
            sqlalchemy.select(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key ==
                intent_key)).mappings().one_or_none()
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table.c.epoch).
            where(serve_state_schema.service_lifecycle_fences_table.c.name ==
                  service_name)).scalar_one_or_none()
        service = connection.execute(
            sqlalchemy.select(_SERVICES).where(
                _SERVICES.c.name == service_name)).mappings().one_or_none()
        replica = connection.execute(
            sqlalchemy.select(_REPLICAS).where(
                _REPLICAS.c.service_name == service_name,
                _REPLICAS.c.replica_id == replica_id)).mappings().one_or_none()
        associations = ordinary_launch_binding.ordinary_launch_associations_table
        association_id = admission['association_id']
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one_or_none()
        if (intent is None or service is None or replica is None or
                association is None or
                lifecycle != identity.service_lifecycle_epoch or
                service['hash'] != identity.service_hash or
                service['lifecycle_epoch'] != identity.service_lifecycle_epoch):
            raise KueueAdmissionConflict(
                'Exact Pod absence target lost its service graph.')
        checked_identity = _validate_admission_intent_identity(
            admission, intent)
        state = replica['replica_state']
        pod_namespace = admission['pod_namespace']
        pod_name = admission['pod_name']
        pod_uid = admission['pod_uid']
        receipt = admission['pod_receipt']
        receipt_digest = admission['pod_receipt_sha256']
        if (checked_identity != identity or admission['state']
                != KueueAdmissionState.POLICY_ADMITTED.value or
                intent['state'] != 'COMMITTED' or
                intent['replica_id'] != replica_id or
                intent['replica_record_id'] != replica_record_id or
                not isinstance(state, Mapping) or
                state.get('replica_record_id') != str(replica_record_id) or
                state.get('reserved_fill_intent_idempotency_key') != intent_key
                or
                replica['reserved_fill_intent_idempotency_key'] != intent_key or
                replica['ordinary_launch_association_id'] is not None or
                association['service_name'] != identity.service_name or
                association['service_hash'] != identity.service_hash or
                association['service_lifecycle_epoch']
                != identity.service_lifecycle_epoch or
                association['service_version'] != identity.service_version or
                association['replica_id'] != replica_id or
                association['replica_record_id'] != replica_record_id or
                association['association_id'] != association_id or
                association['launch_generation']
                != admission['provider_cluster_generation'] or
                association['resolution'] != 'PROJECTED' or
                association['reconciliation_outcome'] != 'PROJECTED' or
                association['effect_phase'] != 'SERVICE_JOB_RECORDED' or
                not isinstance(association['service_job_id'], int) or
                isinstance(association['service_job_id'], bool) or
                association['service_job_id'] < 1 or not all(
                    isinstance(value, str) and value
                    for value in (pod_namespace, pod_name, pod_uid)) or
                not isinstance(receipt, Mapping) or
                not isinstance(receipt_digest, str)):
            raise KueueAdmissionConflict(
                'Exact Pod absence target is not a normal admitted launch.')
        _validate_receipt(receipt,
                          state=KueueAdmissionState.POLICY_ADMITTED,
                          identity=identity,
                          intent_idempotency_key=intent_key,
                          replica_record_id=replica_record_id,
                          pod_namespace=pod_namespace,
                          pod_name=pod_name,
                          pod_uid=pod_uid)
        _, digest_expression = _receipt_expression(receipt)
        canonical_receipt_digest = connection.execute(
            sqlalchemy.select(digest_expression)).scalar_one()
        if receipt_digest != canonical_receipt_digest:
            raise KueueAdmissionConflict(
                'Exact Pod absence target receipt digest is not canonical.')
        target = ExactPodAbsenceProbeTarget(
            identity=identity,
            intent_idempotency_key=intent_key,
            replica_id=replica_id,
            replica_record_id=replica_record_id,
            provider_cluster_generation=int(
                admission['provider_cluster_generation']),
            association_id=association_id,
            pod_namespace=pod_namespace,
            pod_name=pod_name,
            pod_uid=pod_uid)
        target.validate()
        evidence = association['provider_evidence']
        if evidence == 'ABSENT':
            expected_payload, expected_digest = _exact_pod_absence_evidence(
                association, target)
            if (association['execution_quiesced_at'] is None or
                    association['provider_evidence_observed_at'] is None or
                    association['provider_evidence_observed_at']
                    < association['execution_quiesced_at'] or
                    association['provider_evidence_payload'] != expected_payload
                    or
                    association['provider_evidence_digest'] != expected_digest):
                raise KueueAdmissionConflict(
                    'Exact Pod teardown retained noncanonical absence.')
            result = ExactPodAbsenceLoadResult(
                PhysicalAbsenceLoadState.ALREADY_PROVEN)
            result.validate()
            return result
        if (evidence != 'NOT_QUERIED' or
                association['provider_evidence_observed_at'] is not None or
                association['provider_evidence_payload'] is not None or
                association['provider_evidence_digest'] is not None):
            raise KueueAdmissionConflict(
                'Exact Pod teardown requires fresh provider absence.')
        result = ExactPodAbsenceLoadResult(PhysicalAbsenceLoadState.NEEDS_PROBE,
                                           target)
        result.validate()
        return result

    def _load_admissionless_teardown_graph_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        service_name: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
        for_update: bool,
    ) -> _AdmissionlessTeardownGraph | None:
        """Validate one provider-free missing/never-admitted teardown graph."""
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_positive(replica_id, 'replica_id')
        if not isinstance(replica_record_id, uuid.UUID):
            raise ValueError('replica_record_id must be a UUID.')

        def _locked(statement: Any) -> Any:
            return statement.with_for_update() if for_update else statement

        # Discover only the immutable intent edge before taking locks. The
        # locked read below rejects any replacement or projection change.
        replica_discovery = connection.execute(
            sqlalchemy.select(_REPLICAS).where(
                _REPLICAS.c.service_name == service_name,
                _REPLICAS.c.replica_id == replica_id)).mappings().one_or_none()
        if replica_discovery is None:
            return None
        discovery_state = replica_discovery['replica_state']
        if not isinstance(discovery_state, Mapping):
            raise KueueAdmissionConflict(
                'Admissionless teardown found malformed replica state.')
        intent_key = replica_discovery['reserved_fill_intent_idempotency_key']
        projected_key = discovery_state.get(
            'reserved_fill_intent_idempotency_key')
        if intent_key is None:
            if projected_key is not None:
                raise KueueAdmissionConflict(
                    'Admissionless teardown lost its normalized intent edge.')
            return None
        try:
            intent_key = _require_sha256(intent_key, 'intent_idempotency_key')
        except ValueError as error:
            raise KueueAdmissionConflict(
                'Admissionless teardown has a malformed intent edge.'
            ) from error
        if (projected_key != intent_key or
                discovery_state.get('replica_record_id')
                != str(replica_record_id)):
            raise KueueAdmissionConflict(
                'Admissionless teardown found a replaced replica identity.')

        lifecycle = connection.execute(
            _locked(
                sqlalchemy.select(
                    serve_state_schema.service_lifecycle_fences_table).where(
                        serve_state_schema.service_lifecycle_fences_table.c.name
                        == service_name))).mappings().one_or_none()
        service = connection.execute(
            _locked(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name ==
                    service_name))).mappings().one_or_none()
        intent = connection.execute(
            _locked(
                sqlalchemy.select(_INTENTS).where(
                    _INTENTS.c.intent_idempotency_key ==
                    intent_key))).mappings().one_or_none()
        replica = connection.execute(
            _locked(
                sqlalchemy.select(_REPLICAS).where(
                    _REPLICAS.c.service_name == service_name,
                    _REPLICAS.c.replica_id ==
                    replica_id))).mappings().one_or_none()
        if lifecycle is None or service is None or intent is None or replica is None:
            raise KueueAdmissionConflict(
                'Admissionless teardown lost its service or intent graph.')
        try:
            identity = (
                zero_cost_actuation.
                kueue_admission_identity_for_locked_intent_in_connection(
                    connection, intent, require_current_protocol=False))
        except Exception as error:  # pylint: disable=broad-except
            raise KueueAdmissionConflict(
                'Admissionless teardown cannot prove immutable Kueue '
                'identity.') from error
        if identity is None:
            return None
        identity.validate()

        associations = ordinary_launch_binding.ordinary_launch_associations_table
        association_rows = connection.execute(
            _locked(
                sqlalchemy.select(associations).where(
                    associations.c.service_name == service_name,
                    associations.c.replica_id == replica_id,
                    associations.c.replica_record_id == replica_record_id,
                    associations.c.binding_protocol_version == 2).order_by(
                        associations.c.association_id))).mappings().all()
        if len(association_rows) != 1:
            raise KueueAdmissionConflict(
                'Admissionless teardown requires one exact launch association.')
        association = association_rows[0]
        materialized_launch = bool(
            association['resolution'] == 'PROJECTED' and
            association['reconciliation_outcome'] == 'PROJECTED' and
            association['effect_phase'] == 'SERVICE_JOB_RECORDED' and
            type(association['service_job_id']) is int and
            association['service_job_id'] > 0)
        provider_absent_pre_job = _is_provider_absent_pre_job_association(
            association)
        if not materialized_launch and not provider_absent_pre_job:
            raise KueueAdmissionConflict(
                'Admissionless teardown is neither a materialized service '
                'launch nor a provider-absent pre-job launch.')
        if provider_absent_pre_job:
            try:
                association, _ = (
                    ordinary_launch_binding.
                    projected_provider_absence_retirement_authority_in_connection(
                        connection, service_name, replica_id,
                        str(replica_record_id)))
            except Exception as error:  # pylint: disable=broad-except
                raise KueueAdmissionConflict(
                    'Admissionless pre-job teardown lacks canonical '
                    'provider-absence authority.') from error

        paid_claims = serve_state_schema.paid_capacity_claims_table
        claim_rows = connection.execute(
            _locked(
                sqlalchemy.select(paid_claims).where(
                    paid_claims.c.service_name == service_name,
                    paid_claims.c.replica_id == replica_id).order_by(
                        paid_claims.c.pool_key))).mappings().all()
        requests = request_postgres_schema.REQUESTS
        request_rows = connection.execute(
            _locked(
                sqlalchemy.select(requests).where(
                    sqlalchemy.or_(
                        requests.c.request_id == association['request_id'],
                        requests.c.ordinary_launch_association_id ==
                        association['association_id'])).order_by(
                            requests.c.request_id))).mappings().all()
        if len(request_rows) > 1:
            raise KueueAdmissionConflict(
                'Admissionless teardown found multiple request receipts.')
        request = request_rows[0] if request_rows else None
        queue = request_postgres_schema.QUEUE
        queue_rows = connection.execute(
            _locked(
                sqlalchemy.select(queue).where(
                    queue.c.request_id == association['request_id']).order_by(
                        queue.c.request_id))).mappings().all()
        pins = request_postgres_schema.REQUEST_RETENTION_PINS
        pin_rows = connection.execute(
            _locked(
                sqlalchemy.select(pins).where(
                    sqlalchemy.or_(
                        pins.c.request_id == association['request_id'],
                        pins.c.pin_id ==
                        association['association_id'])).order_by(
                            pins.c.pin_kind, pins.c.pin_id))).mappings().all()
        admission_rows = connection.execute(
            _locked(
                sqlalchemy.select(_ADMISSIONS).where(
                    sqlalchemy.or_(
                        _ADMISSIONS.c.intent_idempotency_key == intent_key,
                        sqlalchemy.and_(
                            _ADMISSIONS.c.service_name == service_name,
                            _ADMISSIONS.c.replica_id == replica_id))).order_by(
                                _ADMISSIONS.c.intent_idempotency_key))
        ).mappings().all()
        if admission_rows:
            if len(admission_rows) != 1 or not provider_absent_pre_job:
                raise KueueAdmissionConflict(
                    'Admissionless teardown found materialized admission '
                    'authority.')
            _validate_provider_absent_pre_job_admission(
                admission_rows[0],
                intent,
                identity,
                association,
                replica_id=replica_id,
                replica_record_id=replica_record_id)
        if claim_rows or queue_rows or pin_rows:
            raise KueueAdmissionConflict(
                'Admissionless teardown found paid, queued, or pinned '
                'authority.')
        graph = _validate_admissionless_retirement_rows_in_connection(
            connection,
            lifecycle_epoch=lifecycle['epoch'],
            service=service,
            intent=intent,
            identity=identity,
            intent_idempotency_key=intent_key,
            replica=replica,
            association=association,
            request=request,
            replica_id=replica_id,
            replica_record_id=replica_record_id)
        if (provider_absent_pre_job and graph.provider_absence_state
                is not PhysicalAbsenceLoadState.ALREADY_PROVEN):
            raise KueueAdmissionConflict(
                'Admissionless pre-job teardown requires canonical provider '
                'absence.')
        if provider_absent_pre_job:
            _validate_provider_absent_pre_job_replica(graph.replica_info)
        return graph

    def load_admissionless_physical_absence_probe_target_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        service_name: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
    ) -> AdmissionlessPhysicalAbsenceLoadResult:
        """Load a typed provider decision for provider-free teardown."""
        graph = self._load_admissionless_teardown_graph_in_connection(
            connection,
            service_name=service_name,
            replica_id=replica_id,
            replica_record_id=replica_record_id,
            for_update=False)
        if graph is None:
            result = AdmissionlessPhysicalAbsenceLoadResult(
                PhysicalAbsenceLoadState.NOT_APPLICABLE)
            result.validate()
            return result
        target = (graph.target if graph.provider_absence_state
                  is PhysicalAbsenceLoadState.NEEDS_PROBE else None)
        result = AdmissionlessPhysicalAbsenceLoadResult(
            graph.provider_absence_state, target)
        result.validate()
        return result

    def record_admissionless_physical_absence_after_teardown_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        target: AdmissionlessPhysicalAbsenceProbeTarget,
        *,
        provider_read_started_at: datetime.datetime,
    ) -> Mapping[str, Any]:
        """Stamp canonical physical absence for one missing admission graph."""
        _require_postgres_connection(connection)
        if not isinstance(target, AdmissionlessPhysicalAbsenceProbeTarget):
            raise TypeError(
                'target must be AdmissionlessPhysicalAbsenceProbeTarget.')
        target.validate()
        graph = self._load_admissionless_teardown_graph_in_connection(
            connection,
            service_name=target.identity.service_name,
            replica_id=target.replica_id,
            replica_record_id=target.replica_record_id,
            for_update=True)
        if graph is None or graph.target != target:
            raise KueueAdmissionConflict(
                'Admissionless teardown target changed during provider read.')
        association = graph.association
        observed_at, valid_until = _validate_provider_read_started_at(
            connection, provider_read_started_at)
        if observed_at < association['execution_quiesced_at']:
            raise KueueAdmissionConflict(
                'Admissionless provider absence predates execution quiescence.')
        expected_payload, expected_digest = (
            ordinary_launch_binding._reserved_fill_provider_evidence(  # pylint: disable=protected-access
                association, graph.replica_info,
                ordinary_launch_binding.ProviderEvidence.ABSENT))
        if (graph.provider_absence_state
                is PhysicalAbsenceLoadState.ALREADY_PROVEN):
            return dict(association)
        if graph.provider_absence_state is not PhysicalAbsenceLoadState.NEEDS_PROBE:
            raise KueueAdmissionConflict(
                'Admissionless absence cannot replace provider evidence.')
        associations = ordinary_launch_binding.ordinary_launch_associations_table
        updated = connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == target.association_id,
                associations.c.updated_at == association['updated_at'],
                associations.c.owner_revision == association['owner_revision'],
                associations.c.provider_evidence == 'NOT_QUERIED',
                sqlalchemy.func.clock_timestamp()
                < valid_until).values(**_provider_absence_publication_values(
                    graph.service,
                    association,
                    observed_at=observed_at,
                    payload=expected_payload,
                    digest=expected_digest)).returning(
                        *associations.c)).mappings().one_or_none()
        if updated is None:
            raise KueueAdmissionConflict(
                'Admissionless teardown association changed before stamp.')
        _validate_provider_publication_completed_at(connection, valid_until)
        return dict(updated)

    def record_exact_pod_absence_after_normal_teardown_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        target: ExactPodAbsenceProbeTarget,
        *,
        provider_read_started_at: datetime.datetime,
    ) -> Mapping[str, Any]:
        """Stamp one exact admitted Pod 404 after successful normal teardown.

        The caller performs the Kubernetes read outside every database lock.
        This writer then reconstructs and locks the complete authoritative
        graph before accepting that short-lived provider observation.  It does
        not authorize or perform provider I/O.
        """
        _require_postgres_connection(connection)
        if not isinstance(target, ExactPodAbsenceProbeTarget):
            raise TypeError('target must be an ExactPodAbsenceProbeTarget.')
        target.validate()
        identity = target.identity

        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    identity.service_name).with_for_update()).mappings(
                    ).one_or_none()
        service = connection.execute(
            sqlalchemy.select(_SERVICES).where(
                _SERVICES.c.name == identity.service_name).with_for_update()
        ).mappings().one_or_none()
        paid_claims = serve_state_schema.paid_capacity_claims_table
        claim_rows = connection.execute(
            sqlalchemy.select(paid_claims).where(
                paid_claims.c.service_name == identity.service_name,
                paid_claims.c.replica_id == target.replica_id).order_by(
                    paid_claims.c.pool_key).with_for_update()).mappings().all()
        intent = connection.execute(
            sqlalchemy.select(_INTENTS).where(_INTENTS.c.intent_idempotency_key
                                              == target.intent_idempotency_key).
            with_for_update()).mappings().one_or_none()
        replica = connection.execute(
            sqlalchemy.select(_REPLICAS).where(
                _REPLICAS.c.service_name == identity.service_name,
                _REPLICAS.c.replica_id ==
                target.replica_id).with_for_update()).mappings().one_or_none()
        associations = ordinary_launch_binding.ordinary_launch_associations_table
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id == target.association_id).
            with_for_update()).mappings().one_or_none()
        requests = request_postgres_schema.REQUESTS
        request = None
        request_id = None if association is None else association['request_id']
        if request_id is not None:
            request_rows = connection.execute(
                sqlalchemy.select(requests).where(
                    sqlalchemy.or_(
                        requests.c.request_id == request_id,
                        requests.c.ordinary_launch_association_id ==
                        target.association_id)).order_by(requests.c.request_id).
                with_for_update()).mappings().all()
            if len(request_rows) > 1:
                raise KueueAdmissionConflict(
                    'Exact Pod absence found multiple launch request receipts.')
            request = request_rows[0] if request_rows else None
        queue = request_postgres_schema.QUEUE
        queue_rows = ([] if request_id is None else connection.execute(
            sqlalchemy.select(queue).where(
                queue.c.request_id == request_id).order_by(
                    queue.c.request_id).with_for_update()).mappings().all())
        pins = request_postgres_schema.REQUEST_RETENTION_PINS
        pin_rows = ([] if request_id is None else connection.execute(
            sqlalchemy.select(pins).where(
                sqlalchemy.or_(
                    pins.c.request_id == request_id,
                    pins.c.pin_id == target.association_id)).order_by(
                        pins.c.pin_kind,
                        pins.c.pin_id).with_for_update()).mappings().all())
        admission = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key ==
                target.intent_idempotency_key).with_for_update()).mappings(
                ).one_or_none()

        if (lifecycle is None or service is None or intent is None or
                replica is None or association is None or claim_rows or
                queue_rows or pin_rows or
                lifecycle['epoch'] != identity.service_lifecycle_epoch or
                service['hash'] != identity.service_hash or
                service['lifecycle_epoch'] != identity.service_lifecycle_epoch):
            raise KueueAdmissionConflict(
                'Exact Pod absence lost its provider-clean service graph.')
        if admission is None:
            raise KueueAdmissionConflict(
                'Exact Pod absence lost its admitted Pod receipt.')
        checked_identity = _validate_admission_intent_identity(
            admission, intent)
        state = replica['replica_state']
        try:
            info = serve_state.decode_replica_state_for_authority(
                replica['replica_state_version'], state)
        except (AttributeError, KeyError, RuntimeError, TypeError,
                ValueError) as error:
            raise KueueAdmissionConflict(
                'Exact Pod absence found malformed replica authority.'
            ) from error
        down_status = getattr(info.status_property, 'sky_down_status', None)
        down_value = getattr(down_status, 'value', None)
        cleanup_intended = bool(
            info.status_property.is_scale_down or info.status_property.purged or
            service['status'] in ('SHUTTING_DOWN', 'FAILED_CLEANUP') or
            replica['version'] != service['current_version'] or
            down_value in ('SCHEDULED', 'RUNNING', 'FAILED', 'SUCCEEDED'))
        if (checked_identity != identity or intent['state'] != 'COMMITTED' or
                intent['replica_id'] != target.replica_id or
                intent['replica_record_id'] != target.replica_record_id or
                admission['state'] != KueueAdmissionState.POLICY_ADMITTED.value
                or admission['replica_id'] != target.replica_id or
                admission['replica_record_id'] != target.replica_record_id or
                admission['provider_cluster_generation']
                != target.provider_cluster_generation or
                admission['association_id'] != target.association_id or
                admission['pod_namespace'] != target.pod_namespace or
                admission['pod_name'] != target.pod_name or
                admission['pod_uid'] != target.pod_uid or
                replica['reserved_fill_intent_idempotency_key']
                != target.intent_idempotency_key or
                replica['ordinary_launch_association_id'] is not None or
                not isinstance(state, Mapping) or state.get('replica_record_id')
                != str(target.replica_record_id) or
                state.get('reserved_fill_intent_idempotency_key')
                != target.intent_idempotency_key or
                info.replica_record_id != str(target.replica_record_id) or
                info.replica_id != target.replica_id or
                info.version != identity.service_version or
                info.reserved_fill is not True or
                info.is_zero_cost is not True or
                info.paid_capacity_pool_key is not None or
                not cleanup_intended or
                not _replica_matches_reserved_fill_intent(
                    info, intent, identity, target.intent_idempotency_key)):
            raise KueueAdmissionConflict(
                'Exact Pod absence is not a retiring materialized reserved fill.'
            )

        receipt = admission['pod_receipt']
        receipt_digest = admission['pod_receipt_sha256']
        if not isinstance(receipt, Mapping) or not isinstance(
                receipt_digest, str):
            raise KueueAdmissionConflict(
                'Exact Pod absence lost its canonical admitted receipt.')
        _validate_receipt(receipt,
                          state=KueueAdmissionState.POLICY_ADMITTED,
                          identity=identity,
                          intent_idempotency_key=target.intent_idempotency_key,
                          replica_record_id=target.replica_record_id,
                          pod_namespace=target.pod_namespace,
                          pod_name=target.pod_name,
                          pod_uid=target.pod_uid)
        _, digest_expression = _receipt_expression(receipt)
        canonical_receipt_digest = connection.execute(
            sqlalchemy.select(digest_expression)).scalar_one()
        if receipt_digest != canonical_receipt_digest:
            raise KueueAdmissionConflict(
                'Exact Pod absence receipt digest is not canonical.')

        if (association['service_name'] != identity.service_name or
                association['service_hash'] != identity.service_hash or
                association['service_lifecycle_epoch']
                != identity.service_lifecycle_epoch or
                association['service_version'] != identity.service_version or
                association['replica_id'] != target.replica_id or
                association['replica_record_id'] != target.replica_record_id or
                association['launch_generation']
                != target.provider_cluster_generation or
                association['authorization_reference']
                != f'reserved-fill:{target.intent_idempotency_key}' or
                association['profile_kind'] != 'RESERVED_FILL' or
                association['paid_capacity_pool_key'] is not None or
                association['resolution'] != 'PROJECTED' or
                association['reconciliation_outcome'] != 'PROJECTED' or
                association['effect_phase'] != 'SERVICE_JOB_RECORDED' or
                type(association['service_job_id']) is not int or
                association['service_job_id'] < 1 or
                association['terminal_status'] not in ('SUCCEEDED', 'FAILED',
                                                       'CANCELLED') or
                not isinstance(association['terminal_cause'], str) or
                not association['terminal_cause'] or
                type(association['terminal_execution_generation']) is not int or
                association['terminal_execution_generation'] < 1 or
                association['execution_quiescence_required'] is not True or
                association['execution_quiesced_generation']
                != association['terminal_execution_generation'] or
                association['execution_quiesced_at'] is None or
                association['projected_at'] is None or
                association['pin_released_at'] is None or
                association['tombstone_not_before'] is None or
            (request is not None and
             not _terminal_request_matches_association(request, association))):
            raise KueueAdmissionConflict(
                'Exact Pod absence launch request is not terminal and quiescent.'
            )

        observed_at, valid_until = _validate_provider_read_started_at(
            connection, provider_read_started_at)
        if observed_at < association['execution_quiesced_at']:
            raise KueueAdmissionConflict(
                'Exact Pod absence predates launch execution quiescence.')
        payload, digest = _exact_pod_absence_evidence(association, target)
        current_evidence = association['provider_evidence']
        if current_evidence == 'ABSENT':
            if (association['provider_evidence_payload'] != payload or
                    association['provider_evidence_digest'] != digest or
                    association['provider_evidence_observed_at'] is None or
                    association['provider_evidence_observed_at']
                    < association['execution_quiesced_at']):
                raise KueueAdmissionConflict(
                    'Exact Pod absence replay conflicts with retained evidence.'
                )
            return dict(association)
        if (current_evidence != 'NOT_QUERIED' or
                association['provider_evidence_observed_at'] is not None or
                association['provider_evidence_payload'] is not None or
                association['provider_evidence_digest'] is not None):
            raise KueueAdmissionConflict(
                'Exact Pod absence cannot replace prior provider evidence.')
        updated = connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == target.association_id,
                associations.c.updated_at == association['updated_at'],
                associations.c.owner_revision == association['owner_revision'],
                associations.c.provider_evidence == 'NOT_QUERIED',
                sqlalchemy.func.clock_timestamp()
                < valid_until).values(**_provider_absence_publication_values(
                    service,
                    association,
                    observed_at=observed_at,
                    payload=payload,
                    digest=digest)).returning(
                        *associations.c)).mappings().one_or_none()
        if updated is None:
            raise KueueAdmissionConflict(
                'Exact Pod absence association changed before its stamp.')
        _validate_provider_publication_completed_at(connection, valid_until)
        return dict(updated)

    def _lock_exact_intent(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        intent_idempotency_key: str,
    ) -> Mapping[str, Any]:
        row = connection.execute(
            sqlalchemy.select(_INTENTS).where(
                *_intent_predicates(identity, intent_idempotency_key)).
            with_for_update()).mappings().one_or_none()
        if row is None:
            raise KueueAdmissionConflict(
                'Kueue admission lost its exact durable intent.')
        return row

    def insert_intent_pending_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        intent_idempotency_key: str,
        replacement_surge_units: int = 0,
        replacement_compatibility_sha256: str | None = None,
    ) -> KueueAdmissionRow:
        """Insert the grant-time admission row, or return its exact replay."""
        _require_postgres_connection(connection)
        identity.validate()
        _require_sha256(intent_idempotency_key, 'intent_idempotency_key')
        _require_nonnegative(replacement_surge_units, 'replacement_surge_units')
        if replacement_surge_units == 0:
            if replacement_compatibility_sha256 is not None:
                raise ValueError('A zero surge cannot carry compatibility.')
        else:
            assert replacement_compatibility_sha256 is not None
            _require_sha256(replacement_compatibility_sha256,
                            'replacement_compatibility_sha256')

        # The service-row gap lock serializes an empty table and all bounded
        # same-domain batch inserts.  Intent debits and the sequencer bound the
        # batch; only the one-surge partial index is a database backstop.
        self._lock_service_owner(connection, identity.service_name,
                                 identity.service_hash)
        intent = self._lock_exact_intent(connection, identity,
                                         intent_idempotency_key)
        capacity_unit, planned_capacity = _canonical_capacity(intent, identity)
        replay = self.get_for_intent_in_connection(connection,
                                                   identity.service_name,
                                                   intent_idempotency_key,
                                                   for_update=True)
        if replay is not None:
            if (not all(predicate for predicate in (
                    replay.unresolved_domain_sha256 ==
                    identity.unresolved_domain_sha256,
                    replay.service_hash == identity.service_hash,
                    replay.service_lifecycle_epoch ==
                    identity.service_lifecycle_epoch,
                    replay.service_version == identity.service_version,
                    replay.pool_key == identity.pool_key,
                    replay.pool_epoch == identity.pool_epoch,
                    replay.physical_cluster_uid ==
                    identity.physical_cluster_uid,
                    replay.kubernetes_context == identity.kubernetes_context,
                    replay.accelerator == identity.accelerator,
                    replay.accelerator_count == identity.accelerator_count,
                    replay.worker_projection_sha256 ==
                    identity.worker_projection_sha256,
                    replay.capacity_unit == capacity_unit,
                    replay.planned_capacity == planned_capacity,
                    replay.replacement_surge_units == replacement_surge_units,
                    replay.replacement_compatibility_sha256 ==
                    replacement_compatibility_sha256,
            ))):
                raise KueueAdmissionConflict(
                    'Intent replay maps to different admission authority.')
            return replay
        if intent['state'] not in ('GRANTED', 'ACTUATING', 'RETRYABLE'):
            raise KueueAdmissionConflict(
                'Only a live uncommitted intent may create an admission.')
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        if intent['valid_until'] <= now:
            raise KueueAdmissionConflict(
                'An expired intent cannot create an admission.')
        if replacement_surge_units > planned_capacity:
            raise KueueAdmissionConflict(
                'Replacement surge exceeds the intent configured-unit width.')
        if replacement_surge_units > 0:
            surge = connection.execute(
                sqlalchemy.select(_ADMISSIONS.c.intent_idempotency_key).where(
                    _ADMISSIONS.c.service_name == identity.service_name,
                    _ADMISSIONS.c.replacement_surge_units
                    > 0).with_for_update()).scalar_one_or_none()
            if surge is not None:
                raise KueueAdmissionConflict(
                    'The service already owns its one replacement surge.')
        row = connection.execute(
            sqlalchemy.insert(_ADMISSIONS).values(
                intent_idempotency_key=intent_idempotency_key,
                service_name=identity.service_name,
                unresolved_domain_sha256=identity.unresolved_domain_sha256,
                service_hash=identity.service_hash,
                service_lifecycle_epoch=identity.service_lifecycle_epoch,
                service_version=identity.service_version,
                pool_key=identity.pool_key,
                pool_epoch=identity.pool_epoch,
                physical_cluster_uid=identity.physical_cluster_uid,
                kubernetes_context=identity.kubernetes_context,
                accelerator=identity.accelerator,
                accelerator_count=identity.accelerator_count,
                worker_projection_sha256=identity.worker_projection_sha256,
                capacity_unit=capacity_unit,
                planned_capacity=planned_capacity,
                state=KueueAdmissionState.INTENT_PENDING.value,
                replacement_surge_units=replacement_surge_units,
                replacement_compatibility_sha256=(
                    replacement_compatibility_sha256),
                created_at=now,
                updated_at=now).returning(*_ADMISSIONS.c)).mappings().one()
        return KueueAdmissionRow.from_mapping(row)

    def prelock_provider_free_terminal_admissions_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        service_hash: str,
    ) -> tuple[ProviderFreeTerminalAdmissionProof, ...]:
        """Lock and prove every removable provider-free terminal admission.

        Discovery is deliberately non-locking while the service-row gap lock
        is held.  The method then acquires the complete graph suffix in the
        canonical intent, replica, association, request, queue, pin, admission
        order.  A candidate with any provider/request path is ordinary
        backpressure and produces no proof; malformed identity fails closed.
        """
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_nonempty(service_hash, 'service_hash')
        self._lock_service_owner(connection, service_name, service_hash)
        service_status = connection.execute(
            sqlalchemy.select(_SERVICES.c.status).where(
                _SERVICES.c.name == service_name)).scalar_one()
        teardown_service = service_status in ('SHUTTING_DOWN', 'FAILED_CLEANUP')
        discovery_now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        terminal_eligibility = _INTENTS.c.valid_until <= discovery_now
        if teardown_service:
            terminal_eligibility = sqlalchemy.or_(
                terminal_eligibility,
                _INTENTS.c.last_error == 'service_teardown')
        candidate_keys = tuple(
            connection.execute(
                sqlalchemy.select(
                    _ADMISSIONS.c.intent_idempotency_key).select_from(
                        _ADMISSIONS.join(
                            _INTENTS,
                            sqlalchemy.and_(
                                _INTENTS.c.service_name ==
                                _ADMISSIONS.c.service_name,
                                _INTENTS.c.intent_idempotency_key ==
                                _ADMISSIONS.c.intent_idempotency_key))).where(
                                    _ADMISSIONS.c.service_name == service_name,
                                    _ADMISSIONS.c.service_hash == service_hash,
                                    _ADMISSIONS.c.state ==
                                    KueueAdmissionState.INTENT_PENDING.value,
                                    _ADMISSIONS.c.replica_id.is_(None),
                                    _ADMISSIONS.c.association_id.is_(None),
                                    _INTENTS.c.state == 'TERMINAL',
                                    _INTENTS.c.terminal_at.is_not(None),
                                    terminal_eligibility).
                order_by(_ADMISSIONS.c.intent_idempotency_key)).scalars())
        if not candidate_keys:
            return ()

        intent_rows = connection.execute(
            sqlalchemy.select(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key.in_(candidate_keys)).order_by(
                    _INTENTS.c.intent_idempotency_key).with_for_update()
        ).mappings().all()
        replica_rows = connection.execute(
            sqlalchemy.select(_REPLICAS).where(
                _REPLICAS.c.service_name == service_name,
                _REPLICAS.c.reserved_fill_intent_idempotency_key.
                in_(candidate_keys)).order_by(
                    _REPLICAS.c.replica_id).with_for_update()).mappings().all()

        associations = ordinary_launch_binding.ordinary_launch_associations_table
        authorization_references = tuple(
            f'reserved-fill:{key}' for key in candidate_keys)
        association_rows = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.service_name == service_name,
                associations.c.authorization_reference.in_(
                    authorization_references)).order_by(
                        associations.c.association_id).with_for_update()
        ).mappings().all()
        association_ids = tuple(
            row['association_id'] for row in association_rows)
        association_request_ids = tuple(
            sorted({str(row['request_id']) for row in association_rows}))
        requests = request_postgres_schema.REQUESTS
        queue = request_postgres_schema.QUEUE
        pins = request_postgres_schema.REQUEST_RETENTION_PINS
        request_rows: list[Mapping[str, Any]] = []
        queue_rows: list[Mapping[str, Any]] = []
        pin_rows: list[Mapping[str, Any]] = []
        if association_ids:
            request_rows = connection.execute(
                sqlalchemy.select(requests).where(
                    sqlalchemy.or_(
                        requests.c.ordinary_launch_association_id.in_(
                            association_ids),
                        requests.c.request_id.in_(association_request_ids))).
                order_by(
                    requests.c.request_id).with_for_update()).mappings().all()
            request_ids = tuple(
                sorted({
                    *association_request_ids,
                    *(str(row['request_id']) for row in request_rows)
                }))
            queue_rows = connection.execute(
                sqlalchemy.select(queue).where(
                    queue.c.request_id.in_(request_ids)).order_by(
                        queue.c.request_id).with_for_update()).mappings().all()
            pin_rows = connection.execute(
                sqlalchemy.select(pins).where(
                    sqlalchemy.or_(
                        pins.c.request_id.in_(request_ids),
                        pins.c.pin_id.in_(association_ids))).order_by(
                            pins.c.pin_kind,
                            pins.c.pin_id).with_for_update()).mappings().all()

        admission_rows = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key.in_(
                    candidate_keys)).order_by(
                        _ADMISSIONS.c.intent_idempotency_key).with_for_update()
        ).mappings().all()
        intents = {
            str(row['intent_idempotency_key']): row for row in intent_rows
        }
        admissions = {
            str(row['intent_idempotency_key']): row for row in admission_rows
        }
        if (set(intents) != set(candidate_keys) or
                set(admissions) != set(candidate_keys)):
            raise KueueAdmissionConflict(
                'Provider-free terminal admission graph changed during lock.')

        replica_path_keys = {
            str(row['reserved_fill_intent_idempotency_key'])
            for row in replica_rows
        }
        reference_to_key = {
            f'reserved-fill:{key}': key for key in candidate_keys
        }
        association_id_to_key = {
            row['association_id']: reference_to_key[str(
                row['authorization_reference'])] for row in association_rows
        }
        association_request_to_key = {
            str(row['request_id']): association_id_to_key[row['association_id']]
            for row in association_rows
        }
        request_id_to_key: dict[str, str] = dict(association_request_to_key)
        request_path_keys: set[str] = set()
        for row in request_rows:
            association_id = row['ordinary_launch_association_id']
            key = (association_id_to_key.get(association_id) or
                   association_request_to_key.get(str(row['request_id'])))
            if key is not None:
                request_id_to_key[str(row['request_id'])] = key
                request_path_keys.add(key)
        queue_path_keys = {
            request_id_to_key[str(row['request_id'])]
            for row in queue_rows
            if str(row['request_id']) in request_id_to_key
        }
        pin_path_keys = {(association_id_to_key.get(row['pin_id']) or
                          request_id_to_key.get(str(row['request_id'])))
                         for row in pin_rows}
        provider_path_keys = (replica_path_keys |
                              set(association_id_to_key.values()) |
                              request_path_keys | queue_path_keys |
                              {key for key in pin_path_keys if key is not None})

        transaction_id = int(
            connection.execute(sqlalchemy.select(
                sqlalchemy.func.txid_current())).scalar_one())
        checked_at = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        proofs: list[ProviderFreeTerminalAdmissionProof] = []
        empty_fields = ('replica_id', 'replica_record_id',
                        'provider_cluster_generation', 'association_id',
                        'pod_namespace', 'pod_name', 'pod_uid', 'pod_receipt',
                        'pod_receipt_sha256', 'observed_at', 'valid_until',
                        'admitted_at')
        for key in candidate_keys:
            intent = intents[key]
            admission = admissions[key]
            identity = _validate_admission_intent_identity(admission, intent)
            teardown_authorized = bool(
                teardown_service and intent['last_error'] == 'service_teardown')
            if (identity.service_name != service_name or
                    identity.service_hash != service_hash):
                raise KueueAdmissionConflict(
                    'Provider-free terminal admission lost its service owner.')
            if (intent['state'] != 'TERMINAL' or
                    intent['replica_id'] is not None or
                    intent['replica_record_id'] is not None or
                    intent['terminal_at'] is None or
                (intent['valid_until'] > checked_at and
                 not teardown_authorized) or admission['state']
                    != KueueAdmissionState.INTENT_PENDING.value or
                    any(admission[field] is not None
                        for field in empty_fields)):
                continue
            if key in provider_path_keys:
                continue
            proofs.append(
                ProviderFreeTerminalAdmissionProof(
                    transaction_id=transaction_id,
                    service_name=service_name,
                    service_hash=service_hash,
                    intent_idempotency_key=key,
                    unresolved_domain_sha256=identity.unresolved_domain_sha256,
                    intent_updated_at=intent['updated_at'],
                    admission_updated_at=admission['updated_at'],
                    teardown_authorized=teardown_authorized,
                    checked_at=checked_at))
        return tuple(proofs)

    def delete_provider_free_terminal_admission_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        proof: ProviderFreeTerminalAdmissionProof,
    ) -> KueueAdmissionRow:
        """Delete one prelocked provider-free admission, retaining its intent."""
        _require_postgres_connection(connection)
        if not isinstance(proof, ProviderFreeTerminalAdmissionProof):
            raise TypeError(
                'proof must be a ProviderFreeTerminalAdmissionProof.')
        transaction_id = int(
            connection.execute(sqlalchemy.select(
                sqlalchemy.func.txid_current())).scalar_one())
        if transaction_id != proof.transaction_id:
            raise KueueAdmissionConflict(
                'Provider-free terminal proof belongs to another transaction.')
        intent = connection.execute(
            sqlalchemy.select(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key ==
                proof.intent_idempotency_key)).mappings().one_or_none()
        admission = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key ==
                proof.intent_idempotency_key)).mappings().one_or_none()
        if intent is None or admission is None:
            raise KueueAdmissionConflict(
                'Provider-free terminal proof lost its locked graph.')
        identity = _validate_admission_intent_identity(admission, intent)
        service_status = connection.execute(
            sqlalchemy.select(_SERVICES.c.status).where(
                _SERVICES.c.name == proof.service_name)).scalar_one_or_none()
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        teardown_authorized = bool(
            intent['last_error'] == 'service_teardown' and
            service_status in ('SHUTTING_DOWN', 'FAILED_CLEANUP'))
        empty_fields = ('replica_id', 'replica_record_id',
                        'provider_cluster_generation', 'association_id',
                        'pod_namespace', 'pod_name', 'pod_uid', 'pod_receipt',
                        'pod_receipt_sha256', 'observed_at', 'valid_until',
                        'admitted_at')
        if (identity.service_name != proof.service_name or
                identity.service_hash != proof.service_hash or
                identity.unresolved_domain_sha256
                != proof.unresolved_domain_sha256 or
                intent['updated_at'] != proof.intent_updated_at or
                admission['updated_at'] != proof.admission_updated_at or
                intent['state'] != 'TERMINAL' or
                intent['replica_id'] is not None or
                intent['replica_record_id'] is not None or
                intent['terminal_at'] is None or
                teardown_authorized != proof.teardown_authorized or
            (intent['valid_until'] > now and not teardown_authorized) or
                admission['state'] != KueueAdmissionState.INTENT_PENDING.value
                or any(admission[field] is not None for field in empty_fields)):
            raise KueueAdmissionConflict(
                'Provider-free terminal proof no longer matches authority.')
        deleted = connection.execute(
            sqlalchemy.delete(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key ==
                proof.intent_idempotency_key,
                _ADMISSIONS.c.updated_at == proof.admission_updated_at,
                _ADMISSIONS.c.state == KueueAdmissionState.INTENT_PENDING.value,
                _ADMISSIONS.c.replica_id.is_(None),
                _ADMISSIONS.c.association_id.is_(None)).returning(
                    *_ADMISSIONS.c)).mappings().one_or_none()
        if deleted is None:
            raise KueueAdmissionConflict(
                'Provider-free terminal admission changed before deletion.')
        return KueueAdmissionRow.from_mapping(deleted)

    def prelock_materialized_admission_retirements_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        service_name: str,
        service_hash: str,
        service_lifecycle_epoch: int,
        targets: tuple[MaterializedAdmissionRetirementTarget, ...],
    ) -> tuple[MaterializedRetirementProof, ...]:
        """Lock and prove the provider-clean Kueue subset of a delete batch.

        The caller must already hold the lifecycle and service owner locks and
        must call this before locking replica rows.  Linked East replicas are
        positively classified from their immutable version projection and
        return no proof.  A linked Kueue replica without its exact admission
        fails closed except at the provider-free whole-service teardown
        boundary documented below.
        """
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_nonempty(service_hash, 'service_hash')
        _require_positive(service_lifecycle_epoch, 'service_lifecycle_epoch')
        if not isinstance(targets, tuple):
            raise TypeError('targets must be an immutable tuple.')
        for target in targets:
            if not isinstance(target, MaterializedAdmissionRetirementTarget):
                raise TypeError(
                    'targets must contain MaterializedAdmissionRetirementTarget.'
                )
            target.validate()
        replica_ids = [target.replica_id for target in targets]
        if len(set(replica_ids)) != len(replica_ids):
            raise ValueError('Retirement targets must not repeat replica IDs.')
        if not targets:
            return ()
        sorted_targets = tuple(sorted(targets,
                                      key=lambda item: item.replica_id))
        expected_records = {
            target.replica_id: target.replica_record_id
            for target in sorted_targets
        }

        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table.c.epoch).
            where(serve_state_schema.service_lifecycle_fences_table.c.name ==
                  service_name)).scalar_one_or_none()
        service = connection.execute(
            sqlalchemy.select(_SERVICES).where(
                _SERVICES.c.name == service_name)).mappings().one_or_none()
        if (lifecycle != service_lifecycle_epoch or service is None or
                service['hash'] != service_hash or
                service['lifecycle_epoch'] != service_lifecycle_epoch):
            raise KueueAdmissionConflict(
                'Materialized retirement lost its service lifecycle owner.')

        discovery_rows = connection.execute(
            sqlalchemy.select(_REPLICAS).where(
                _REPLICAS.c.service_name == service_name,
                _REPLICAS.c.replica_id.in_(replica_ids)).order_by(
                    _REPLICAS.c.replica_id)).mappings().all()
        discovered_by_id = {
            int(row['replica_id']): row for row in discovery_rows
        }
        linked_keys_by_replica: dict[int, str] = {}
        for replica_id, row in discovered_by_id.items():
            state = row['replica_state']
            if not isinstance(state, Mapping):
                raise KueueAdmissionConflict(
                    'Materialized retirement found malformed replica state.')
            if state.get('replica_record_id') != str(
                    expected_records[replica_id]):
                raise KueueAdmissionConflict(
                    'Materialized retirement found a replaced replica record.')
            scalar_key = row['reserved_fill_intent_idempotency_key']
            projected_key = state.get('reserved_fill_intent_idempotency_key')
            if scalar_key is None:
                if projected_key is not None:
                    raise KueueAdmissionConflict(
                        'Protocol-v2 replica lost its normalized intent edge.')
                continue
            try:
                key = _require_sha256(scalar_key,
                                      'reserved_fill_intent_idempotency_key')
            except ValueError as error:
                raise KueueAdmissionConflict(
                    'Protocol-v2 replica has a malformed intent edge.'
                ) from error
            if projected_key != key:
                raise KueueAdmissionConflict(
                    'Protocol-v2 replica intent projections diverged.')
            linked_keys_by_replica[replica_id] = key
        if not linked_keys_by_replica:
            return ()

        linked_replica_ids = tuple(sorted(linked_keys_by_replica))
        linked_keys = tuple(sorted(set(linked_keys_by_replica.values())))
        paid_claims = serve_state_schema.paid_capacity_claims_table
        paid_claim_rows = connection.execute(
            sqlalchemy.select(paid_claims).where(
                paid_claims.c.service_name == service_name,
                paid_claims.c.replica_id.in_(linked_replica_ids)).order_by(
                    paid_claims.c.replica_id).with_for_update()).mappings().all(
                    )
        intent_rows = connection.execute(
            sqlalchemy.select(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key.in_(linked_keys)).order_by(
                    _INTENTS.c.intent_idempotency_key).with_for_update()
        ).mappings().all()
        intents = {
            str(row['intent_idempotency_key']): row for row in intent_rows
        }
        if set(intents) != set(linked_keys):
            raise KueueAdmissionConflict(
                'Protocol-v2 replica lost its committed intent.')

        identity_by_replica: dict[int, KueueAdmissionIdentity] = {}
        east_replica_ids: set[int] = set()
        for replica_id, key in linked_keys_by_replica.items():
            intent = intents[key]
            expected_record = expected_records[replica_id]
            if (intent['service_name'] != service_name or
                    intent['service_hash'] != service_hash or
                    intent['service_lifecycle_epoch'] != service_lifecycle_epoch
                    or intent['state'] != 'COMMITTED' or
                    intent['replica_id'] != replica_id or
                    intent['replica_record_id'] != expected_record):
                raise KueueAdmissionConflict(
                    'Protocol-v2 replica lost its exact committed handoff.')
            try:
                identity = (
                    zero_cost_actuation.
                    kueue_admission_identity_for_locked_intent_in_connection(
                        connection, intent, require_current_protocol=False))
            except Exception as error:  # pylint: disable=broad-except
                raise KueueAdmissionConflict(
                    'Protocol-v2 replica admission mode is not provable from '
                    'its immutable version.') from error
            if identity is None:
                east_replica_ids.add(replica_id)
            else:
                identity.validate()
                identity_by_replica[replica_id] = identity

        admission_discovery = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                sqlalchemy.or_(
                    _ADMISSIONS.c.intent_idempotency_key.in_(linked_keys),
                    sqlalchemy.and_(
                        _ADMISSIONS.c.service_name == service_name,
                        _ADMISSIONS.c.replica_id.in_(linked_replica_ids)))).
            order_by(_ADMISSIONS.c.intent_idempotency_key)).mappings().all()
        for row in admission_discovery:
            row_replica_id = row['replica_id']
            if (type(row_replica_id) is not int or
                    row_replica_id not in linked_keys_by_replica or
                    row['service_name'] != service_name or
                    row['intent_idempotency_key']
                    != linked_keys_by_replica[row_replica_id] or
                    row['replica_record_id']
                    != expected_records[row_replica_id]):
                raise KueueAdmissionConflict(
                    'Protocol-v2 retirement found a foreign Kueue admission.')
        discovered_admissions = {
            str(row['intent_idempotency_key']): row
            for row in admission_discovery
        }
        for replica_id in east_replica_ids:
            if linked_keys_by_replica[replica_id] in discovered_admissions:
                raise KueueAdmissionConflict(
                    'Scheduler-admitted East replica owns a Kueue admission.')
        admissionless_replica_ids = {
            replica_id for replica_id in identity_by_replica
            if linked_keys_by_replica[replica_id] not in discovered_admissions
        }
        if (admissionless_replica_ids and
                service['status'] not in ('SHUTTING_DOWN', 'FAILED_CLEANUP')):
            raise KueueAdmissionConflict(
                'Kueue-bound protocol-v2 replica lost its admission.')

        locked_replica_rows = connection.execute(
            sqlalchemy.select(_REPLICAS).where(
                _REPLICAS.c.service_name == service_name,
                _REPLICAS.c.replica_id.in_(replica_ids)).order_by(
                    _REPLICAS.c.replica_id).with_for_update()).mappings().all()
        locked_by_id = {
            int(row['replica_id']): row for row in locked_replica_rows
        }
        if set(locked_by_id) != set(discovered_by_id):
            raise KueueAdmissionConflict(
                'Retirement replica inventory changed during locking.')

        kueue_pairs = tuple((replica_id, expected_records[replica_id])
                            for replica_id in sorted(identity_by_replica))
        associations = ordinary_launch_binding.ordinary_launch_associations_table
        association_rows = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.service_name == service_name,
                sqlalchemy.tuple_(
                    associations.c.replica_id,
                    associations.c.replica_record_id).in_(kueue_pairs),
                associations.c.binding_protocol_version == 2).order_by(
                    associations.c.association_id).with_for_update()).mappings(
                    ).all()
        associations_by_replica: dict[int, list[Mapping[str, Any]]] = {}
        for row in association_rows:
            associations_by_replica.setdefault(int(row['replica_id']),
                                               []).append(row)
        if any(
                len(associations_by_replica.get(replica_id, ())) != 1
                for replica_id in identity_by_replica):
            raise KueueAdmissionConflict(
                'Kueue retirement requires one exact launch association.')

        # A successful admitted launch uses an exact Pod 404 receipt.  An
        # interrupted pre-materialization launch retains the older canonical
        # physical-cluster absence envelope.  These are the only two accepted
        # provider-clean retirement shapes.
        checked_associations: dict[int, Mapping[str, Any]] = {}
        normal_teardown_ids: set[int] = set()
        provider_free_pre_job_ids: set[int] = set()
        for replica_id in sorted(identity_by_replica):
            association = associations_by_replica[replica_id][0]
            if (association['resolution'] == 'PROJECTED' and
                    association['reconciliation_outcome'] == 'PROJECTED' and
                    association['effect_phase'] == 'SERVICE_JOB_RECORDED' and
                    type(association['service_job_id']) is int and
                    association['service_job_id'] > 0):
                checked_associations[replica_id] = association
                normal_teardown_ids.add(replica_id)
                continue
            try:
                checked, checked_info = (
                    ordinary_launch_binding.
                    projected_provider_absence_retirement_authority_in_connection(
                        connection, service_name, replica_id,
                        str(expected_records[replica_id])))
            except Exception as error:  # pylint: disable=broad-except
                raise KueueAdmissionConflict(
                    'Kueue retirement lacks canonical provider-absence '
                    'authority.') from error
            if ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
                    checked_info):
                checked_associations[replica_id] = checked
                continue
            if (service['status'] in ('SHUTTING_DOWN', 'FAILED_CLEANUP') and
                    _is_provider_absent_pre_job_association(checked)):
                checked_associations[replica_id] = checked
                provider_free_pre_job_ids.add(replica_id)
                continue
            raise KueueAdmissionConflict(
                'Kueue retirement lacks a terminal provider-absence policy.')
        if paid_claim_rows:
            raise KueueAdmissionConflict(
                'Kueue zero-cost retirement found paid-capacity authority.')

        association_ids = tuple(
            checked_associations[replica_id]['association_id']
            for replica_id in sorted(checked_associations))
        association_request_ids = tuple(
            sorted(
                str(association['request_id'])
                for association in checked_associations.values()))
        requests = request_postgres_schema.REQUESTS
        queue = request_postgres_schema.QUEUE
        pins = request_postgres_schema.REQUEST_RETENTION_PINS
        request_rows = connection.execute(
            sqlalchemy.select(requests).where(
                sqlalchemy.or_(
                    requests.c.ordinary_launch_association_id.in_(
                        association_ids),
                    requests.c.request_id.in_(association_request_ids))).
            order_by(requests.c.request_id).with_for_update()).mappings().all()
        request_ids = tuple(
            sorted({
                *association_request_ids,
                *(str(row['request_id']) for row in request_rows)
            }))
        queue_rows = connection.execute(
            sqlalchemy.select(queue).where(
                queue.c.request_id.in_(request_ids)).order_by(
                    queue.c.request_id).with_for_update()).mappings().all()
        pin_rows = connection.execute(
            sqlalchemy.select(pins).where(
                sqlalchemy.or_(
                    pins.c.request_id.in_(request_ids),
                    pins.c.pin_id.in_(association_ids))).order_by(
                        pins.c.pin_kind,
                        pins.c.pin_id).with_for_update()).mappings().all()
        kueue_keys = tuple(
            sorted(linked_keys_by_replica[replica_id]
                   for replica_id in identity_by_replica))
        kueue_replica_ids = tuple(sorted(identity_by_replica))
        admission_rows = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                sqlalchemy.or_(
                    _ADMISSIONS.c.intent_idempotency_key.in_(kueue_keys),
                    sqlalchemy.and_(
                        _ADMISSIONS.c.service_name == service_name,
                        _ADMISSIONS.c.replica_id.in_(
                            kueue_replica_ids)))).order_by(
                                _ADMISSIONS.c.intent_idempotency_key).
            with_for_update()).mappings().all()
        for row in admission_rows:
            row_replica_id = row['replica_id']
            if (type(row_replica_id) is not int or
                    row_replica_id not in identity_by_replica or
                    row['service_name'] != service_name or
                    row['intent_idempotency_key']
                    != linked_keys_by_replica[row_replica_id] or
                    row['replica_record_id']
                    != expected_records[row_replica_id]):
                raise KueueAdmissionConflict(
                    'Kueue retirement found a foreign admission during lock.')
        admissions = {
            str(row['intent_idempotency_key']): row for row in admission_rows
        }
        if queue_rows or pin_rows:
            raise KueueAdmissionConflict(
                'Kueue retirement found a live queue or retention pin.')

        transaction_id = int(
            connection.execute(sqlalchemy.select(
                sqlalchemy.func.txid_current())).scalar_one())
        checked_at = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        proofs: list[MaterializedRetirementProof] = []
        for replica_id in sorted(identity_by_replica):
            key = linked_keys_by_replica[replica_id]
            intent = intents[key]
            identity = identity_by_replica[replica_id]
            admission = admissions.get(key)
            association = checked_associations[replica_id]
            request_id_value = association['request_id']
            if not isinstance(request_id_value, str) or not request_id_value:
                raise KueueAdmissionConflict(
                    'Kueue retirement lost its launch request identity.')
            request_id = request_id_value
            linked_requests = [
                row for row in request_rows
                if (str(row['request_id']) == request_id or
                    row['ordinary_launch_association_id'] ==
                    association['association_id'])
            ]
            if len(linked_requests) > 1:
                raise KueueAdmissionConflict(
                    'Kueue retirement found multiple launch request receipts.')
            request = linked_requests[0] if linked_requests else None
            replica = locked_by_id[replica_id]
            state = replica['replica_state']
            normal_teardown = replica_id in normal_teardown_ids
            request_is_exact = bool(
                request is not None and
                _terminal_request_matches_association(request, association))
            if (replica['ordinary_launch_association_id'] is not None or
                    replica['reserved_fill_intent_idempotency_key'] != key or
                    not isinstance(state, Mapping) or
                    state.get('replica_record_id') != str(
                        expected_records[replica_id]) or
                    state.get('reserved_fill_intent_idempotency_key') != key or
                (request is not None and not request_is_exact)):
                raise KueueAdmissionConflict(
                    'Kueue retirement graph is not exact and provider-clean.')

            provider_free_pre_job = replica_id in provider_free_pre_job_ids
            provider_free_graph: _AdmissionlessTeardownGraph | None = None
            if provider_free_pre_job:
                provider_free_graph = (
                    _validate_admissionless_retirement_rows_in_connection(
                        connection,
                        lifecycle_epoch=service_lifecycle_epoch,
                        service=service,
                        intent=intent,
                        identity=identity,
                        intent_idempotency_key=key,
                        replica=replica,
                        association=association,
                        request=request,
                        replica_id=replica_id,
                        replica_record_id=expected_records[replica_id]))
                if (provider_free_graph.provider_absence_state
                        is not PhysicalAbsenceLoadState.ALREADY_PROVEN):
                    raise KueueAdmissionConflict(
                        'Provider-free pre-job retirement requires canonical '
                        'provider absence.')
                _validate_provider_absent_pre_job_replica(
                    provider_free_graph.replica_info)

            if admission is None:
                if (replica_id not in admissionless_replica_ids or
                        service['status']
                        not in ('SHUTTING_DOWN', 'FAILED_CLEANUP')):
                    raise KueueAdmissionConflict(
                        'Kueue-bound protocol-v2 replica lost its locked '
                        'admission.')
                graph = (provider_free_graph or
                         _validate_admissionless_retirement_rows_in_connection(
                             connection,
                             lifecycle_epoch=service_lifecycle_epoch,
                             service=service,
                             intent=intent,
                             identity=identity,
                             intent_idempotency_key=key,
                             replica=replica,
                             association=association,
                             request=request,
                             replica_id=replica_id,
                             replica_record_id=expected_records[replica_id]))
                if (graph.provider_absence_state
                        is not PhysicalAbsenceLoadState.ALREADY_PROVEN):
                    raise KueueAdmissionConflict(
                        'Admissionless retirement lacks exact terminal '
                        'provider-absence authority.')
                proofs.append(
                    AdmissionlessMaterializedRetirementProof(
                        transaction_id=transaction_id,
                        service_name=service_name,
                        service_hash=service_hash,
                        service_lifecycle_epoch=service_lifecycle_epoch,
                        intent_idempotency_key=key,
                        intent_updated_at=intent['updated_at'],
                        replica_id=replica_id,
                        replica_record_id=expected_records[replica_id],
                        provider_cluster_generation=int(
                            association['launch_generation']),
                        association_id=association['association_id'],
                        association_updated_at=association['updated_at'],
                        request_id=request_id,
                        request_updated_at=(None if request is None else
                                            request['updated_at']),
                        checked_at=checked_at))
                continue

            if replica_id in admissionless_replica_ids:
                raise KueueAdmissionConflict(
                    'Kueue admission appeared after retirement discovery.')
            if provider_free_pre_job:
                _validate_provider_absent_pre_job_admission(
                    admission,
                    intent,
                    identity,
                    association,
                    replica_id=replica_id,
                    replica_record_id=expected_records[replica_id])
            else:
                checked_identity = _validate_admission_intent_identity(
                    admission, intent)
                if (checked_identity != identity or
                        admission['replica_id'] != replica_id or
                        admission['replica_record_id']
                        != expected_records[replica_id] or
                        admission['provider_cluster_generation']
                        != association['launch_generation'] or
                        admission['association_id']
                        != association['association_id']):
                    raise KueueAdmissionConflict(
                        'Kueue retirement admission graph is not exact.')
            if normal_teardown:
                try:
                    info = serve_state.decode_replica_state_for_authority(
                        replica['replica_state_version'], state)
                except (AttributeError, KeyError, RuntimeError, TypeError,
                        ValueError) as error:
                    raise KueueAdmissionConflict(
                        'Kueue normal retirement has malformed replica '
                        'authority.') from error
                pod_namespace = admission['pod_namespace']
                pod_name = admission['pod_name']
                pod_uid = admission['pod_uid']
                receipt = admission['pod_receipt']
                receipt_digest = admission['pod_receipt_sha256']
                if (admission['state']
                        != KueueAdmissionState.POLICY_ADMITTED.value or not all(
                            isinstance(value, str) and value
                            for value in (pod_namespace, pod_name, pod_uid)) or
                        not isinstance(receipt, Mapping) or
                        not isinstance(receipt_digest, str)):
                    raise KueueAdmissionConflict(
                        'Kueue normal retirement lost its admitted Pod receipt.'
                    )
                _validate_receipt(
                    receipt,
                    state=KueueAdmissionState.POLICY_ADMITTED,
                    identity=identity,
                    intent_idempotency_key=key,
                    replica_record_id=expected_records[replica_id],
                    pod_namespace=pod_namespace,
                    pod_name=pod_name,
                    pod_uid=pod_uid)
                _, receipt_digest_expression = _receipt_expression(receipt)
                canonical_receipt_digest = connection.execute(
                    sqlalchemy.select(receipt_digest_expression)).scalar_one()
                exact_target = ExactPodAbsenceProbeTarget(
                    identity=identity,
                    intent_idempotency_key=key,
                    replica_id=replica_id,
                    replica_record_id=expected_records[replica_id],
                    provider_cluster_generation=int(
                        association['launch_generation']),
                    association_id=association['association_id'],
                    pod_namespace=pod_namespace,
                    pod_name=pod_name,
                    pod_uid=pod_uid)
                exact_target.validate()
                expected_payload, expected_digest = (
                    _exact_pod_absence_evidence(association, exact_target))
                down_status = getattr(info.status_property.sky_down_status,
                                      'value', None)
                cleanup_intended = bool(
                    info.status_property.is_scale_down or
                    info.status_property.purged or
                    service['status'] in ('SHUTTING_DOWN', 'FAILED_CLEANUP') or
                    replica['version'] != service['current_version'] or
                    down_status in ('SCHEDULED', 'RUNNING', 'FAILED',
                                    'SUCCEEDED'))
                if (receipt_digest != canonical_receipt_digest or
                        association['profile_kind'] != 'RESERVED_FILL' or
                        association['authorization_reference']
                        != f'reserved-fill:{key}' or
                        association['paid_capacity_pool_key'] is not None or
                        association['provider_evidence'] != 'ABSENT' or
                        association['provider_evidence_observed_at'] is None or
                        association['provider_evidence_observed_at']
                        < association['execution_quiesced_at'] or
                        association['provider_evidence_payload']
                        != expected_payload or
                        association['provider_evidence_digest']
                        != expected_digest or info.replica_id != replica_id or
                        info.replica_record_id != str(
                            expected_records[replica_id]) or
                        info.version != identity.service_version or
                        info.cluster_name != association['cluster_name'] or
                        info.reserved_fill is not True or
                        info.is_zero_cost is not True or
                        info.paid_capacity_pool_key is not None or
                        not cleanup_intended or
                        not _replica_matches_reserved_fill_intent(
                            info, intent, identity, key)):
                    raise KueueAdmissionConflict(
                        'Kueue normal retirement lacks exact post-down Pod '
                        'absence authority.')
            proofs.append(
                MaterializedAdmissionRetirementProof(
                    transaction_id=transaction_id,
                    service_name=service_name,
                    service_hash=service_hash,
                    service_lifecycle_epoch=service_lifecycle_epoch,
                    intent_idempotency_key=key,
                    intent_updated_at=intent['updated_at'],
                    replica_id=replica_id,
                    replica_record_id=expected_records[replica_id],
                    provider_cluster_generation=int(
                        association['launch_generation']),
                    association_id=association['association_id'],
                    association_updated_at=association['updated_at'],
                    admission_updated_at=admission['updated_at'],
                    request_id=request_id,
                    request_updated_at=(None if request is None else
                                        request['updated_at']),
                    checked_at=checked_at))
        return tuple(proofs)

    @staticmethod
    def _validate_retirement_proofs(
        connection: sqlalchemy.engine.Connection,
        proofs: tuple[MaterializedRetirementProof, ...],
    ) -> tuple[MaterializedRetirementProof, ...]:
        if not isinstance(proofs, tuple) or any(
                not isinstance(proof,
                               (MaterializedAdmissionRetirementProof,
                                AdmissionlessMaterializedRetirementProof))
                for proof in proofs):
            raise TypeError('proofs must be an immutable retirement tuple.')
        keys = [proof.intent_idempotency_key for proof in proofs]
        replica_ids = [proof.replica_id for proof in proofs]
        if len(set(keys)) != len(keys) or len(
                set(replica_ids)) != len(replica_ids):
            raise ValueError('Retirement proofs must be unique.')
        transaction_id = int(
            connection.execute(sqlalchemy.select(
                sqlalchemy.func.txid_current())).scalar_one())
        if any(proof.transaction_id != transaction_id for proof in proofs):
            raise KueueAdmissionConflict(
                'Materialized retirement proof belongs to another transaction.')
        return tuple(
            sorted(proofs, key=lambda proof: proof.intent_idempotency_key))

    def delete_materialized_admissions_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        proofs: tuple[MaterializedRetirementProof, ...],
    ) -> tuple[KueueAdmissionRow, ...]:
        """Delete prelocked admissions before their replica parents."""
        _require_postgres_connection(connection)
        ordered = self._validate_retirement_proofs(connection, proofs)
        deleted_rows: list[KueueAdmissionRow] = []
        for proof in ordered:
            if isinstance(proof, AdmissionlessMaterializedRetirementProof):
                admission = connection.execute(
                    sqlalchemy.select(_ADMISSIONS).where(
                        _ADMISSIONS.c.intent_idempotency_key ==
                        proof.intent_idempotency_key).with_for_update()
                ).mappings().one_or_none()
                if admission is not None:
                    raise KueueAdmissionConflict(
                        'Admissionless retirement found a concurrent '
                        'admission row.')
                continue
            row = connection.execute(
                sqlalchemy.delete(_ADMISSIONS).where(
                    _ADMISSIONS.c.intent_idempotency_key ==
                    proof.intent_idempotency_key,
                    _ADMISSIONS.c.service_name == proof.service_name,
                    _ADMISSIONS.c.service_hash == proof.service_hash,
                    _ADMISSIONS.c.replica_id == proof.replica_id,
                    _ADMISSIONS.c.replica_record_id == proof.replica_record_id,
                    _ADMISSIONS.c.provider_cluster_generation ==
                    proof.provider_cluster_generation,
                    _ADMISSIONS.c.association_id == proof.association_id,
                    _ADMISSIONS.c.updated_at == proof.admission_updated_at).
                returning(*_ADMISSIONS.c)).mappings().one_or_none()
            if row is None:
                raise KueueAdmissionConflict(
                    'Materialized admission changed before retirement.')
            deleted_rows.append(KueueAdmissionRow.from_mapping(row))
        return tuple(deleted_rows)

    def finalize_materialized_admission_retirements_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        proofs: tuple[MaterializedRetirementProof, ...],
    ) -> tuple[str, ...]:
        """Delete committed intents after the caller deleted exact replicas."""
        _require_postgres_connection(connection)
        ordered = self._validate_retirement_proofs(connection, proofs)
        deleted_keys: list[str] = []
        associations = ordinary_launch_binding.ordinary_launch_associations_table
        for proof in ordered:
            admission_exists = connection.execute(
                sqlalchemy.select(sqlalchemy.literal(True)).where(
                    sqlalchemy.exists().where(
                        _ADMISSIONS.c.intent_idempotency_key ==
                        proof.intent_idempotency_key))).scalar_one_or_none()
            replica_exists = connection.execute(
                sqlalchemy.select(sqlalchemy.literal(True)).where(
                    sqlalchemy.exists().where(
                        _REPLICAS.c.service_name == proof.service_name,
                        _REPLICAS.c.replica_id ==
                        proof.replica_id))).scalar_one_or_none()
            association = connection.execute(
                sqlalchemy.select(associations.c.association_id).where(
                    associations.c.association_id == proof.association_id,
                    associations.c.service_name == proof.service_name,
                    associations.c.service_hash == proof.service_hash,
                    associations.c.service_lifecycle_epoch ==
                    proof.service_lifecycle_epoch,
                    associations.c.replica_id == proof.replica_id,
                    associations.c.replica_record_id == proof.replica_record_id,
                    associations.c.updated_at ==
                    proof.association_updated_at)).scalar_one_or_none()
            if (admission_exists is not None or replica_exists is not None or
                    association != proof.association_id):
                raise KueueAdmissionConflict(
                    'Materialized retirement did not preserve safe delete order.'
                )
            deleted = connection.execute(
                sqlalchemy.delete(_INTENTS).where(
                    _INTENTS.c.intent_idempotency_key ==
                    proof.intent_idempotency_key,
                    _INTENTS.c.service_name == proof.service_name,
                    _INTENTS.c.service_hash == proof.service_hash,
                    _INTENTS.c.service_lifecycle_epoch ==
                    proof.service_lifecycle_epoch,
                    _INTENTS.c.state == 'COMMITTED',
                    _INTENTS.c.replica_id == proof.replica_id,
                    _INTENTS.c.replica_record_id == proof.replica_record_id,
                    _INTENTS.c.updated_at == proof.intent_updated_at).returning(
                        _INTENTS.c.intent_idempotency_key)).scalar_one_or_none(
                        )
            if deleted != proof.intent_idempotency_key:
                raise KueueAdmissionConflict(
                    'Committed intent changed before retirement finalization.')
            deleted_keys.append(str(deleted))
        return tuple(deleted_keys)

    def validate_replacement_surge_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        *,
        intent_idempotency_key: str,
        expected_compatibility_sha256: str,
    ) -> KueueAdmissionRow:
        """Revalidate the frozen unit and compatibility of a surge lease."""
        _require_postgres_connection(connection)
        identity.validate()
        _require_sha256(intent_idempotency_key, 'intent_idempotency_key')
        _require_sha256(expected_compatibility_sha256,
                        'expected_compatibility_sha256')
        intent = self._lock_exact_intent(connection, identity,
                                         intent_idempotency_key)
        capacity_unit, planned_capacity = _canonical_capacity(intent, identity)
        row = self.get_for_intent_in_connection(connection,
                                                identity.service_name,
                                                intent_idempotency_key,
                                                for_update=True)
        if (row is None or row.unresolved_domain_sha256
                != identity.unresolved_domain_sha256 or
                row.capacity_unit != capacity_unit or
                row.planned_capacity != planned_capacity or
                row.replacement_surge_units > planned_capacity or
            (row.replacement_surge_units == 0 and
             row.replacement_compatibility_sha256 is not None) or
            (row.replacement_surge_units > 0 and
             row.replacement_compatibility_sha256
             != expected_compatibility_sha256)):
            raise KueueAdmissionConflict(
                'Replacement surge lost its immutable unit or compatibility.')
        return row

    def release_satisfied_replacement_surge_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        service_name: str,
        service_hash: str,
        capacity_unit: str,
        physical_capacity_debit: int,
        max_capacity: int,
    ) -> KueueAdmissionRow | None:
        """Release the lease after exact provider-clean capacity is <= max.

        ``physical_capacity_debit`` retains the existing caller-facing name,
        but its value is always expressed in ``capacity_unit``.  It may reflect
        paid drain or any independently proved provider-clean reduction.
        """
        _require_postgres_connection(connection)
        _require_nonempty(service_name, 'service_name')
        _require_nonempty(service_hash, 'service_hash')
        _require_capacity_unit(capacity_unit)
        _require_nonnegative(physical_capacity_debit, 'physical_capacity_debit')
        _require_nonnegative(max_capacity, 'max_capacity')
        if physical_capacity_debit > max_capacity:
            raise KueueAdmissionConflict(
                'Replacement surge remains required above the ceiling.')
        rows = self.lock_service_admissions_in_connection(
            connection, service_name, service_hash)
        leases = [row for row in rows if row.replacement_surge_units > 0]
        if not leases:
            return None
        if len(leases) != 1 or leases[0].capacity_unit != capacity_unit:
            raise KueueAdmissionConflict(
                'Replacement surge has a mismatched configured ceiling unit.')
        lease = leases[0]
        row = connection.execute(
            sqlalchemy.update(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key ==
                lease.intent_idempotency_key,
                _ADMISSIONS.c.replacement_surge_units ==
                lease.replacement_surge_units,
                _ADMISSIONS.c.replacement_compatibility_sha256 ==
                lease.replacement_compatibility_sha256).values(
                    replacement_surge_units=0,
                    replacement_compatibility_sha256=None,
                    updated_at=sqlalchemy.func.clock_timestamp()).returning(
                        *_ADMISSIONS.c)).mappings().one_or_none()
        if row is None:
            raise KueueAdmissionConflict(
                'Replacement surge changed before release.')
        return KueueAdmissionRow.from_mapping(row)

    @staticmethod
    def _validate_materialized_arguments(
        intent_idempotency_key: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
        provider_cluster_generation: int,
        association_id: uuid.UUID,
    ) -> None:
        _require_sha256(intent_idempotency_key, 'intent_idempotency_key')
        _require_positive(replica_id, 'replica_id')
        if not isinstance(replica_record_id, uuid.UUID):
            raise ValueError('replica_record_id must be a UUID.')
        _require_positive(provider_cluster_generation,
                          'provider_cluster_generation')
        if not isinstance(association_id, uuid.UUID):
            raise ValueError('association_id must be a UUID.')

    def bind_materialized_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        *,
        intent_idempotency_key: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
        provider_cluster_generation: int,
        association_id: uuid.UUID,
    ) -> KueueAdmissionRow:
        """CAS-bind the complete exact replica/association graph."""
        _require_postgres_connection(connection)
        identity.validate()
        self._validate_materialized_arguments(intent_idempotency_key,
                                              replica_id, replica_record_id,
                                              provider_cluster_generation,
                                              association_id)
        graph = _materialized_graph_predicate(identity, intent_idempotency_key,
                                              replica_id, replica_record_id,
                                              provider_cluster_generation,
                                              association_id)
        committed_intent = sqlalchemy.exists(
            sqlalchemy.select(sqlalchemy.literal(1)).where(
                *_intent_predicates(identity, intent_idempotency_key),
                _INTENTS.c.state == 'COMMITTED',
                _INTENTS.c.replica_id == replica_id,
                _INTENTS.c.replica_record_id == replica_record_id))
        row = connection.execute(
            sqlalchemy.update(_ADMISSIONS).where(
                *_identity_predicates(identity),
                _ADMISSIONS.c.intent_idempotency_key == intent_idempotency_key,
                _ADMISSIONS.c.state == KueueAdmissionState.INTENT_PENDING.value,
                _ADMISSIONS.c.replica_id.is_(None), committed_intent,
                graph).values(
                    replica_id=replica_id,
                    replica_record_id=replica_record_id,
                    provider_cluster_generation=provider_cluster_generation,
                    association_id=association_id,
                    updated_at=sqlalchemy.func.clock_timestamp()).returning(
                        *_ADMISSIONS.c)).mappings().one_or_none()
        if row is not None:
            return KueueAdmissionRow.from_mapping(row)
        replay = self.validate_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=intent_idempotency_key,
            replica_id=replica_id,
            replica_record_id=replica_record_id,
            provider_cluster_generation=provider_cluster_generation,
            association_id=association_id)
        if replay is None:
            raise KueueAdmissionConflict(
                'Materialized intent, replica, association, or admission '
                'changed.')
        return replay

    def validate_materialized_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        *,
        intent_idempotency_key: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
        provider_cluster_generation: int,
        association_id: uuid.UUID,
    ) -> KueueAdmissionRow | None:
        """Return a row only while its complete exact live graph holds."""
        _require_postgres_connection(connection)
        identity.validate()
        self._validate_materialized_arguments(intent_idempotency_key,
                                              replica_id, replica_record_id,
                                              provider_cluster_generation,
                                              association_id)
        committed_intent = sqlalchemy.exists(
            sqlalchemy.select(sqlalchemy.literal(1)).where(
                *_intent_predicates(identity, intent_idempotency_key),
                _INTENTS.c.state == 'COMMITTED',
                _INTENTS.c.replica_id == replica_id,
                _INTENTS.c.replica_record_id == replica_record_id))
        row = connection.execute(
            sqlalchemy.select(_ADMISSIONS).where(
                *_identity_predicates(identity),
                _ADMISSIONS.c.intent_idempotency_key == intent_idempotency_key,
                _ADMISSIONS.c.replica_id == replica_id,
                _ADMISSIONS.c.replica_record_id == replica_record_id,
                _ADMISSIONS.c.provider_cluster_generation ==
                provider_cluster_generation,
                _ADMISSIONS.c.association_id == association_id,
                committed_intent,
                _materialized_graph_predicate(identity, intent_idempotency_key,
                                              replica_id, replica_record_id,
                                              provider_cluster_generation,
                                              association_id)).with_for_update(
                                              )).mappings().one_or_none()
        return None if row is None else KueueAdmissionRow.from_mapping(row)

    def observe_pod_waiting_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        *,
        intent_idempotency_key: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
        provider_cluster_generation: int,
        association_id: uuid.UUID,
        pod_namespace: str,
        pod_name: str,
        pod_uid: str,
        pod_receipt: Mapping[str, Any],
        provider_read_started_at: datetime.datetime,
        ttl_seconds: int = WAITING_OBSERVATION_TTL_SECONDS,
    ) -> KueueAdmissionRow:
        """Create or renew one exact gated-Pod receipt using database time."""
        if ttl_seconds != WAITING_OBSERVATION_TTL_SECONDS:
            raise ValueError(
                'Kueue waiting receipt TTL is fixed at 15 seconds.')
        materialized = self.validate_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=intent_idempotency_key,
            replica_id=replica_id,
            replica_record_id=replica_record_id,
            provider_cluster_generation=provider_cluster_generation,
            association_id=association_id)
        if materialized is None:
            raise KueueAdmissionConflict(
                'Pod observation lacks an exact materialized graph.')
        for field, value in (('pod_namespace', pod_namespace),
                             ('pod_name', pod_name), ('pod_uid', pod_uid)):
            _require_nonempty(value, field)
        _validate_receipt(pod_receipt,
                          state=KueueAdmissionState.POD_WAITING,
                          identity=identity,
                          intent_idempotency_key=intent_idempotency_key,
                          replica_record_id=replica_record_id,
                          pod_namespace=pod_namespace,
                          pod_name=pod_name,
                          pod_uid=pod_uid)
        if materialized.state is KueueAdmissionState.POLICY_ADMITTED:
            raise KueueAdmissionConflict(
                'Policy admission cannot transition back to waiting.')
        if materialized.state is KueueAdmissionState.POD_WAITING and (
                materialized.pod_namespace != pod_namespace or
                materialized.pod_name != pod_name or materialized.pod_uid
                != pod_uid or materialized.pod_receipt != dict(pod_receipt)):
            raise KueueAdmissionConflict(
                'Waiting receipt changed its exact Pod facts.')
        receipt, receipt_sha256 = _receipt_expression(pod_receipt)
        observed_at, valid_until = _validate_provider_read_started_at(
            connection, provider_read_started_at)
        if (materialized.observed_at is not None and
                observed_at < materialized.observed_at):
            raise KueueAdmissionConflict(
                'Provider-read token regressed the admission observation.')
        row = connection.execute(
            sqlalchemy.update(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key == intent_idempotency_key,
                _ADMISSIONS.c.state == materialized.state.value,
                _ADMISSIONS.c.updated_at == materialized.updated_at,
                sqlalchemy.func.clock_timestamp() < valid_until).values(
                    state=KueueAdmissionState.POD_WAITING.value,
                    pod_namespace=pod_namespace,
                    pod_name=pod_name,
                    pod_uid=pod_uid,
                    pod_receipt=receipt,
                    pod_receipt_sha256=receipt_sha256,
                    observed_at=observed_at,
                    valid_until=valid_until,
                    updated_at=sqlalchemy.func.clock_timestamp()).returning(
                        *_ADMISSIONS.c)).mappings().one_or_none()
        if row is None:
            raise KueueAdmissionConflict(
                'Pod waiting receipt changed before publication.')
        _validate_provider_publication_completed_at(connection, valid_until)
        return KueueAdmissionRow.from_mapping(row)

    def observe_policy_admitted_in_connection(
        self,
        connection: sqlalchemy.engine.Connection,
        identity: KueueAdmissionIdentity,
        *,
        intent_idempotency_key: str,
        replica_id: int,
        replica_record_id: uuid.UUID,
        provider_cluster_generation: int,
        association_id: uuid.UUID,
        pod_namespace: str,
        pod_name: str,
        pod_uid: str,
        pod_receipt: Mapping[str, Any],
        provider_read_started_at: datetime.datetime,
    ) -> KueueAdmissionRow:
        """Publish the monotonic admitted fact for one exact Pod UID."""
        materialized = self.validate_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=intent_idempotency_key,
            replica_id=replica_id,
            replica_record_id=replica_record_id,
            provider_cluster_generation=provider_cluster_generation,
            association_id=association_id)
        if materialized is None:
            raise KueueAdmissionConflict(
                'Admission observation lacks an exact materialized graph.')
        for field, value in (('pod_namespace', pod_namespace),
                             ('pod_name', pod_name), ('pod_uid', pod_uid)):
            _require_nonempty(value, field)
        _validate_receipt(pod_receipt,
                          state=KueueAdmissionState.POLICY_ADMITTED,
                          identity=identity,
                          intent_idempotency_key=intent_idempotency_key,
                          replica_record_id=replica_record_id,
                          pod_namespace=pod_namespace,
                          pod_name=pod_name,
                          pod_uid=pod_uid)
        if materialized.pod_uid is not None and (
                materialized.pod_namespace != pod_namespace or
                materialized.pod_name != pod_name or
                materialized.pod_uid != pod_uid):
            raise KueueAdmissionConflict(
                'Admitted receipt changed the exact Pod identity.')
        new_receipt = copy.deepcopy(dict(pod_receipt))
        observed_at, valid_until = _validate_provider_read_started_at(
            connection, provider_read_started_at)
        if (materialized.observed_at is not None and
                observed_at < materialized.observed_at):
            raise KueueAdmissionConflict(
                'Provider-read token regressed the admission observation.')
        if materialized.state is KueueAdmissionState.POLICY_ADMITTED:
            if (materialized.pod_receipt != new_receipt and
                (materialized.pod_receipt is None or
                 not _admitted_receipt_refresh_allowed(materialized.pod_receipt,
                                                       new_receipt))):
                raise KueueAdmissionConflict(
                    'Admitted Pod receipt changed immutable or monotonic facts.'
                )
        receipt, receipt_sha256 = _receipt_expression(new_receipt)
        admitted_at = materialized.admitted_at or observed_at
        row = connection.execute(
            sqlalchemy.update(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key == intent_idempotency_key,
                _ADMISSIONS.c.state == materialized.state.value,
                _ADMISSIONS.c.updated_at == materialized.updated_at,
                sqlalchemy.func.clock_timestamp() < valid_until).values(
                    state=KueueAdmissionState.POLICY_ADMITTED.value,
                    pod_namespace=pod_namespace,
                    pod_name=pod_name,
                    pod_uid=pod_uid,
                    pod_receipt=receipt,
                    pod_receipt_sha256=receipt_sha256,
                    observed_at=observed_at,
                    valid_until=None,
                    admitted_at=admitted_at,
                    updated_at=sqlalchemy.func.clock_timestamp()).returning(
                        *_ADMISSIONS.c)).mappings().one_or_none()
        if row is None:
            raise KueueAdmissionConflict(
                'Policy-admitted fact changed before publication.')
        _validate_provider_publication_completed_at(connection, valid_until)
        return KueueAdmissionRow.from_mapping(row)
