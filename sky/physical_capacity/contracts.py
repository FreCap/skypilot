"""Pure contracts for the bounded physical-capacity evidence scan.

These objects contain no persistence or provider behavior.  They make the C2
mapping vocabulary closed and keep selector and finding validation usable by
both source adapters and the scan repository.
"""

from __future__ import annotations

import dataclasses
import enum
import re
from typing import TypeAlias

from sky.physical_capacity import canonical
from sky.physical_capacity import models
from sky.skylet import constants as skylet_constants

MAPPING_VERSION = 1
_LOWERCASE_SHA256 = re.compile(r'^[0-9a-f]{64}$')
_MIN_SIGNED_64_BIT_INTEGER = -(1 << 63)
_MAX_SIGNED_64_BIT_INTEGER = (1 << 63) - 1


def validate_workspace(value: object) -> str:
    """Validate a workspace with both canonical and public-name bounds."""
    workspace = canonical.validate_bounded_string(
        value,
        max_bytes=canonical.MAX_WORKSPACE_IDENTIFIER_BYTES,
        field='Selector workspace')
    if (len(workspace) > skylet_constants.WORKSPACE_NAME_MAX_LENGTH or
            re.fullmatch(skylet_constants.WORKSPACE_NAME_VALID_REGEX,
                         workspace) is None):
        raise ValueError(f'Invalid selector workspace {workspace!r}.')
    return workspace


def _validate_source_identifier(value: object, *, field: str) -> str:
    return canonical.validate_bounded_string(
        value, max_bytes=canonical.MAX_SOURCE_IDENTIFIER_BYTES, field=field)


def _validate_integer(value: object,
                      *,
                      field: str,
                      minimum: int = _MIN_SIGNED_64_BIT_INTEGER) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f'{field} must be an integer.')
    if not minimum <= value <= _MAX_SIGNED_64_BIT_INTEGER:
        raise ValueError(f'{field} must be between {minimum} and '
                         f'{_MAX_SIGNED_64_BIT_INTEGER}.')
    return value


def _validate_hash(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _LOWERCASE_SHA256.fullmatch(value) is None:
        raise ValueError(f'{field} must be a lowercase SHA-256 digest.')
    return value


@dataclasses.dataclass(frozen=True)
class ServeSourceSelector:
    """One explicitly selected SkyServe service or pool."""

    workspace: str
    source_kind: models.ProjectionSourceKind
    service_name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, 'workspace',
                           validate_workspace(self.workspace))
        try:
            source_kind = models.ProjectionSourceKind(self.source_kind)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f'Invalid Serve source kind {self.source_kind!r}.') from e
        if source_kind not in (models.ProjectionSourceKind.SERVE_SERVICE,
                               models.ProjectionSourceKind.SERVE_POOL):
            raise ValueError('Serve selectors require serve_service or '
                             'serve_pool source kind.')
        object.__setattr__(self, 'source_kind', source_kind)
        object.__setattr__(
            self, 'service_name',
            _validate_source_identifier(self.service_name,
                                        field='Selector service_name'))

    def to_payload(self) -> dict[str, object]:
        return {
            'workspace': self.workspace,
            'source_kind': self.source_kind.value,
            'service_name': self.service_name,
        }


@dataclasses.dataclass(frozen=True)
class ManagedJobTaskSelector:
    """One explicitly selected consolidated managed-job task."""

    workspace: str
    spot_job_id: int
    task_id: int
    source_kind: models.ProjectionSourceKind = dataclasses.field(
        default=models.ProjectionSourceKind.MANAGED_JOB_TASK, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, 'workspace',
                           validate_workspace(self.workspace))
        object.__setattr__(
            self, 'spot_job_id',
            _validate_integer(self.spot_job_id,
                              field='Selector spot_job_id',
                              minimum=1))
        object.__setattr__(
            self, 'task_id',
            _validate_integer(self.task_id, field='Selector task_id',
                              minimum=0))

    def to_payload(self) -> dict[str, object]:
        return {
            'workspace': self.workspace,
            'source_kind': self.source_kind.value,
            'spot_job_id': self.spot_job_id,
            'task_id': self.task_id,
        }


SourceSelector: TypeAlias = ServeSourceSelector | ManagedJobTaskSelector


@dataclasses.dataclass(frozen=True)
class SourcePartition:
    """A bounded scan partition independent of its configured selector set."""

    workspace: str
    source_kind: models.ProjectionSourceKind

    def __post_init__(self) -> None:
        object.__setattr__(self, 'workspace',
                           validate_workspace(self.workspace))
        try:
            source_kind = models.ProjectionSourceKind(self.source_kind)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f'Invalid projection source kind {self.source_kind!r}.') from e
        object.__setattr__(self, 'source_kind', source_kind)

    def to_payload(self) -> dict[str, object]:
        return {
            'mapping_version': MAPPING_VERSION,
            'workspace': self.workspace,
            'source_kind': self.source_kind.value,
        }


def selector_payload(selector: SourceSelector) -> dict[str, object]:
    """Return the strict JSON selector object."""
    if not isinstance(selector, (ServeSourceSelector, ManagedJobTaskSelector)):
        raise TypeError('selector must be a typed source selector.')
    return selector.to_payload()


def selector_partition(selector: SourceSelector) -> SourcePartition:
    """Return the physical-capacity partition containing ``selector``."""
    return SourcePartition(selector.workspace, selector.source_kind)


def owner_kind_for_selector(selector: SourceSelector) -> models.OwnerKind:
    """Map a selector to its C1 owner-kind allowlist value."""
    if isinstance(selector, ManagedJobTaskSelector):
        return models.OwnerKind.MANAGED_JOB_TASK
    if isinstance(selector, ServeSourceSelector):
        if selector.source_kind is models.ProjectionSourceKind.SERVE_SERVICE:
            return models.OwnerKind.SERVICE
        return models.OwnerKind.POOL
    raise TypeError('selector must be a typed source selector.')


class EvidenceRecordType(str, enum.Enum):
    GROUP = 'group'
    ALLOCATION_CANDIDATE = 'allocation_candidate'


class EvidenceGroupConfidence(str, enum.Enum):
    EXACT = 'exact'
    LEGACY = 'legacy'
    UNKNOWN = 'unknown'


class EvidenceIdentityConfidence(str, enum.Enum):
    LEGACY = 'legacy'
    UNKNOWN = 'unknown'


class EvidenceLifecycle(str, enum.Enum):
    ACTIVE = 'active'
    RETIRING = 'retiring'
    UNKNOWN = 'unknown'


class EvidenceStatusClass(str, enum.Enum):
    PRESENT = 'present'
    ABSENT = 'absent'
    UNKNOWN = 'unknown'


class EvidenceAssociationStatus(str, enum.Enum):
    REGISTRY_HASH = 'registry_hash'
    REGISTRY_MISSING = 'registry_missing'
    REGISTRY_UNSAFE = 'registry_unsafe'
    SOURCE_MALFORMED = 'source_malformed'


class EvidenceDesiredState(str, enum.Enum):
    PRESENT = 'present'
    ABSENT = 'absent'
    UNKNOWN = 'unknown'


class EvidenceObservedState(str, enum.Enum):
    UNKNOWN = 'unknown'
    PROVISIONING = 'provisioning'
    UP = 'up'
    STOPPED = 'stopped'
    PARTIAL = 'partial'


@dataclasses.dataclass(frozen=True)
class GroupEvidenceRecord:
    """Ephemeral normalized evidence for one selected logical owner."""

    source_incarnation_hash: str
    confidence: EvidenceGroupConfidence
    lifecycle: EvidenceLifecycle
    status_class: EvidenceStatusClass
    record_type: EvidenceRecordType = dataclasses.field(
        default=EvidenceRecordType.GROUP, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'source_incarnation_hash',
            _validate_hash(self.source_incarnation_hash,
                           field='Group source_incarnation_hash'))
        object.__setattr__(self, 'confidence',
                           EvidenceGroupConfidence(self.confidence))
        object.__setattr__(self, 'lifecycle', EvidenceLifecycle(self.lifecycle))
        object.__setattr__(self, 'status_class',
                           EvidenceStatusClass(self.status_class))

    def to_payload(self) -> dict[str, object]:
        return {
            'mapping_version': MAPPING_VERSION,
            'record_type': self.record_type.value,
            'source_incarnation_hash': self.source_incarnation_hash,
            'confidence': self.confidence.value,
            'lifecycle': self.lifecycle.value,
            'status_class': self.status_class.value,
        }


@dataclasses.dataclass(frozen=True)
class AllocationCandidateEvidenceRecord:
    """Ephemeral normalized evidence for one physical-allocation candidate."""

    source_incarnation_hash: str
    group_source_incarnation_hash: str
    identity_confidence: EvidenceIdentityConfidence
    association_status: EvidenceAssociationStatus
    desired_state: EvidenceDesiredState
    observed_state: EvidenceObservedState
    scalar_placement_hash: str | None = None
    record_type: EvidenceRecordType = dataclasses.field(
        default=EvidenceRecordType.ALLOCATION_CANDIDATE, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'source_incarnation_hash',
            _validate_hash(self.source_incarnation_hash,
                           field='Allocation source_incarnation_hash'))
        object.__setattr__(
            self, 'group_source_incarnation_hash',
            _validate_hash(self.group_source_incarnation_hash,
                           field='Allocation group_source_incarnation_hash'))
        object.__setattr__(self, 'identity_confidence',
                           EvidenceIdentityConfidence(self.identity_confidence))
        object.__setattr__(self, 'association_status',
                           EvidenceAssociationStatus(self.association_status))
        object.__setattr__(self, 'desired_state',
                           EvidenceDesiredState(self.desired_state))
        object.__setattr__(self, 'observed_state',
                           EvidenceObservedState(self.observed_state))
        if self.scalar_placement_hash is not None:
            object.__setattr__(
                self, 'scalar_placement_hash',
                _validate_hash(self.scalar_placement_hash,
                               field='Allocation scalar_placement_hash'))

    def to_payload(self) -> dict[str, object]:
        return {
            'mapping_version': MAPPING_VERSION,
            'record_type': self.record_type.value,
            'source_incarnation_hash': self.source_incarnation_hash,
            'group_source_incarnation_hash': self.group_source_incarnation_hash,
            'identity_confidence': self.identity_confidence.value,
            'association_status': self.association_status.value,
            'desired_state': self.desired_state.value,
            'observed_state': self.observed_state.value,
            'scalar_placement_hash': self.scalar_placement_hash,
        }


EvidenceRecord: TypeAlias = (GroupEvidenceRecord |
                             AllocationCandidateEvidenceRecord)


class FindingKey(str, enum.Enum):
    """All and only the keys stored for a completed C2 scan."""

    SOURCE_ROWS = 'source_rows'
    SELECTORS_PRESENT = 'selectors_present'
    SELECTORS_MISSING = 'selectors_missing'
    GROUPS_EXACT = 'groups_exact'
    GROUPS_LEGACY = 'groups_legacy'
    GROUPS_UNKNOWN = 'groups_unknown'
    ALLOCATION_CANDIDATES = 'allocation_candidates'
    ALLOCATIONS_EXACT = 'allocations_exact'
    ALLOCATIONS_LEGACY = 'allocations_legacy'
    ALLOCATIONS_UNKNOWN = 'allocations_unknown'
    IDENTITY_GAP = 'identity_gap'
    NO_CLUSTER_YET = 'no_cluster_yet'
    SCALAR_PLACEMENT_KNOWN = 'scalar_placement_known'
    SELECTED_SPEC_GAP = 'selected_spec_gap'
    DESIRED_PRESENT = 'desired_present'
    DESIRED_ABSENT = 'desired_absent'
    DESIRED_UNKNOWN = 'desired_unknown'
    SOURCE_CONFLICT = 'source_conflict'
    POOL_ASSIGNMENT_UNFENCED = 'pool_assignment_unfenced'
    POOL_ASSIGNMENT_AMBIGUOUS = 'pool_assignment_ambiguous'


@dataclasses.dataclass
class FindingCounts:
    """The exact committed C2 finding shape and its closed arithmetic."""

    source_rows: int = 0
    selectors_present: int = 0
    selectors_missing: int = 0
    groups_exact: int = 0
    groups_legacy: int = 0
    groups_unknown: int = 0
    allocation_candidates: int = 0
    allocations_exact: int = 0
    allocations_legacy: int = 0
    allocations_unknown: int = 0
    identity_gap: int = 0
    no_cluster_yet: int = 0
    scalar_placement_known: int = 0
    selected_spec_gap: int = 0
    desired_present: int = 0
    desired_absent: int = 0
    desired_unknown: int = 0
    source_conflict: int = 0
    pool_assignment_unfenced: int = 0
    pool_assignment_ambiguous: int = 0

    def increment(self, key: FindingKey | str, amount: int = 1) -> None:
        """Increment one closed finding without accepting Boolean integers."""
        try:
            normalized_key = FindingKey(key)
        except (TypeError, ValueError) as e:
            raise ValueError(f'Unknown finding key {key!r}.') from e
        _validate_integer(amount, field='Finding increment', minimum=0)
        current = getattr(self, normalized_key.value)
        updated = current + amount
        _validate_integer(updated, field=normalized_key.value, minimum=0)
        setattr(self, normalized_key.value, updated)

    def to_dict(self) -> dict[str, int]:
        """Return all and only the required persisted finding keys."""
        result = dataclasses.asdict(self)
        for key, value in result.items():
            _validate_integer(value, field=key, minimum=0)
        if set(result) != {member.value for member in FindingKey}:
            raise ValueError('FindingCounts fields do not match FindingKey.')
        return result

    def validate(self, configured_selectors: int) -> None:
        """Validate the exact per-completed-partition arithmetic."""
        _validate_integer(configured_selectors,
                          field='configured_selectors',
                          minimum=0)
        self.to_dict()
        if self.selectors_present + self.selectors_missing != (
                configured_selectors):
            raise ValueError('Selector finding arithmetic does not close.')
        if (self.groups_exact + self.groups_legacy + self.groups_unknown
                != self.selectors_present):
            raise ValueError('Group finding arithmetic does not close.')
        if (self.allocations_exact + self.allocations_legacy +
                self.allocations_unknown != self.allocation_candidates):
            raise ValueError('Allocation finding arithmetic does not close.')
        if (self.desired_present + self.desired_absent + self.desired_unknown
                != self.allocation_candidates):
            raise ValueError('Desired-state finding arithmetic does not close.')
        if self.identity_gap != self.allocation_candidates:
            raise ValueError('Identity-gap finding arithmetic does not close.')
        if self.selected_spec_gap != self.allocation_candidates:
            raise ValueError(
                'Selected-spec-gap finding arithmetic does not close.')
        if self.scalar_placement_known > self.allocations_legacy:
            raise ValueError('Scalar placement exceeds legacy allocations.')
        if self.allocations_exact != 0:
            raise ValueError('Mapping version 1 has no exact allocations.')


@dataclasses.dataclass(frozen=True)
class PartitionEvidenceResult:
    """One bounded adapter result before scan publication."""

    records: tuple[EvidenceRecord, ...]
    findings: FindingCounts
    rows_seen: int

    def validate(self, configured_selectors: int) -> None:
        _validate_integer(self.rows_seen, field='rows_seen', minimum=0)
        self.findings.validate(configured_selectors)
        if self.rows_seen < self.findings.source_rows:
            raise ValueError('rows_seen must include every source row.')
        for record in self.records:
            if not isinstance(
                    record,
                (GroupEvidenceRecord, AllocationCandidateEvidenceRecord)):
                raise ValueError('Partition evidence contains an unknown DTO.')
        if len(self.records) != (self.findings.selectors_present +
                                 self.findings.allocation_candidates):
            raise ValueError('Evidence record count does not match findings.')
