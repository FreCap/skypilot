"""Pure, validated state for bounded SkyServe system-OOM recovery.

This module deliberately owns no I/O.  Replica managers reduce trusted remote
observations here, then persist the returned state through the owner-fenced
Serve-state primitives.
"""

import dataclasses
import enum
import math
import re
from typing import Any
import uuid

from sky.skylet import job_lib

CONTROLLER_RECOVERY_CONTRACT_VERSION = 2
RECOVERY_AUTHORIZATION_VERSION = 3
RUNTIME_RECOVERY_PROFILE_VERSION = 2
SYSTEM_RECOVERY_CAPABILITY = 'subreaper-v2+owned-local-docker-v1'
CANDIDATE_RELEASE_GUARD_SECONDS = 35.0

_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_IMAGE_DIGEST_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_NONCE_RE = re.compile(r'^[0-9a-f]{64}$')


class RecoveryStateError(ValueError):
    """A recovery observation or persisted state is invalid."""


class RemoteRecoveryPhase(str, enum.Enum):
    """Durable phases emitted by the replica-local recovery session."""

    ARMED = 'ARMED'
    WAITING_CLEANUP = 'WAITING_CLEANUP'
    WAITING_MEMORY = 'WAITING_MEMORY'
    RESUBMITTING = 'RESUBMITTING'
    RETRY_SUBMITTED = 'RETRY_SUBMITTED'
    EXHAUSTED = 'EXHAUSTED'


class ControllerRecoveryState(str, enum.Enum):
    """Controller projection of one driver-owned recovery session."""

    ARMED = 'ARMED'
    RECOVERING = 'RECOVERING'
    RETRY_SUBMITTED = 'RETRY_SUBMITTED'
    RECOVERED = 'RECOVERED'
    EXHAUSTED = 'EXHAUSTED'


class SystemRecoveryDisposition(str, enum.Enum):
    """Launch disposition for one replica generation."""

    ORDINARY = 'ORDINARY'
    CANDIDATE = 'CANDIDATE'
    CAPABLE = 'CAPABLE'


class RecoveryQuarantineReason(str, enum.Enum):
    """Bounded reason codes safe to persist and log without row contents."""

    PARTIAL_V13_BUNDLE = 'PARTIAL_V13_BUNDLE'
    MALFORMED_V13_BUNDLE = 'MALFORMED_V13_BUNDLE'
    INCONSISTENT_V13_BUNDLE = 'INCONSISTENT_V13_BUNDLE'


_PHASE_ORDER = {
    RemoteRecoveryPhase.ARMED: 0,
    RemoteRecoveryPhase.WAITING_CLEANUP: 1,
    RemoteRecoveryPhase.WAITING_MEMORY: 2,
    RemoteRecoveryPhase.RESUBMITTING: 3,
    RemoteRecoveryPhase.RETRY_SUBMITTED: 4,
    RemoteRecoveryPhase.EXHAUSTED: 5,
}

_CAPABILITY_PROFILE_VERSIONS = {
    # Read-only transition compatibility.  Authorization v3 never selects v1.
    'subreaper-v1+local-docker-empty-inventory-v1': 1,
    SYSTEM_RECOVERY_CAPABILITY: RUNTIME_RECOVERY_PROFILE_VERSION,
}


def _strict_positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RecoveryStateError(f'{name} must be a positive integer.')
    return value


def _nonnegative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RecoveryStateError(f'{name} must be a nonnegative integer.')
    return value


def _finite_timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RecoveryStateError(f'{name} must be a positive timestamp.')
    try:
        timestamp = float(value)
    except (OverflowError, TypeError, ValueError) as e:
        raise RecoveryStateError(f'{name} must be a positive timestamp.') from e
    if not math.isfinite(timestamp) or timestamp <= 0:
        raise RecoveryStateError(f'{name} must be a positive timestamp.')
    return timestamp


def _optional_timestamp(value: object, name: str) -> float | None:
    if value is None:
        return None
    return _finite_timestamp(value, name)


def _positive_duration(value: object,
                       name: str,
                       *,
                       maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RecoveryStateError(f'{name} must be positive.')
    try:
        duration = float(value)
    except (OverflowError, TypeError, ValueError) as e:
        raise RecoveryStateError(f'{name} must be positive.') from e
    if (not math.isfinite(duration) or duration <= 0 or
        (maximum is not None and duration > maximum)):
        raise RecoveryStateError(f'{name} must be positive.')
    return duration


def _nonempty(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise RecoveryStateError(f'{name} must be a nonempty string.')
    return value


def _text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise RecoveryStateError(f'{name} must be a string.')
    return value


def _optional_nonempty(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, name)


def _uuid(value: object, name: str) -> str:
    text = _nonempty(value, name)
    try:
        parsed = uuid.UUID(text)
    except ValueError as e:
        raise RecoveryStateError(f'{name} must be a UUID.') from e
    if str(parsed) != text:
        raise RecoveryStateError(f'{name} must be a canonical UUID.')
    return text


def _optional_uuid(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _uuid(value, name)


def _sha256(value: object, name: str) -> str:
    text = _nonempty(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise RecoveryStateError(f'{name} must be a lowercase SHA-256.')
    return text


def _runtime_image_digest(value: object) -> str:
    text = _nonempty(value, 'runtime_image_digest')
    if _IMAGE_DIGEST_RE.fullmatch(text) is None:
        raise RecoveryStateError(
            'runtime_image_digest must be a sha256 image digest.')
    return text


def normalize_status_barrier_started_at(value: object, *,
                                        trusted_now: float) -> float:
    """Return a conservative durable anchor for capable-startup fencing."""
    trusted_now = _finite_timestamp(trusted_now, 'trusted_now')
    try:
        anchor = _finite_timestamp(value,
                                   'system_recovery_status_barrier_started_at')
    except RecoveryStateError:
        return trusted_now
    return min(anchor, trusted_now)


def next_recovery_revision(current: object) -> int:
    """Validate and increment one recovery-subdocument revision."""
    return _nonnegative_int(current, 'system_recovery_revision') + 1


@dataclasses.dataclass(frozen=True)
class SystemRecoveryLaunchIntent:
    """Closed historical authorization intent for one replica generation."""

    version: int
    controller_contract_version: int
    recovery_authorization_version: int
    recovery_authorization_profile_id: str
    recovery_authorization_sha256: str
    runtime_profile_version: int
    expected_runtime_capability: str
    service_hash: str
    replica_id: int
    launch_generation: int
    launch_nonce: str
    workspace: str
    resource_envelope_sha256: str
    task_sha256: str
    runtime_image_digest: str
    owned_container_spec_sha256: str
    execution_envelope_sha256: str

    def __post_init__(self) -> None:
        if self.version != 1 or isinstance(self.version, bool):
            raise RecoveryStateError('Unknown recovery launch intent version.')
        if (self.controller_contract_version
                != CONTROLLER_RECOVERY_CONTRACT_VERSION or
                isinstance(self.controller_contract_version, bool)):
            raise RecoveryStateError(
                'Unknown controller recovery contract version.')
        if (self.recovery_authorization_version
                != RECOVERY_AUTHORIZATION_VERSION or
                isinstance(self.recovery_authorization_version, bool)):
            raise RecoveryStateError('Unknown recovery authorization version.')
        if (self.runtime_profile_version != RUNTIME_RECOVERY_PROFILE_VERSION or
                isinstance(self.runtime_profile_version, bool)):
            raise RecoveryStateError('Unknown runtime recovery profile.')
        if self.expected_runtime_capability != SYSTEM_RECOVERY_CAPABILITY:
            raise RecoveryStateError('Unknown expected runtime capability.')
        _nonempty(self.recovery_authorization_profile_id,
                  'recovery_authorization_profile_id')
        _sha256(self.recovery_authorization_sha256,
                'recovery_authorization_sha256')
        _nonempty(self.service_hash, 'service_hash')
        _strict_positive_int(self.replica_id, 'replica_id')
        _strict_positive_int(self.launch_generation, 'launch_generation')
        nonce = _nonempty(self.launch_nonce, 'launch_nonce')
        if _NONCE_RE.fullmatch(nonce) is None:
            raise RecoveryStateError(
                'launch_nonce must be 64 lowercase hexadecimal characters.')
        _text(self.workspace, 'workspace')
        _sha256(self.resource_envelope_sha256, 'resource_envelope_sha256')
        _sha256(self.task_sha256, 'task_sha256')
        _runtime_image_digest(self.runtime_image_digest)
        _sha256(self.owned_container_spec_sha256, 'owned_container_spec_sha256')
        _sha256(self.execution_envelope_sha256, 'execution_envelope_sha256')

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, payload: object) -> 'SystemRecoveryLaunchIntent':
        if not isinstance(payload, dict):
            raise RecoveryStateError(
                'system_recovery_launch_intent must be an object.')
        expected = {field.name for field in dataclasses.fields(cls)}
        if set(payload) != expected:
            raise RecoveryStateError(
                'system_recovery_launch_intent has invalid fields.')
        try:
            return cls(**payload)
        except TypeError as e:
            raise RecoveryStateError(
                f'Invalid system recovery launch intent: {e}') from e


@dataclasses.dataclass(frozen=True)
class SystemRecoveryQuarantine:
    """Reason-only marker for an isolated malformed v13 recovery bundle."""

    reason: RecoveryQuarantineReason

    def __post_init__(self) -> None:
        if not isinstance(self.reason, RecoveryQuarantineReason):
            raise RecoveryStateError('Invalid recovery quarantine reason.')

    def to_dict(self) -> dict[str, str]:
        return {'reason': self.reason.value}

    @classmethod
    def from_dict(cls, payload: object) -> 'SystemRecoveryQuarantine':
        if not isinstance(payload, dict) or set(payload) != {'reason'}:
            raise RecoveryStateError('system_recovery_quarantine is invalid.')
        try:
            return cls(reason=RecoveryQuarantineReason(payload['reason']))
        except (TypeError, ValueError) as e:
            raise RecoveryStateError(
                'system_recovery_quarantine has an invalid reason.') from e


@dataclasses.dataclass(frozen=True)
class RecoveryObservation:
    """One validated remote recovery record for the exact service job."""

    job_id: int
    capability: str
    phase: RemoteRecoveryPhase
    original_attempt_id: str
    replacement_attempt_id: str | None
    node_boot_id: str
    occurrence_count: int
    armed_at: float
    updated_at: float
    event_id: str | None = None
    reason: str | None = None
    occurred_at: float | None = None
    deadline_at: float | None = None

    def __post_init__(self) -> None:
        _strict_positive_int(self.job_id, 'job_id')
        if not isinstance(self.phase, RemoteRecoveryPhase):
            raise RecoveryStateError('Recovery phase has an invalid type.')
        if (not isinstance(self.capability, str) or
                self.capability not in _CAPABILITY_PROFILE_VERSIONS):
            raise RecoveryStateError('Unknown recovery capability.')
        _uuid(self.original_attempt_id, 'original_attempt_id')
        _optional_uuid(self.replacement_attempt_id, 'replacement_attempt_id')
        if self.replacement_attempt_id == self.original_attempt_id:
            raise RecoveryStateError(
                'replacement_attempt_id must differ from original_attempt_id.')
        _nonempty(self.node_boot_id, 'node_boot_id')
        _nonnegative_int(self.occurrence_count, 'occurrence_count')
        armed_at = _finite_timestamp(self.armed_at, 'armed_at')
        updated_at = _finite_timestamp(self.updated_at, 'updated_at')
        if updated_at < armed_at:
            raise RecoveryStateError('updated_at precedes armed_at.')
        _optional_uuid(self.event_id, 'event_id')
        _optional_nonempty(self.reason, 'reason')
        occurred_at = _optional_timestamp(self.occurred_at, 'occurred_at')
        deadline_at = _optional_timestamp(self.deadline_at, 'deadline_at')

        if self.phase == RemoteRecoveryPhase.ARMED:
            if (self.occurrence_count != 0 or self.event_id is not None or
                    self.reason is not None or occurred_at is not None or
                    deadline_at is not None or
                    self.replacement_attempt_id is not None):
                raise RecoveryStateError('ARMED observation has event state.')
            return

        if (self.phase == RemoteRecoveryPhase.EXHAUSTED and
                self.occurrence_count == 0):
            if any(value is not None
                   for value in (self.event_id, self.reason, occurred_at,
                                 deadline_at, self.replacement_attempt_id)):
                raise RecoveryStateError(
                    'Eventless EXHAUSTED observation has event state.')
            return

        if (self.occurrence_count < 1 or self.event_id is None or
                self.reason != 'RAY_NODE_OOM' or occurred_at is None or
                deadline_at is None):
            raise RecoveryStateError(
                'Post-OOM observation is missing event state.')
        if deadline_at < occurred_at:
            raise RecoveryStateError('Recovery deadline precedes occurrence.')
        replacement_required = self.phase in (
            RemoteRecoveryPhase.RESUBMITTING,
            RemoteRecoveryPhase.RETRY_SUBMITTED)
        if replacement_required and self.replacement_attempt_id is None:
            raise RecoveryStateError(
                f'{self.phase.value} requires replacement_attempt_id.')
        if (self.phase in (RemoteRecoveryPhase.WAITING_CLEANUP,
                           RemoteRecoveryPhase.WAITING_MEMORY) and
                self.replacement_attempt_id is not None):
            raise RecoveryStateError(
                f'{self.phase.value} cannot have replacement_attempt_id.')

    @property
    def profile_version(self) -> int:
        return _CAPABILITY_PROFILE_VERSIONS[self.capability]

    @classmethod
    def from_job_system_recovery_info(
            cls, job_id: int,
            info: job_lib.JobSystemRecoveryInfo) -> 'RecoveryObservation':
        """Convert the typed API-v1 remote detail for one Serve task."""
        if not isinstance(info, job_lib.JobSystemRecoveryInfo):
            raise RecoveryStateError('Recovery detail has an invalid type.')
        if info.task_index != 0:
            raise RecoveryStateError(
                'Serve recovery detail must have task_index zero.')
        try:
            phase = RemoteRecoveryPhase(info.phase.value)
        except (AttributeError, ValueError) as e:
            raise RecoveryStateError(
                'Recovery detail has an invalid phase.') from e
        return cls(job_id=job_id,
                   capability=info.capability,
                   phase=phase,
                   original_attempt_id=info.original_attempt_id,
                   replacement_attempt_id=info.replacement_attempt_id,
                   node_boot_id=info.node_boot_id,
                   occurrence_count=info.occurrence_count,
                   armed_at=info.armed_at,
                   updated_at=info.updated_at,
                   event_id=info.event_id,
                   reason=info.reason,
                   occurred_at=info.occurred_at,
                   deadline_at=info.deadline_at)


@dataclasses.dataclass(frozen=True)
class ReplicaSystemRecovery:
    """Validated nested controller state persisted with one replica."""

    state: ControllerRecoveryState
    job_id: int
    capability: str
    original_attempt_id: str
    replacement_attempt_id: str | None
    node_boot_id: str
    remote_phase: RemoteRecoveryPhase
    occurrence_count: int
    armed_at: float
    event_id: str | None = None
    reason: str | None = None
    started_at: float | None = None
    deadline: float | None = None
    retry_submitted_adopted_at: float | None = None
    completed_at: float | None = None
    detection_deadline: float | None = None
    status_barrier_started_at: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, ControllerRecoveryState):
            raise RecoveryStateError(
                'Controller recovery state has an invalid type.')
        if not isinstance(self.remote_phase, RemoteRecoveryPhase):
            raise RecoveryStateError(
                'Remote recovery phase has an invalid type.')

        # Reuse the remote contract validator.  Controller timestamps replace
        # the remote updated timestamp only to validate shape, not authority.
        has_remote_event = (self.remote_phase != RemoteRecoveryPhase.ARMED and
                            self.occurrence_count > 0)
        RecoveryObservation(
            job_id=self.job_id,
            capability=self.capability,
            phase=self.remote_phase,
            original_attempt_id=self.original_attempt_id,
            replacement_attempt_id=self.replacement_attempt_id,
            node_boot_id=self.node_boot_id,
            occurrence_count=self.occurrence_count,
            armed_at=self.armed_at,
            updated_at=max(
                self.armed_at, self.completed_at or self.started_at or
                self.armed_at),
            event_id=self.event_id,
            reason=self.reason,
            occurred_at=self.started_at if has_remote_event else None,
            deadline_at=self.deadline if has_remote_event else None)
        _optional_timestamp(self.retry_submitted_adopted_at,
                            'retry_submitted_adopted_at')
        _optional_timestamp(self.completed_at, 'completed_at')
        _optional_timestamp(self.detection_deadline, 'detection_deadline')
        _optional_timestamp(self.status_barrier_started_at,
                            'status_barrier_started_at')

        if self.state == ControllerRecoveryState.ARMED:
            if self.remote_phase != RemoteRecoveryPhase.ARMED:
                raise RecoveryStateError('ARMED state has a non-ARMED phase.')
            if any(value is not None
                   for value in (self.started_at, self.deadline,
                                 self.retry_submitted_adopted_at,
                                 self.completed_at)):
                raise RecoveryStateError('ARMED state has active timestamps.')
        elif self.state == ControllerRecoveryState.RECOVERING:
            if self.remote_phase not in (RemoteRecoveryPhase.WAITING_CLEANUP,
                                         RemoteRecoveryPhase.WAITING_MEMORY,
                                         RemoteRecoveryPhase.RESUBMITTING):
                raise RecoveryStateError(
                    'RECOVERING state has an invalid remote phase.')
            if self.retry_submitted_adopted_at is not None:
                raise RecoveryStateError(
                    'RECOVERING state cannot have replacement adoption.')
            if self.completed_at is not None:
                raise RecoveryStateError(
                    'RECOVERING state cannot be completed.')
        elif self.state in (ControllerRecoveryState.RETRY_SUBMITTED,
                            ControllerRecoveryState.RECOVERED):
            if self.remote_phase != RemoteRecoveryPhase.RETRY_SUBMITTED:
                raise RecoveryStateError(
                    'Retry state has a non-RETRY_SUBMITTED phase.')
            if (self.replacement_attempt_id is None or
                    self.retry_submitted_adopted_at is None):
                raise RecoveryStateError(
                    'Retry state is missing replacement adoption.')
            if (self.state == ControllerRecoveryState.RETRY_SUBMITTED and
                    self.completed_at is not None):
                raise RecoveryStateError(
                    'RETRY_SUBMITTED state cannot be completed.')

        if self.state in (ControllerRecoveryState.RECOVERING,
                          ControllerRecoveryState.RETRY_SUBMITTED):
            if self.started_at is None or self.deadline is None:
                raise RecoveryStateError('Active state is missing its window.')
        if (self.state in (ControllerRecoveryState.RECOVERED,
                           ControllerRecoveryState.EXHAUSTED) and
                self.completed_at is None):
            raise RecoveryStateError('Terminal state lacks completed_at.')
        if (self.state in (ControllerRecoveryState.RECOVERED,
                           ControllerRecoveryState.EXHAUSTED) and
                self.remote_phase != RemoteRecoveryPhase.ARMED and
            (self.started_at is None or self.deadline is None)):
            raise RecoveryStateError(
                'Post-OOM terminal state must retain its fixed window.')
        if (self.state != ControllerRecoveryState.ARMED and
                self.detection_deadline is not None):
            raise RecoveryStateError(
                'Only ARMED state may have a detection deadline.')
        if (self.started_at is not None and self.deadline is not None and
                self.deadline < self.started_at):
            raise RecoveryStateError('Recovery deadline precedes its start.')
        if self.retry_submitted_adopted_at is not None:
            if self.remote_phase not in (RemoteRecoveryPhase.RETRY_SUBMITTED,
                                         RemoteRecoveryPhase.EXHAUSTED):
                raise RecoveryStateError(
                    'Replacement adoption has an invalid remote phase.')
            if (self.started_at is None or
                    self.retry_submitted_adopted_at < self.started_at):
                raise RecoveryStateError(
                    'Replacement adoption precedes recovery start.')
        if self.completed_at is not None:
            if (self.started_at is not None and
                    self.completed_at < self.started_at):
                raise RecoveryStateError(
                    'Recovery completion precedes recovery start.')
            if (self.retry_submitted_adopted_at is not None and
                    self.completed_at < self.retry_submitted_adopted_at):
                raise RecoveryStateError(
                    'Recovery completion precedes replacement adoption.')

    @property
    def profile_version(self) -> int:
        return _CAPABILITY_PROFILE_VERSIONS[self.capability]

    def to_dict(self) -> dict[str, Any]:
        payload = dataclasses.asdict(self)
        payload['state'] = self.state.value
        payload['remote_phase'] = self.remote_phase.value
        return payload

    @classmethod
    def from_dict(cls, payload: object) -> 'ReplicaSystemRecovery':
        if not isinstance(payload, dict):
            raise RecoveryStateError('system_recovery must be an object.')
        expected = {field.name for field in dataclasses.fields(cls)}
        if set(payload) != expected:
            raise RecoveryStateError('system_recovery has invalid fields.')
        try:
            return cls(
                **{
                    **payload,
                    'state': ControllerRecoveryState(payload['state']),
                    'remote_phase': RemoteRecoveryPhase(payload['remote_phase']
                                                       ),
                })
        except (KeyError, TypeError, ValueError) as e:
            raise RecoveryStateError(
                f'Invalid system_recovery state: {e}') from e


@dataclasses.dataclass(frozen=True)
class RecoveryReduction:
    """Pure reducer output; callers own persistence and side effects."""

    state: ReplicaSystemRecovery | None
    changed: bool
    force_off_route: bool
    clear_probe_failure_window: bool
    mark_ready: bool
    schedule_legacy_teardown: bool

    @property
    def teardown(self) -> bool:
        """Compatibility alias for the explicit legacy-teardown action."""
        return self.schedule_legacy_teardown


@dataclasses.dataclass(frozen=True)
class CandidateReadinessReduction:
    """Pure freshness reduction for an unresolved candidate."""

    disposition: SystemRecoveryDisposition
    candidate_ready_observed_at: float | None
    ordinary_release_not_before: float | None
    changed: bool
    force_off_route: bool
    record_application_readiness: bool
    mark_ready: bool
    schedule_legacy_teardown: bool


def _result(previous: ReplicaSystemRecovery | None,
            updated: ReplicaSystemRecovery | None,
            *,
            off_route: bool,
            clear_probe: bool = False,
            ready: bool = False,
            teardown: bool = False) -> RecoveryReduction:
    return RecoveryReduction(state=updated,
                             changed=updated != previous,
                             force_off_route=off_route,
                             clear_probe_failure_window=clear_probe,
                             mark_ready=ready,
                             schedule_legacy_teardown=teardown)


def _exhaust(
        current: ReplicaSystemRecovery,
        now: float,
        observation: RecoveryObservation | None = None
) -> ReplicaSystemRecovery:
    phase = current.remote_phase
    replacement_id = current.replacement_attempt_id
    occurrence_count = current.occurrence_count
    if observation is not None:
        phase = observation.phase
        replacement_id = observation.replacement_attempt_id or replacement_id
        occurrence_count = max(occurrence_count, observation.occurrence_count)
    timestamp = max(now, current.armed_at, current.started_at or 0,
                    current.retry_submitted_adopted_at or 0,
                    current.completed_at or 0)
    return dataclasses.replace(current,
                               state=ControllerRecoveryState.EXHAUSTED,
                               remote_phase=phase,
                               replacement_attempt_id=replacement_id,
                               occurrence_count=occurrence_count,
                               completed_at=timestamp,
                               detection_deadline=None)


def terminalize_for_teardown(current: ReplicaSystemRecovery | None, *,
                             now: float) -> ReplicaSystemRecovery | None:
    """Make an existing nested recovery state irreversibly terminal."""
    now = _finite_timestamp(now, 'now')
    if current is None or current.state == ControllerRecoveryState.EXHAUSTED:
        return current
    return _exhaust(current, now)


def _from_observation(observation: RecoveryObservation, now: float,
                      grace: float) -> ReplicaSystemRecovery:
    if observation.phase == RemoteRecoveryPhase.ARMED:
        state = ControllerRecoveryState.ARMED
        started_at = None
        deadline = None
        retry_adopted_at = None
        completed_at = None
    elif observation.phase == RemoteRecoveryPhase.RETRY_SUBMITTED:
        state = ControllerRecoveryState.RETRY_SUBMITTED
        started_at = now
        deadline = now + grace
        retry_adopted_at = now
        completed_at = None
    elif observation.phase == RemoteRecoveryPhase.EXHAUSTED:
        state = ControllerRecoveryState.EXHAUSTED
        started_at = now
        deadline = now + grace
        retry_adopted_at = None
        completed_at = now
    else:
        state = ControllerRecoveryState.RECOVERING
        started_at = now
        deadline = now + grace
        retry_adopted_at = None
        completed_at = None
    return ReplicaSystemRecovery(
        state=state,
        job_id=observation.job_id,
        capability=observation.capability,
        original_attempt_id=observation.original_attempt_id,
        replacement_attempt_id=observation.replacement_attempt_id,
        node_boot_id=observation.node_boot_id,
        remote_phase=observation.phase,
        occurrence_count=observation.occurrence_count,
        armed_at=observation.armed_at,
        event_id=observation.event_id,
        reason=observation.reason,
        started_at=started_at,
        deadline=deadline,
        retry_submitted_adopted_at=retry_adopted_at,
        completed_at=completed_at)


def _exhaust_observed(current: ReplicaSystemRecovery | None,
                      observation: RecoveryObservation, now: float,
                      grace: float) -> ReplicaSystemRecovery:
    if (current is None or (current.state == ControllerRecoveryState.ARMED and
                            observation.phase != RemoteRecoveryPhase.ARMED)):
        current = _from_observation(observation, now, grace)
    return _exhaust(current, now, observation)


def _identities_match(current: ReplicaSystemRecovery,
                      observation: RecoveryObservation) -> bool:
    strict_phase_regression = (_PHASE_ORDER[observation.phase]
                               < _PHASE_ORDER[current.remote_phase])
    if (current.job_id != observation.job_id or
            current.capability != observation.capability or
            current.original_attempt_id != observation.original_attempt_id or
            current.node_boot_id != observation.node_boot_id):
        return False
    if (current.event_id is not None and
            current.event_id != observation.event_id and
            not (strict_phase_regression and observation.event_id is None)):
        return False
    if current.replacement_attempt_id is not None:
        if (observation.replacement_attempt_id is None and
                strict_phase_regression):
            return True
        return (observation.replacement_attempt_id ==
                current.replacement_attempt_id)
    return True


def _advance(current: ReplicaSystemRecovery, observation: RecoveryObservation,
             now: float) -> ReplicaSystemRecovery:
    if observation.phase == RemoteRecoveryPhase.ARMED:
        return current
    if observation.phase == RemoteRecoveryPhase.EXHAUSTED:
        return _exhaust(current, now, observation)
    state = (ControllerRecoveryState.RETRY_SUBMITTED
             if observation.phase == RemoteRecoveryPhase.RETRY_SUBMITTED else
             ControllerRecoveryState.RECOVERING)
    retry_adopted_at = current.retry_submitted_adopted_at
    if (state == ControllerRecoveryState.RETRY_SUBMITTED and
            retry_adopted_at is None):
        retry_adopted_at = now
    if current.deadline is None:
        raise RecoveryStateError('Active recovery lacks a fixed deadline.')
    return dataclasses.replace(
        current,
        state=state,
        remote_phase=observation.phase,
        replacement_attempt_id=(observation.replacement_attempt_id or
                                current.replacement_attempt_id),
        occurrence_count=observation.occurrence_count,
        event_id=observation.event_id,
        reason=observation.reason,
        started_at=current.started_at or now,
        retry_submitted_adopted_at=retry_adopted_at,
        detection_deadline=None)


def reduce_remote_observation(current: ReplicaSystemRecovery | None,
                              observation: RecoveryObservation | None,
                              *,
                              now: float,
                              controller_grace_seconds: float,
                              job_terminal: bool = False,
                              teardown_intent: bool = False,
                              quarantined: bool = False) -> RecoveryReduction:
    """Reduce one exact-job status/detail observation without side effects."""
    now = _finite_timestamp(now, 'now')
    controller_grace_seconds = _positive_duration(controller_grace_seconds,
                                                  'controller_grace_seconds',
                                                  maximum=900)

    if quarantined:
        return _result(current, current, off_route=True, teardown=True)
    if (current is not None and
            current.state == ControllerRecoveryState.EXHAUSTED):
        return _result(current, current, off_route=True, teardown=True)
    if teardown_intent:
        exhausted: ReplicaSystemRecovery
        if current is None:
            if observation is None:
                return _result(None, None, off_route=True, teardown=True)
            exhausted = _exhaust_observed(None, observation, now,
                                          controller_grace_seconds)
        else:
            terminal = terminalize_for_teardown(current, now=now)
            assert terminal is not None
            exhausted = terminal
        return _result(current, exhausted, off_route=True, teardown=True)
    if (current is not None and
            current.state == ControllerRecoveryState.RECOVERED):
        if observation is None:
            if job_terminal:
                return _result(current,
                               _exhaust(current, now),
                               off_route=True,
                               teardown=True)
            return _result(current, current, off_route=False, ready=True)
        identities_match = _identities_match(current, observation)
        second_occurrence = (observation.occurrence_count
                             > current.occurrence_count)
        if (not identities_match or second_occurrence or
                observation.phase == RemoteRecoveryPhase.EXHAUSTED or
                job_terminal):
            exhausted = _exhaust(current, now,
                                 observation if identities_match else None)
            return _result(current, exhausted, off_route=True, teardown=True)
        return _result(current, current, off_route=False, ready=True)
    if (current is not None and current.deadline is not None and
            now >= current.deadline):
        return _result(current,
                       _exhaust(current, now),
                       off_route=True,
                       teardown=True)
    if observation is None:
        if current is not None and (job_terminal or current.state in
                                    (ControllerRecoveryState.RECOVERING,
                                     ControllerRecoveryState.RETRY_SUBMITTED)):
            return _result(current,
                           _exhaust(current, now),
                           off_route=True,
                           teardown=True)
        return _result(
            current,
            current,
            off_route=(current is not None and
                       current.state != ControllerRecoveryState.ARMED))

    if current is not None and not _identities_match(current, observation):
        return _result(current,
                       _exhaust(current, now),
                       off_route=True,
                       teardown=True)
    if job_terminal:
        exhausted = _exhaust_observed(current, observation, now,
                                      controller_grace_seconds)
        return _result(current, exhausted, off_route=True, teardown=True)
    if (current is not None and
            observation.occurrence_count < current.occurrence_count):
        return _result(current,
                       current,
                       off_route=(current.state
                                  != ControllerRecoveryState.ARMED))
    if observation.occurrence_count > 1:
        exhausted = _exhaust_observed(current, observation, now,
                                      controller_grace_seconds)
        return _result(current, exhausted, off_route=True, teardown=True)
    if (current is not None and _PHASE_ORDER[observation.phase]
            < _PHASE_ORDER[current.remote_phase]):
        return _result(current,
                       current,
                       off_route=(current.state
                                  != ControllerRecoveryState.ARMED))
    if (observation.phase != RemoteRecoveryPhase.RETRY_SUBMITTED and
            observation.deadline_at is not None and
            now >= observation.deadline_at):
        exhausted = _exhaust_observed(current, observation, now,
                                      controller_grace_seconds)
        return _result(current, exhausted, off_route=True, teardown=True)

    if current is None:
        updated = _from_observation(observation, now, controller_grace_seconds)
    elif (current.state == ControllerRecoveryState.ARMED and
          observation.phase != RemoteRecoveryPhase.ARMED):
        updated = _from_observation(observation, now, controller_grace_seconds)
    else:
        if (current.replacement_attempt_id is None and
                observation.replacement_attempt_id is not None and
                observation.phase not in (RemoteRecoveryPhase.RESUBMITTING,
                                          RemoteRecoveryPhase.RETRY_SUBMITTED,
                                          RemoteRecoveryPhase.EXHAUSTED)):
            return _result(current,
                           _exhaust(current, now, observation),
                           off_route=True,
                           teardown=True)
        updated = _advance(current, observation, now)

    if updated.state == ControllerRecoveryState.EXHAUSTED:
        return _result(current, updated, off_route=True, teardown=True)
    return _result(current,
                   updated,
                   off_route=(updated.state != ControllerRecoveryState.ARMED),
                   clear_probe=(updated.state
                                in (ControllerRecoveryState.RECOVERING,
                                    ControllerRecoveryState.RETRY_SUBMITTED)))


def reduce_probe_result(current: ReplicaSystemRecovery | None,
                        *,
                        succeeded: bool,
                        probe_started_at: float,
                        now: float,
                        was_ready: bool,
                        detection_window_seconds: float,
                        teardown_intent: bool = False,
                        quarantined: bool = False) -> RecoveryReduction:
    """Reduce one readiness result with the post-adoption freshness fence."""
    if type(succeeded) is not bool or type(was_ready) is not bool:
        raise RecoveryStateError('Probe flags must be booleans.')
    probe_started_at = _finite_timestamp(probe_started_at, 'probe_started_at')
    now = _finite_timestamp(now, 'now')
    detection_window_seconds = _positive_duration(detection_window_seconds,
                                                  'detection_window_seconds')
    if quarantined:
        return _result(current, current, off_route=True, teardown=True)
    if current is None:
        return _result(None,
                       None,
                       off_route=teardown_intent,
                       teardown=teardown_intent)
    if current.state == ControllerRecoveryState.EXHAUSTED:
        return _result(current, current, off_route=True, teardown=True)
    if teardown_intent:
        exhausted = terminalize_for_teardown(current, now=now)
        assert exhausted is not None
        return _result(current, exhausted, off_route=True, teardown=True)
    if current.state == ControllerRecoveryState.RECOVERED:
        return _result(current,
                       current,
                       off_route=not succeeded,
                       ready=succeeded)
    if current.state == ControllerRecoveryState.ARMED:
        deadline = current.detection_deadline
        if deadline is not None:
            if now >= deadline:
                return _result(current,
                               _exhaust(current, now),
                               off_route=True,
                               teardown=True)
            return _result(current, current, off_route=True)
        if succeeded:
            return _result(current, current, off_route=False, ready=True)
        if was_ready:
            deadline = now + detection_window_seconds
        updated = dataclasses.replace(current, detection_deadline=deadline)
        return _result(current, updated, off_route=True)
    if current.deadline is not None and now >= current.deadline:
        return _result(current,
                       _exhaust(current, now),
                       off_route=True,
                       teardown=True)
    if current.state == ControllerRecoveryState.RECOVERING:
        return _result(current, current, off_route=True)
    assert current.state == ControllerRecoveryState.RETRY_SUBMITTED
    assert current.retry_submitted_adopted_at is not None
    if succeeded and probe_started_at > current.retry_submitted_adopted_at:
        recovered = dataclasses.replace(current,
                                        state=ControllerRecoveryState.RECOVERED,
                                        completed_at=now,
                                        detection_deadline=None)
        return _result(current,
                       recovered,
                       off_route=False,
                       clear_probe=True,
                       ready=True)
    return _result(current, current, off_route=True)


def reduce_candidate_readiness(
    disposition: SystemRecoveryDisposition,
    candidate_ready_observed_at: float | None,
    ordinary_release_not_before: float | None,
    *,
    succeeded: bool,
    probe_started_at: float,
    now: float,
    monotonic_guard_satisfied: bool,
    exact_job_nonterminal: bool,
    exact_detail_absent: bool,
    teardown_intent: bool = False,
    quarantined: bool = False,
    guard_seconds: float = CANDIDATE_RELEASE_GUARD_SECONDS,
) -> CandidateReadinessReduction:
    """Reduce the bounded candidate-to-ordinary freshness protocol.

    ``monotonic_guard_satisfied`` is process-local evidence owned by the
    caller.  It is intentionally not reconstructed from wall time here.
    """
    if not isinstance(disposition, SystemRecoveryDisposition):
        raise RecoveryStateError('Invalid recovery disposition.')
    if any(
            type(value) is not bool
            for value in (succeeded, monotonic_guard_satisfied,
                          exact_job_nonterminal, exact_detail_absent,
                          teardown_intent, quarantined)):
        raise RecoveryStateError('Candidate reduction flags must be booleans.')
    probe_started_at = _finite_timestamp(probe_started_at, 'probe_started_at')
    now = _finite_timestamp(now, 'now')
    guard_seconds = _positive_duration(guard_seconds, 'guard_seconds')
    ready_at = _optional_timestamp(candidate_ready_observed_at,
                                   'candidate_ready_observed_at')
    release_at = _optional_timestamp(ordinary_release_not_before,
                                     'ordinary_release_not_before')
    if (ready_at is None) != (release_at is None):
        raise RecoveryStateError('Candidate freshness anchors are partial.')
    if ready_at is not None:
        assert release_at is not None
        if not math.isclose(
                release_at - ready_at, guard_seconds, rel_tol=0, abs_tol=1e-9):
            raise RecoveryStateError('Candidate freshness anchors disagree.')

    if quarantined or teardown_intent:
        return CandidateReadinessReduction(
            disposition=disposition,
            candidate_ready_observed_at=ready_at,
            ordinary_release_not_before=release_at,
            changed=False,
            force_off_route=True,
            record_application_readiness=False,
            mark_ready=False,
            schedule_legacy_teardown=True)
    if disposition != SystemRecoveryDisposition.CANDIDATE:
        return CandidateReadinessReduction(
            disposition=disposition,
            candidate_ready_observed_at=ready_at,
            ordinary_release_not_before=release_at,
            changed=False,
            force_off_route=(disposition == SystemRecoveryDisposition.CAPABLE),
            record_application_readiness=False,
            mark_ready=False,
            schedule_legacy_teardown=False)
    if not succeeded:
        return CandidateReadinessReduction(
            disposition=disposition,
            candidate_ready_observed_at=ready_at,
            ordinary_release_not_before=release_at,
            changed=False,
            force_off_route=True,
            record_application_readiness=False,
            mark_ready=False,
            schedule_legacy_teardown=False)
    if ready_at is None:
        return CandidateReadinessReduction(disposition=disposition,
                                           candidate_ready_observed_at=now,
                                           ordinary_release_not_before=now +
                                           guard_seconds,
                                           changed=True,
                                           force_off_route=True,
                                           record_application_readiness=True,
                                           mark_ready=False,
                                           schedule_legacy_teardown=False)

    assert release_at is not None
    fresh = (probe_started_at > release_at and now >= release_at and
             monotonic_guard_satisfied)
    releasable = (fresh and exact_job_nonterminal and exact_detail_absent)
    if releasable:
        return CandidateReadinessReduction(
            disposition=SystemRecoveryDisposition.ORDINARY,
            candidate_ready_observed_at=ready_at,
            ordinary_release_not_before=release_at,
            changed=True,
            force_off_route=False,
            record_application_readiness=False,
            mark_ready=True,
            schedule_legacy_teardown=False)
    return CandidateReadinessReduction(disposition=disposition,
                                       candidate_ready_observed_at=ready_at,
                                       ordinary_release_not_before=release_at,
                                       changed=False,
                                       force_off_route=True,
                                       record_application_readiness=False,
                                       mark_ready=False,
                                       schedule_legacy_teardown=False)
