"""Typed values for owner-fenced replica observation persistence.

This module deliberately owns no I/O.  It defines the exact row identity that
an observer reduced and the bounded result returned by the PostgreSQL commit
boundary.  The optional recovery subdocument remains restricted to legacy
non-pool services; generic health observations apply to every Serve mode.
"""

from __future__ import annotations

import copy
import dataclasses
import math
from typing import Any, TYPE_CHECKING
import uuid

from sky.serve import system_recovery_state
from sky.utils import common_utils

if TYPE_CHECKING:
    from sky.serve.replica_info import ReplicaInfo
else:
    ReplicaInfo = Any

# One observation transaction takes a service/lifecycle lock plus row locks.
# Keep its worst-case lock set and bind cardinality independent of fleet size;
# the manager windows larger waves through this exact boundary.
REPLICA_OBSERVATION_COMMIT_MAX_ROWS = 256


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f'{name} must be a positive integer.')
    return value


def _nonnegative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer.')
    return value


def _canonical_record_id(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError('replica_record_id must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(
            'replica_record_id must be a canonical UUID string.') from error
    if str(parsed) != value:
        raise ValueError('replica_record_id must be a canonical UUID string.')
    return value


def _exact_bool(value: object, name: str) -> bool:
    if type(value) is not bool:  # pylint: disable=unidiomatic-typecheck
        raise ValueError(f'{name} must be a boolean.')
    return value


def _optional_nonnegative_timestamp(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f'{name} must be a nonnegative finite timestamp.')
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f'{name} must be a nonnegative finite timestamp.')
    return normalized


def _optional_first_ready_time(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            'first_ready_time must be -1 or a nonnegative finite timestamp.')
    normalized = float(value)
    if (not math.isfinite(normalized) or
        (normalized != -1.0 and normalized < 0)):
        raise ValueError(
            'first_ready_time must be -1 or a nonnegative finite timestamp.')
    return normalized


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaObserverOwnerFence:
    """Policy-independent identity for one controller observation writer."""

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    controller_pid: int | None
    controller_ip: str | None
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int

    def __post_init__(self) -> None:
        if not isinstance(self.service_name, str) or not self.service_name:
            raise ValueError('service_name must be a nonempty string.')
        if not isinstance(self.service_hash, str) or not self.service_hash:
            raise ValueError('service_hash must be a nonempty string.')
        _positive_int(self.service_lifecycle_epoch, 'service_lifecycle_epoch')
        if (self.controller_pid is not None and
            (isinstance(self.controller_pid, bool) or
             not isinstance(self.controller_pid, int) or
             self.controller_pid < 1)):
            raise ValueError(
                'controller_pid must be a positive integer or None.')
        if (self.controller_ip is not None and
            (not isinstance(self.controller_ip, str) or
             not self.controller_ip)):
            raise ValueError('controller_ip must be a nonempty string or None.')
        if not isinstance(self.controller_incarnation, uuid.UUID):
            raise ValueError('controller_incarnation must be a UUID.')
        _positive_int(self.controller_owner_epoch, 'controller_owner_epoch')

    @property
    def controller_owner(self) -> tuple[int | None, str | None]:
        return self.controller_pid, self.controller_ip


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaSystemRecoveryPatch:
    """Immutable copy of the complete mutable recovery subdocument."""

    system_recovery_launch_intent: (
        system_recovery_state.SystemRecoveryLaunchIntent | None)
    system_recovery_disposition: (
        system_recovery_state.SystemRecoveryDisposition)
    launch_request_id: str | None
    service_job_id: int | None
    candidate_ready_observed_at: float | None
    ordinary_release_not_before: float | None
    system_recovery_revision: int
    system_recovery: system_recovery_state.ReplicaSystemRecovery | None
    system_recovery_quarantine: (system_recovery_state.SystemRecoveryQuarantine
                                 | None)

    @classmethod
    def from_replica_info(cls, info: ReplicaInfo) -> ReplicaSystemRecoveryPatch:
        try:
            values = {
                field.name: copy.deepcopy(getattr(info, field.name))
                for field in dataclasses.fields(cls)
            }
        except AttributeError as error:
            raise ValueError(
                'desired_info has an incomplete recovery subdocument.'
            ) from error
        return cls(**values)

    def apply_to(self, info: ReplicaInfo) -> ReplicaInfo:
        """Return a detached row copy carrying exactly this subdocument."""
        desired = copy.deepcopy(info)
        for field in dataclasses.fields(self):
            setattr(desired, field.name, getattr(self, field.name))
        return desired


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaObservationPatch:
    """Immutable fields exclusively owned by health/recovery reducers.

    This intentionally excludes launch progress and logical-retirement fields.
    A probe observation must not overwrite either owner merely because it was
    reduced in the same transaction as the recovery subdocument.
    """

    user_app_failed: bool
    service_ready_now: bool
    first_ready_time: float | None
    sky_down_status: common_utils.ProcessStatus | None
    is_scale_down: bool
    preempted: bool
    purged: bool
    drain_cap_seconds: int | None
    drain_started_at: float | None
    wait_for_idle_before_termination: bool
    first_not_ready_time: float | None
    first_consecutive_failure_time: float | None

    def __post_init__(self) -> None:
        for name in ('user_app_failed', 'service_ready_now', 'is_scale_down',
                     'preempted', 'purged', 'wait_for_idle_before_termination'):
            _exact_bool(getattr(self, name), name)
        if (self.sky_down_status is not None and not isinstance(
                self.sky_down_status, common_utils.ProcessStatus)):
            raise ValueError('sky_down_status must be a ProcessStatus or None.')
        if (self.drain_cap_seconds is not None and
            (type(self.drain_cap_seconds) is not int or  # pylint: disable=unidiomatic-typecheck
             self.drain_cap_seconds < 0)):
            raise ValueError(
                'drain_cap_seconds must be a nonnegative integer or None.')
        object.__setattr__(self, 'first_ready_time',
                           _optional_first_ready_time(self.first_ready_time))
        for name in ('drain_started_at', 'first_not_ready_time',
                     'first_consecutive_failure_time'):
            object.__setattr__(
                self, name,
                _optional_nonnegative_timestamp(getattr(self, name), name))

    @classmethod
    def from_replica_info(cls, info: ReplicaInfo) -> ReplicaObservationPatch:
        try:
            status = info.status_property
            return cls(
                user_app_failed=status.user_app_failed,
                service_ready_now=status.service_ready_now,
                first_ready_time=status.first_ready_time,
                sky_down_status=status.sky_down_status,
                is_scale_down=status.is_scale_down,
                preempted=status.preempted,
                purged=status.purged,
                drain_cap_seconds=status.drain_cap_seconds,
                drain_started_at=status.drain_started_at,
                wait_for_idle_before_termination=(
                    status.wait_for_idle_before_termination),
                first_not_ready_time=info.first_not_ready_time,
                first_consecutive_failure_time=(
                    info.first_consecutive_failure_time),
            )
        except AttributeError as error:
            raise ValueError(
                'desired_info has an incomplete probe-owned state.') from error

    def apply_to(self, info: ReplicaInfo) -> ReplicaInfo:
        """Return a detached row copy carrying only probe-owned state."""
        desired = copy.deepcopy(info)
        status = desired.status_property
        status.user_app_failed = self.user_app_failed
        status.service_ready_now = self.service_ready_now
        status.first_ready_time = self.first_ready_time
        status.sky_down_status = self.sky_down_status
        status.is_scale_down = self.is_scale_down
        status.preempted = self.preempted
        status.purged = self.purged
        status.drain_cap_seconds = self.drain_cap_seconds
        status.drain_started_at = self.drain_started_at
        status.wait_for_idle_before_termination = (
            self.wait_for_idle_before_termination)
        desired.first_not_ready_time = self.first_not_ready_time
        desired.first_consecutive_failure_time = (
            self.first_consecutive_failure_time)
        return desired


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaObservationWrite:
    """One pure reduction against an exact previously observed replica row."""

    replica_id: int
    replica_record_id: str
    service_version: int
    expected_revision: int
    desired_info: dataclasses.InitVar[ReplicaInfo]
    expected_observation_state: ReplicaObservationPatch | None = None
    desired_observation_state: ReplicaObservationPatch | None = None
    desired_recovery: ReplicaSystemRecoveryPatch = dataclasses.field(init=False)

    def __post_init__(self, desired_info: ReplicaInfo) -> None:
        _positive_int(self.replica_id, 'replica_id')
        _canonical_record_id(self.replica_record_id)
        _positive_int(self.service_version, 'service_version')
        _nonnegative_int(self.expected_revision, 'expected_revision')
        if getattr(desired_info, 'replica_id', None) != self.replica_id:
            raise ValueError('desired_info must match replica_id.')
        if (getattr(desired_info, 'replica_record_id', None)
                != self.replica_record_id):
            raise ValueError('desired_info must match replica_record_id.')
        if getattr(desired_info, 'version', None) != self.service_version:
            raise ValueError('desired_info must match service_version.')
        if (getattr(desired_info, 'system_recovery_revision', None)
                != self.expected_revision):
            raise ValueError('desired_info must carry expected_revision.')
        if ((self.expected_observation_state is None)
                != (self.desired_observation_state is None)):
            raise ValueError(
                'expected_observation_state and desired_observation_state '
                'must be provided together.')
        if (self.expected_observation_state is not None and
            (not isinstance(self.expected_observation_state,
                            ReplicaObservationPatch) or
             not isinstance(self.desired_observation_state,
                            ReplicaObservationPatch))):
            raise ValueError('Observation states must be '
                             'ReplicaObservationPatch values.')
        object.__setattr__(
            self, 'desired_recovery',
            ReplicaSystemRecoveryPatch.from_replica_info(desired_info))


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaObservationBatchResult:
    """Outcome of one batch commit, ordered by replica ID within each field."""

    updated_infos: tuple[ReplicaInfo, ...]
    unchanged_infos: tuple[ReplicaInfo, ...]
    stale_replica_ids: tuple[int, ...]
