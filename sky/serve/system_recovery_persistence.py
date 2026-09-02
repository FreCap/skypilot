"""Typed values for owner-fenced system-recovery persistence.

This module deliberately owns no I/O.  It defines the exact row identity that
an observer reduced and the bounded result returned by the PostgreSQL commit
boundary.
"""

from __future__ import annotations

import copy
import dataclasses
from typing import Any, TYPE_CHECKING
import uuid

from sky.serve import system_recovery_state

if TYPE_CHECKING:
    from sky.serve.replica_info import ReplicaInfo
else:
    ReplicaInfo = Any


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
class ReplicaSystemRecoveryWrite:
    """One pure reduction against an exact previously observed replica row."""

    replica_id: int
    replica_record_id: str
    service_version: int
    expected_revision: int
    desired_info: dataclasses.InitVar[ReplicaInfo]
    desired_recovery: ReplicaSystemRecoveryPatch = dataclasses.field(init=False)

    def __post_init__(self, desired_info: ReplicaInfo) -> None:
        _positive_int(self.replica_id, 'replica_id')
        _canonical_record_id(self.replica_record_id)
        _positive_int(self.service_version, 'service_version')
        _nonnegative_int(self.expected_revision, 'expected_revision')
        if getattr(desired_info, 'replica_id', None) != self.replica_id:
            raise ValueError('desired_info must match replica_id.')
        if (getattr(desired_info, 'replica_record_id', None) !=
                self.replica_record_id):
            raise ValueError('desired_info must match replica_record_id.')
        if getattr(desired_info, 'version', None) != self.service_version:
            raise ValueError('desired_info must match service_version.')
        if (getattr(desired_info, 'system_recovery_revision', None) !=
                self.expected_revision):
            raise ValueError('desired_info must carry expected_revision.')
        object.__setattr__(
            self, 'desired_recovery',
            ReplicaSystemRecoveryPatch.from_replica_info(desired_info))


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaSystemRecoveryBatchResult:
    """Outcome of one batch commit, ordered by replica ID within each field."""

    updated_infos: tuple[ReplicaInfo, ...]
    unchanged_infos: tuple[ReplicaInfo, ...]
    stale_replica_ids: tuple[int, ...]
