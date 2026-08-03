"""Closed row enums for the revision-001 physical-capacity foundation.

This module intentionally contains no persistence models or production
projection payloads.  Those contracts require a separate C2 design review.
"""

import enum


class ProjectionSourceKind(str, enum.Enum):
    SERVE_SERVICE = 'serve_service'
    SERVE_POOL = 'serve_pool'
    MANAGED_JOB_TASK = 'managed_job_task'


class ProjectionScanState(str, enum.Enum):
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'


class OwnerKind(str, enum.Enum):
    SERVICE = 'service'
    POOL = 'pool'
    MANAGED_JOB_TASK = 'managed_job_task'


class WriterFenceKind(str, enum.Enum):
    SERVE_LIFECYCLE = 'serve_lifecycle'
    CONTROLLER_GENERATION = 'controller_generation'
    LEGACY = 'legacy'


class ProjectionConfidence(str, enum.Enum):
    EXACT = 'exact'
    LEGACY = 'legacy'
    UNKNOWN = 'unknown'


class GroupLifecycleState(str, enum.Enum):
    ACTIVE = 'active'
    RETIRING = 'retiring'
    RETIRED = 'retired'


class AllocationSourceKind(str, enum.Enum):
    SERVE_REPLICA = 'serve_replica'
    POOL_WORKER = 'pool_worker'
    MANAGED_JOB_CLUSTER = 'managed_job_cluster'


class AllocationIdentityConfidence(str, enum.Enum):
    EXACT = 'exact'
    LEGACY = 'legacy'
    UNKNOWN = 'unknown'


class AllocationProjectionState(str, enum.Enum):
    CURRENT = 'current'
    SOURCE_MISSING = 'source_missing'
    QUARANTINED = 'quarantined'


class AllocationObservedState(str, enum.Enum):
    UNKNOWN = 'unknown'
    PROVISIONING = 'provisioning'
    UP = 'up'
    STOPPED = 'stopped'
    ABSENT = 'absent'
    FAILED = 'failed'
    PARTIAL = 'partial'


class ObservationCertainty(str, enum.Enum):
    LEGACY = 'legacy'
    REGISTRY = 'registry'
    PROVIDER = 'provider'


class DesiredState(str, enum.Enum):
    PRESENT = 'present'
    STOPPED = 'stopped'
    ABSENT = 'absent'


class ReleaseGate(str, enum.Enum):
    BLOCKED = 'blocked'
    OPEN = 'open'


class DesireReasonCode(str, enum.Enum):
    PROJECTION = 'projection'
    CARRY_FORWARD = 'carry_forward'
    SCALE_UP = 'scale_up'
    REPLACEMENT = 'replacement'
    RECOVERY = 'recovery'
    SCALE_DOWN = 'scale_down'
    TEARDOWN = 'teardown'


class ActorType(str, enum.Enum):
    SYSTEM = 'system'
    BASIC = 'basic'
    SERVICE_ACCOUNT = 'sa'
    SSO = 'sso'
    LEGACY = 'legacy'
    UNKNOWN = 'unknown'
