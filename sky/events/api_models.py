"""Credential-free API models for operational events."""

from __future__ import annotations

import enum

import pydantic

OPERATIONAL_EVENTS_UNAVAILABLE = 'OPERATIONAL_EVENTS_UNAVAILABLE'
STALE_OPERATIONAL_EVENT_CURSOR = 'STALE_OPERATIONAL_EVENT_CURSOR'


class _ApiModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra='forbid',
                                       hide_input_in_errors=True)


class EventKind(str, enum.Enum):
    CLUSTER_LAUNCH = 'cluster.launch'
    CLUSTER_START = 'cluster.start'
    CLUSTER_STOP = 'cluster.stop'
    CLUSTER_DOWN = 'cluster.down'
    CLUSTER_AUTOSTOP = 'cluster.autostop'


class EventPhase(str, enum.Enum):
    TERMINAL = 'terminal'


class EventOutcome(str, enum.Enum):
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    CANCELED = 'canceled'


class EventCause(str, enum.Enum):
    """Closed reason codes for terminal event outcomes."""

    HANDLER_SUCCEEDED = 'handler_succeeded'
    HANDLER_FAILED = 'handler_failed'
    DISPATCHER_SUBMIT_FAILED = 'dispatcher_submit_failed'
    EXPLICIT_CANCEL = 'explicit_cancel'
    COROUTINE_DISCONNECTED = 'coroutine_disconnected'
    GRACEFUL_SHUTDOWN_RETRY = 'graceful_shutdown_retry'
    COMPATIBILITY_RESTART = 'compatibility_restart'
    CONTROLLER_LEADERSHIP_LOST = 'controller_leadership_lost'
    EXECUTION_LEASE_EXPIRED = 'execution_lease_expired'
    PRECONDITION_FAILED = 'precondition_failed'
    CONTROLLER_RESERVATION_CONFLICT = 'controller_reservation_conflict'


class TraversalDirection(str, enum.Enum):
    OLDER = 'older'
    NEWER = 'newer'


class EventActorType(str, enum.Enum):
    SYSTEM = 'system'
    BASIC = 'basic'
    SERVICE_ACCOUNT = 'sa'
    SSO = 'sso'
    LEGACY = 'legacy'
    UNKNOWN = 'unknown'


class EventTargetType(str, enum.Enum):
    CLUSTER = 'cluster'


class EventActor(_ApiModel):
    id: str
    name: str
    type: EventActorType


class EventTarget(_ApiModel):
    type: EventTargetType
    id: str | None = None
    name: str


class OperationalEvent(_ApiModel):
    """One immutable actor-aware operational event."""

    id: str
    occurred_at: str
    kind: EventKind
    phase: EventPhase
    outcome: EventOutcome
    cause: EventCause
    message: str
    workspace: str
    actor: EventActor
    request_id: str
    execution_generation: int
    targets: list[EventTarget]


class EventsPage(_ApiModel):
    items: list[OperationalEvent]
    next_cursor: str | None = None
    poll_cursor: str
    has_more: bool
