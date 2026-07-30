"""Closed server-owned models for operational event emission."""

from __future__ import annotations

from typing import Any

import pydantic

from sky import models as user_models
from sky.events import api_models
from sky.server.requests import request_names

EVENT_CONTEXT_VERSION = 1
MAX_EVENT_STRING_LENGTH = 256
MAX_EVENT_TARGETS = 16
UNKNOWN_ACTOR_TYPE = 'unknown'

_REQUEST_KIND = {
    request_names.RequestName.CLUSTER_LAUNCH:
        api_models.EventKind.CLUSTER_LAUNCH,
    request_names.RequestName.CLUSTER_START: api_models.EventKind.CLUSTER_START,
    request_names.RequestName.CLUSTER_STOP: api_models.EventKind.CLUSTER_STOP,
    request_names.RequestName.CLUSTER_DOWN: api_models.EventKind.CLUSTER_DOWN,
    request_names.RequestName.CLUSTER_AUTOSTOP:
        api_models.EventKind.CLUSTER_AUTOSTOP,
}
_PREFIXED_REQUEST_KIND = {
    f'sky.{request_name.value}': kind
    for request_name, kind in _REQUEST_KIND.items()
}

_ACTION_LABELS = {
    api_models.EventKind.CLUSTER_LAUNCH: 'launch',
    api_models.EventKind.CLUSTER_START: 'start',
    api_models.EventKind.CLUSTER_STOP: 'stop',
    api_models.EventKind.CLUSTER_DOWN: 'teardown',
    api_models.EventKind.CLUSTER_AUTOSTOP: 'autostop update',
}
_AMBIGUOUS_CAUSES = frozenset({
    api_models.EventCause.CONTROLLER_LEADERSHIP_LOST,
    api_models.EventCause.EXECUTION_LEASE_EXPIRED,
    api_models.EventCause.CONTROLLER_RESERVATION_CONFLICT,
})


class _ContextModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra='forbid',
                                       hide_input_in_errors=True)

    @pydantic.field_validator('*', mode='before')
    @classmethod
    def _bound_strings(cls, value: Any) -> Any:
        if isinstance(value, str) and len(value) > MAX_EVENT_STRING_LENGTH:
            raise ValueError('Operational event context value is too long.')
        return value


class EventTargetContext(_ContextModel):
    """Validated target snapshot persisted with an API request."""

    type: str = 'cluster'
    id: str | None = None
    name: str

    @pydantic.field_validator('type')
    @classmethod
    def _validate_type(cls, value: str) -> str:
        if value != 'cluster':
            raise ValueError('Unsupported operational event target type.')
        return value


class EventContext(_ContextModel):
    """Validated event context persisted before request execution."""

    version: int = EVENT_CONTEXT_VERSION
    kind: api_models.EventKind
    actor_name: str
    actor_type: str
    workspace: str | None = None
    targets: list[EventTargetContext]

    @pydantic.field_validator('version')
    @classmethod
    def _validate_version(cls, value: int) -> int:
        if value != EVENT_CONTEXT_VERSION:
            raise ValueError('Unsupported operational event context version.')
        return value

    @pydantic.field_validator('actor_type')
    @classmethod
    def _validate_actor_type(cls, value: str) -> str:
        allowed = {user_type.value for user_type in user_models.UserType}
        allowed.add(UNKNOWN_ACTOR_TYPE)
        if value not in allowed:
            raise ValueError('Unsupported operational event actor type.')
        return value

    @pydantic.field_validator('targets')
    @classmethod
    def _validate_targets(
            cls, value: list[EventTargetContext]) -> list[EventTargetContext]:
        if not value or len(value) > MAX_EVENT_TARGETS:
            raise ValueError('Operational events require 1-16 targets.')
        return value

    @property
    def complete(self) -> bool:
        return self.workspace is not None

    def with_workspace(self, workspace: str) -> EventContext:
        values = self.model_dump(mode='json')
        values['workspace'] = workspace
        return EventContext.model_validate(values)

    def with_primary_target_id(self, target_id: str | None) -> EventContext:
        if target_id is None or not self.targets:
            return self
        values = self.model_dump(mode='json')
        values['targets'][0]['id'] = target_id
        return EventContext.model_validate(values)

    def durable_dict(self) -> dict[str, Any]:
        return self.model_dump(mode='json')


def initial_context(
    request_name: request_names.RequestName,
    *,
    actor_name: str,
    actor_type: str | None,
    cluster_name: str | None,
) -> dict[str, Any] | None:
    """Build a server-owned event context for an opted-in request."""
    kind = _REQUEST_KIND.get(request_name)
    if kind is None or cluster_name is None:
        return None
    normalized_type = actor_type or UNKNOWN_ACTOR_TYPE
    context = EventContext(
        kind=kind,
        actor_name=actor_name,
        actor_type=normalized_type,
        targets=[EventTargetContext(name=cluster_name)],
    )
    return context.durable_dict()


def request_kind(request_name: str) -> api_models.EventKind | None:
    return _PREFIXED_REQUEST_KIND.get(request_name)


def outcome_for_status(status: str) -> api_models.EventOutcome:
    mapping = {
        'SUCCEEDED': api_models.EventOutcome.SUCCEEDED,
        'FAILED': api_models.EventOutcome.FAILED,
        'CANCELLED': api_models.EventOutcome.CANCELED,
    }
    try:
        return mapping[status]
    except KeyError:
        raise ValueError(
            f'Non-terminal operational event status: {status!r}') from None


def safe_message(kind: api_models.EventKind, outcome: api_models.EventOutcome,
                 cause: api_models.EventCause) -> str:
    """Render a fixed message without request or exception values."""
    action = _ACTION_LABELS[kind]
    if cause in _AMBIGUOUS_CAUSES:
        return (f'Cluster {action} was interrupted. The external outcome may '
                'be uncertain.')
    if outcome == api_models.EventOutcome.SUCCEEDED:
        return f'Cluster {action} succeeded.'
    if outcome == api_models.EventOutcome.FAILED:
        return f'Cluster {action} failed.'
    return f'Cluster {action} was canceled.'
