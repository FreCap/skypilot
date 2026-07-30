"""Direct REST API for actor-aware operational events."""

from __future__ import annotations

import datetime

import fastapi

from sky.events import api_models
from sky.server import common as server_common
from sky.server.events import cursors
from sky.server.events import models
from sky.server.events import store
from sky.users import permission
from sky.users import rbac
from sky.utils import common_utils
from sky.workspaces import core as workspaces_core

router = fastapi.APIRouter()
_MAX_FILTER_VALUES = 16


def _bounded_strings(values: list[str], name: str) -> tuple[str, ...]:
    if len(values) > _MAX_FILTER_VALUES:
        raise fastapi.HTTPException(
            status_code=422,
            detail=f'{name} accepts at most {_MAX_FILTER_VALUES} values.')
    for value in values:
        if not value or len(value) > models.MAX_EVENT_STRING_LENGTH:
            raise fastapi.HTTPException(
                status_code=422,
                detail=(f'{name} values must contain 1-'
                        f'{models.MAX_EVENT_STRING_LENGTH} characters.'))
    return tuple(sorted(set(values)))


def _bounded_enums(values: list, name: str) -> tuple:
    if len(values) > _MAX_FILTER_VALUES:
        raise fastapi.HTTPException(
            status_code=422,
            detail=f'{name} accepts at most {_MAX_FILTER_VALUES} values.')
    return tuple(sorted(set(values), key=lambda value: value.value))


def _bounded_optional(value: str | None, name: str) -> str | None:
    if value is not None and (not value or
                              len(value) > models.MAX_EVENT_STRING_LENGTH):
        raise fastapi.HTTPException(
            status_code=422,
            detail=(f'{name} must contain 1-'
                    f'{models.MAX_EVENT_STRING_LENGTH} characters.'))
    return value


def _aware(value: datetime.datetime | None,
           name: str) -> datetime.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise fastapi.HTTPException(
            status_code=422, detail=f'{name} must include an RFC3339 timezone.')
    return value.astimezone(datetime.timezone.utc)


def _authorization_scope(
    request: fastapi.Request,
    requested_workspaces: tuple[str, ...],
) -> store.AuthorizationScope:
    # This direct sync handler bypasses the executor's per-request config
    # reload, so refresh workspace and Casbin state before deriving SQL scope.
    server_common.refresh_workspace_state_for_sync_handler()
    auth_user = request.state.auth_user
    if auth_user is None:
        effective = requested_workspaces or None
        return store.AuthorizationScope(
            principal_id=common_utils.get_user_hash(),
            is_admin=True,
            effective_workspaces=effective,
        )

    roles = permission.permission_service.get_user_roles(auth_user.id)
    is_admin = rbac.RoleName.ADMIN.value in roles
    if is_admin:
        effective = requested_workspaces or None
    else:
        accessible = (workspaces_core.get_accessible_workspace_names_for_user(
            auth_user.id, roles=roles))
        if requested_workspaces:
            accessible.intersection_update(requested_workspaces)
        effective = tuple(sorted(accessible))
    return store.AuthorizationScope(principal_id=auth_user.id,
                                    is_admin=is_admin,
                                    effective_workspaces=effective)


@router.get('', response_model=api_models.EventsPage)
def list_events(
    request: fastapi.Request,
    workspace: list[str] = fastapi.Query(default=[]),
    kind: list[api_models.EventKind] = fastapi.Query(default=[]),
    outcome: list[api_models.EventOutcome] = fastapi.Query(default=[]),
    actor_id: list[str] = fastapi.Query(default=[]),
    actor_type: list[api_models.EventActorType] = fastapi.Query(default=[]),
    target_type: api_models.EventTargetType | None = None,
    target_id: str | None = None,
    target_name: str | None = None,
    request_id: str | None = None,
    since: datetime.datetime | None = None,
    until: datetime.datetime | None = None,
    direction: api_models.TraversalDirection = (
        api_models.TraversalDirection.OLDER),
    limit: int = fastapi.Query(default=50, ge=1, le=100),
    cursor: str | None = None,
) -> api_models.EventsPage:
    """List authorized operational events with signed keyset pagination."""
    normalized_workspaces = _bounded_strings(workspace, 'workspace')
    normalized_target_id = _bounded_optional(target_id, 'target_id')
    normalized_target_name = _bounded_optional(target_name, 'target_name')
    normalized_request_id = _bounded_optional(request_id, 'request_id')
    if ((normalized_target_id is not None or normalized_target_name is not None)
            and target_type is None):
        raise fastapi.HTTPException(
            status_code=422,
            detail='target_id and target_name require target_type.')
    normalized_since = _aware(since, 'since')
    normalized_until = _aware(until, 'until')
    if (normalized_since is not None and normalized_until is not None and
            normalized_since > normalized_until):
        raise fastapi.HTTPException(
            status_code=422, detail='since must not be later than until.')
    query = store.EventQuery(
        workspaces=normalized_workspaces,
        kinds=_bounded_enums(kind, 'kind'),
        outcomes=_bounded_enums(outcome, 'outcome'),
        actor_ids=_bounded_strings(actor_id, 'actor_id'),
        actor_types=_bounded_enums(actor_type, 'actor_type'),
        target_type=target_type,
        target_id=normalized_target_id,
        target_name=normalized_target_name,
        request_id=normalized_request_id,
        since=normalized_since,
        until=normalized_until,
        direction=direction,
        limit=limit,
        cursor=cursor,
    )
    scope = _authorization_scope(request, normalized_workspaces)
    try:
        return store.list_events(query, scope)
    except store.OperationalEventsUnavailableError as e:
        raise fastapi.HTTPException(
            status_code=503,
            detail={
                'code': api_models.OPERATIONAL_EVENTS_UNAVAILABLE,
                'message': str(e),
            }) from e
    except cursors.StaleCursorError as e:
        raise fastapi.HTTPException(
            status_code=409,
            detail={
                'code': api_models.STALE_OPERATIONAL_EVENT_CURSOR,
                'message': str(e),
            }) from e
