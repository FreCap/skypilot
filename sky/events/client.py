"""Python SDK for the direct operational event API."""

from __future__ import annotations

import datetime
from typing import Any

from sky import exceptions
from sky.events import api_models
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import versions
from sky.usage import usage_lib
from sky.utils import annotations as annotations_lib
from sky.utils import context


def _value(value: Any) -> str:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            raise ValueError('Event timestamps must include a timezone.')
        return value.astimezone(datetime.timezone.utc).isoformat().replace(
            '+00:00', 'Z')
    if hasattr(value, 'value'):
        return str(value.value)
    return str(value)


def _append_many(params: list[tuple[str, str]], name: str,
                 values: list | tuple | None) -> None:
    if values is not None:
        params.extend((name, _value(value)) for value in values)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(
    server_constants.MIN_OPERATIONAL_EVENTS_API_VERSION)
def list_events(
    *,
    cluster: str | None = None,
    workspaces: list[str] | tuple[str, ...] | None = None,
    kinds: list[api_models.EventKind | str] |
    tuple[api_models.EventKind | str, ...] | None = None,
    outcomes: list[api_models.EventOutcome | str] |
    tuple[api_models.EventOutcome | str, ...] | None = None,
    actor_ids: list[str] | tuple[str, ...] | None = None,
    actor_types: list[api_models.EventActorType | str] |
    tuple[api_models.EventActorType | str, ...] | None = None,
    target_type: api_models.EventTargetType | str | None = None,
    target_id: str | None = None,
    target_name: str | None = None,
    request_id: str | None = None,
    since: datetime.datetime | str | None = None,
    until: datetime.datetime | str | None = None,
    direction: api_models.TraversalDirection |
    str = (api_models.TraversalDirection.OLDER),
    limit: int = 50,
    cursor: str | None = None,
) -> api_models.EventsPage:
    """Return one authorized operational event page."""
    if cluster is not None:
        if target_name is not None and target_name != cluster:
            raise ValueError('cluster and target_name must match.')
        if target_type is not None and _value(
                target_type) != api_models.EventTargetType.CLUSTER.value:
            raise ValueError('cluster requires target_type=cluster.')
        target_type = api_models.EventTargetType.CLUSTER
        target_name = cluster

    params: list[tuple[str, str]] = []
    _append_many(params, 'workspace', workspaces)
    _append_many(params, 'kind', kinds)
    _append_many(params, 'outcome', outcomes)
    _append_many(params, 'actor_id', actor_ids)
    _append_many(params, 'actor_type', actor_types)
    optional = {
        'target_type': target_type,
        'target_id': target_id,
        'target_name': target_name,
        'request_id': request_id,
        'since': since,
        'until': until,
        'direction': direction,
        'limit': limit,
        'cursor': cursor,
    }
    params.extend((name, _value(value))
                  for name, value in optional.items()
                  if value is not None)
    response = server_common.make_authenticated_request('GET',
                                                        '/events',
                                                        params=params)
    if response.status_code >= 400:
        try:
            detail = response.json().get('detail', {})
        except (AttributeError, ValueError):
            detail = {}
        code = detail.get('code') if isinstance(detail, dict) else None
        message = (detail.get('message') if isinstance(detail, dict) else None)
        if code == api_models.OPERATIONAL_EVENTS_UNAVAILABLE:
            raise exceptions.OperationalEventsUnavailableError(
                message or
                'Operational events require a PostgreSQL-backed API server.')
        if code == api_models.STALE_OPERATIONAL_EVENT_CURSOR:
            raise exceptions.StaleOperationalEventCursorError(
                message or 'The operational event cursor is stale.')
    server_common.handle_request_error(response)
    return api_models.EventsPage.model_validate(response.json())
