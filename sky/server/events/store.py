"""Authorized PostgreSQL reads for operational events."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import os
from typing import Any
import uuid

import sqlalchemy

from sky import sky_logging
from sky import skypilot_config
from sky.events import api_models
from sky.server.events import cursors
from sky.server.events import schema
from sky.server.requests import postgres as request_postgres

DEFAULT_OPERATIONAL_EVENT_RETENTION_HOURS = 720.0
_RETENTION_INTERVAL_SECONDS = 3600
_RETENTION_BATCH_SIZE = 1000

logger = sky_logging.init_logger(__name__)


class OperationalEventsUnavailableError(RuntimeError):
    """The deployment does not have the PostgreSQL event plane."""


@dataclasses.dataclass(frozen=True)
class EventQuery:
    """Normalized, bounded event query filters."""

    workspaces: tuple[str, ...] = ()
    kinds: tuple[api_models.EventKind, ...] = ()
    outcomes: tuple[api_models.EventOutcome, ...] = ()
    actor_ids: tuple[str, ...] = ()
    actor_types: tuple[api_models.EventActorType, ...] = ()
    target_type: api_models.EventTargetType | None = None
    target_id: str | None = None
    target_name: str | None = None
    request_id: str | None = None
    since: datetime.datetime | None = None
    until: datetime.datetime | None = None
    direction: api_models.TraversalDirection = (
        api_models.TraversalDirection.OLDER)
    limit: int = 50
    cursor: str | None = None

    def cursor_filters(self) -> dict[str, Any]:
        """Return stable JSON values included in the cursor signature."""

        def timestamp(value: datetime.datetime | None) -> str | None:
            if value is None:
                return None
            return value.astimezone(datetime.timezone.utc).isoformat().replace(
                '+00:00', 'Z')

        return {
            'workspaces': list(self.workspaces),
            'kinds': [kind.value for kind in self.kinds],
            'outcomes': [outcome.value for outcome in self.outcomes],
            'actor_ids': list(self.actor_ids),
            'actor_types': [
                actor_type.value for actor_type in self.actor_types
            ],
            'target_type': (
                self.target_type.value if self.target_type is not None else None
            ),
            'target_id': self.target_id,
            'target_name': self.target_name,
            'request_id': self.request_id,
            'since': timestamp(self.since),
            'until': timestamp(self.until),
        }


@dataclasses.dataclass(frozen=True)
class AuthorizationScope:
    """Authorization state applied inside the SQL query."""

    principal_id: str
    is_admin: bool
    # None means trusted/admin access to all workspaces. An empty tuple means
    # no visible workspaces and compiles to SQL false.
    effective_workspaces: tuple[str, ...] | None

    def cursor_workspaces(self) -> tuple[str, ...]:
        return ('*',) if self.effective_workspaces is None else (
            self.effective_workspaces)


def is_available() -> bool:
    """Whether this process is configured for the PostgreSQL event plane."""
    return (os.environ.get(request_postgres.REQUEST_BACKEND_ENV_VAR) ==
            request_postgres.POSTGRES_REQUEST_BACKEND)


def _authority_state(
        connection: sqlalchemy.engine.Connection) -> tuple[bytes, int]:
    value = connection.execute(
        sqlalchemy.select(schema.REQUEST_STORE_METADATA.c.value).where(
            schema.REQUEST_STORE_METADATA.c.key ==
            schema.CURSOR_AUTHORITY_METADATA_KEY)).scalar_one_or_none()
    if not isinstance(value, dict) or not isinstance(value.get('authority_id'),
                                                     str):
        raise OperationalEventsUnavailableError(
            'Operational event cursor authority is unavailable.')
    event_sequence = value.get('event_sequence')
    if (isinstance(event_sequence, bool) or
            not isinstance(event_sequence, int) or event_sequence < 0):
        raise OperationalEventsUnavailableError(
            'Operational event sequence state is unavailable.')
    return cursors.derive_key(value['authority_id']), event_sequence


def _format_timestamp(value: datetime.datetime) -> str:
    return value.astimezone(datetime.timezone.utc).isoformat().replace(
        '+00:00', 'Z')


def _event_from_row(
    row: sqlalchemy.engine.RowMapping,
    targets: list[api_models.EventTarget],
) -> api_models.OperationalEvent:
    return api_models.OperationalEvent(
        id=str(row['event_id']),
        occurred_at=_format_timestamp(row['occurred_at']),
        kind=api_models.EventKind(row['kind']),
        phase=api_models.EventPhase(row['phase']),
        outcome=api_models.EventOutcome(row['outcome']),
        cause=api_models.EventCause(row['cause']),
        message=row['message'],
        workspace=row['workspace'],
        actor=api_models.EventActor(id=row['actor_id'],
                                    name=row['actor_name'],
                                    type=row['actor_type']),
        request_id=row['source_request_id'],
        execution_generation=row['source_execution_generation'],
        targets=targets,
    )


def list_events(query: EventQuery,
                scope: AuthorizationScope) -> api_models.EventsPage:
    """Return one authorized keyset-paginated event page."""
    if not is_available():
        raise OperationalEventsUnavailableError(
            'Operational events require a PostgreSQL-backed API server.')
    engine = request_postgres.initialize_and_get_db()
    events = schema.RESOURCE_EVENTS
    targets = schema.RESOURCE_EVENT_TARGETS
    with engine.connect() as connection:
        authority_key, committed_high_watermark = _authority_state(connection)
        bindings = cursors.CursorBindings(
            principal_id=scope.principal_id,
            is_admin=scope.is_admin,
            workspaces=scope.cursor_workspaces(),
            filters=query.cursor_filters(),
        )
        cursor_state = None
        if query.cursor is not None:
            cursor_state = cursors.verify(query.cursor, authority_key, bindings,
                                          query.direction)
            if cursor_state.high_watermark > committed_high_watermark:
                raise cursors.StaleCursorError(
                    'Invalid operational event cursor.')
        if (cursor_state is not None and
            (query.direction == api_models.TraversalDirection.OLDER or
             cursor_state.position < cursor_state.high_watermark)):
            page_high_watermark = cursor_state.high_watermark
        else:
            page_high_watermark = committed_high_watermark

        conditions: list[sqlalchemy.ColumnElement[bool]] = [
            events.c.event_sequence <= page_high_watermark
        ]
        if scope.effective_workspaces is not None:
            if scope.effective_workspaces:
                conditions.append(
                    events.c.workspace.in_(scope.effective_workspaces))
            else:
                conditions.append(sqlalchemy.false())
        if query.kinds:
            conditions.append(
                events.c.kind.in_([kind.value for kind in query.kinds]))
        if query.outcomes:
            conditions.append(
                events.c.outcome.in_(
                    [outcome.value for outcome in query.outcomes]))
        if query.actor_ids:
            conditions.append(events.c.actor_id.in_(query.actor_ids))
        if query.actor_types:
            conditions.append(
                events.c.actor_type.in_(
                    [actor_type.value for actor_type in query.actor_types]))
        if query.request_id is not None:
            conditions.append(events.c.source_request_id == query.request_id)
        if query.since is not None:
            conditions.append(events.c.occurred_at >= query.since)
        if query.until is not None:
            conditions.append(events.c.occurred_at <= query.until)

        if query.target_type is not None:
            target_conditions = [
                targets.c.event_id == events.c.event_id,
                targets.c.target_type == query.target_type.value,
            ]
            if query.target_id is not None:
                target_conditions.append(targets.c.target_id == query.target_id)
            if query.target_name is not None:
                target_conditions.append(
                    targets.c.target_name == query.target_name)
            conditions.append(
                sqlalchemy.exists(
                    sqlalchemy.select(
                        sqlalchemy.literal(1)).select_from(targets).where(
                            *target_conditions)))

        if cursor_state is not None:
            if query.direction == api_models.TraversalDirection.OLDER:
                conditions.append(
                    events.c.event_sequence < cursor_state.position)
            else:
                conditions.append(
                    events.c.event_sequence > cursor_state.position)

        if query.direction == api_models.TraversalDirection.OLDER:
            ordering = (events.c.event_sequence.desc(),)
        else:
            ordering = (events.c.event_sequence.asc(),)
        statement = sqlalchemy.select(events).where(*conditions).order_by(
            *ordering).limit(query.limit + 1)
        rows = list(connection.execute(statement).mappings())
        has_more = len(rows) > query.limit
        page_rows = rows[:query.limit]

        targets_by_event: dict[uuid.UUID, list[api_models.EventTarget]] = {}
        if page_rows:
            event_ids = [row['event_id'] for row in page_rows]
            target_rows = connection.execute(
                sqlalchemy.select(targets).where(
                    targets.c.event_id.in_(event_ids)).order_by(
                        targets.c.event_id, targets.c.position)).mappings()
            for target in target_rows:
                targets_by_event.setdefault(target['event_id'], []).append(
                    api_models.EventTarget(type=target['target_type'],
                                           id=target['target_id'],
                                           name=target['target_name']))

        items = [
            _event_from_row(row, targets_by_event.get(row['event_id'], []))
            for row in page_rows
        ]
        next_cursor = None
        if has_more and page_rows:
            next_cursor = cursors.issue(
                authority_key,
                bindings,
                query.direction,
                cursors.CursorState(
                    position=int(page_rows[-1]['event_sequence']),
                    high_watermark=page_high_watermark,
                ),
            )
        poll_cursor = cursors.issue(
            authority_key, bindings, api_models.TraversalDirection.NEWER,
            cursors.CursorState(
                position=page_high_watermark,
                high_watermark=page_high_watermark,
            ))
    return api_models.EventsPage(items=items,
                                 next_cursor=next_cursor,
                                 poll_cursor=poll_cursor,
                                 has_more=has_more)


def delete_expired_events(retention_hours: float,
                          batch_size: int = 1000) -> int:
    """Delete one bounded retention batch and cascade its targets."""
    if not is_available() or retention_hours < 0:
        return 0
    engine = request_postgres.initialize_and_get_db()
    events = schema.RESOURCE_EVENTS
    cutoff = (sqlalchemy.func.clock_timestamp() -
              datetime.timedelta(hours=retention_hours))
    with engine.begin() as connection:
        expired = sqlalchemy.select(
            events.c.event_id).where(events.c.occurred_at < cutoff).order_by(
                events.c.occurred_at).limit(batch_size)
        result = connection.execute(
            sqlalchemy.delete(events).where(events.c.event_id.in_(expired)))
        return result.rowcount


async def retention_daemon() -> None:
    """Delete expired operational events in bounded committed batches."""
    while True:
        try:
            skypilot_config.reload_config()
            retention_hours = float(
                skypilot_config.get_nested(
                    ('api_server', 'operational_event_retention_hours'),
                    DEFAULT_OPERATIONAL_EVENT_RETENTION_HOURS))
            if retention_hours >= 0:
                deleted = 0
                while True:
                    batch = await asyncio.to_thread(delete_expired_events,
                                                    retention_hours,
                                                    _RETENTION_BATCH_SIZE)
                    deleted += batch
                    if batch < _RETENTION_BATCH_SIZE:
                        break
                    await asyncio.sleep(0)
                if deleted:
                    logger.info(f'Deleted {deleted} expired operational '
                                'events.')
        except asyncio.CancelledError:
            logger.info('Operational event retention daemon cancelled.')
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Operational event retention failed: {e}')
        await asyncio.sleep(_RETENTION_INTERVAL_SECONDS)
