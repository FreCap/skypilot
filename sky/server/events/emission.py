"""Transactional operational event emission."""

from __future__ import annotations

from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky import sky_logging
from sky.events import api_models
from sky.server.events import models
from sky.server.events import schema

logger = sky_logging.init_logger(__name__)


def _allocate_event_sequence(connection: sqlalchemy.engine.Connection) -> int:
    """Allocate one commit-ordered sequence in the caller's transaction."""
    metadata_row = connection.execute(
        sqlalchemy.select(schema.REQUEST_STORE_METADATA.c.value).where(
            schema.REQUEST_STORE_METADATA.c.key ==
            schema.CURSOR_AUTHORITY_METADATA_KEY).with_for_update()
    ).scalar_one_or_none()
    if not isinstance(metadata_row, dict):
        raise RuntimeError('Operational event sequence state is unavailable.')
    current = metadata_row.get('event_sequence')
    if (isinstance(current, bool) or not isinstance(current, int) or
            current < 0):
        raise RuntimeError('Operational event sequence state is invalid.')
    event_sequence = current + 1
    if event_sequence > 2**63 - 1:
        raise RuntimeError('Operational event sequence is exhausted.')
    updated_metadata = dict(metadata_row)
    updated_metadata['event_sequence'] = event_sequence
    connection.execute(
        sqlalchemy.update(schema.REQUEST_STORE_METADATA).where(
            schema.REQUEST_STORE_METADATA.c.key ==
            schema.CURSOR_AUTHORITY_METADATA_KEY).values(
                value=updated_metadata,
                updated_at=sqlalchemy.func.clock_timestamp()))
    return event_sequence


def emit_terminal_event(
    connection: sqlalchemy.engine.Connection,
    request_row: dict[str, Any],
    *,
    status: str,
    cause: api_models.EventCause,
) -> bool:
    """Insert one normalized terminal event in the caller's transaction."""
    raw_context = request_row.get('event_context')
    if raw_context is None:
        return False
    try:
        context = models.EventContext.model_validate(raw_context)
    except (TypeError, ValueError) as e:
        logger.warning('Skipping malformed operational event context for '
                       f'request {request_row.get("request_id")}: {e}')
        return False
    if not context.complete:
        return False
    expected_kind = models.request_kind(str(request_row['name']))
    if expected_kind is None or expected_kind != context.kind:
        logger.warning('Skipping mismatched operational event context for '
                       f'request {request_row["request_id"]}.')
        return False

    outcome = models.outcome_for_status(status)
    event_id = uuid.uuid4()
    event_sequence = _allocate_event_sequence(connection)
    event_values = {
        'event_id': event_id,
        'event_sequence': event_sequence,
        'workspace': context.workspace,
        'kind': context.kind.value,
        'phase': api_models.EventPhase.TERMINAL.value,
        'outcome': outcome.value,
        'cause': cause.value,
        'message': models.safe_message(context.kind, outcome, cause),
        'actor_id': str(request_row['user_id']),
        'actor_name': context.actor_name,
        'actor_type': context.actor_type,
        'source_request_id': str(request_row['request_id']),
        'source_execution_generation': int(
            request_row.get('execution_generation') or 0),
    }
    inserted = connection.execute(
        postgresql.insert(schema.RESOURCE_EVENTS).values(
            **event_values).on_conflict_do_nothing(
                constraint='uq_resource_events_source_phase').returning(
                    schema.RESOURCE_EVENTS.c.event_id)).scalar_one_or_none()
    if inserted is None:
        return False
    connection.execute(sqlalchemy.insert(schema.RESOURCE_EVENT_TARGETS), [{
        'event_id': event_id,
        'position': position,
        'target_type': target.type,
        'target_id': target.id,
        'target_name': target.name,
    } for position, target in enumerate(context.targets)])
    return True
