"""Diagnostic PostgreSQL history for ordinary SkyServe launch handoffs.

This module is deliberately observability-only.  Callers enqueue immutable
events without waiting for PostgreSQL; neither a dropped event nor a writer
failure can authorize, delay, cancel, retry, or project a replica launch.
"""

import asyncio
from collections.abc import Callable
import dataclasses
import datetime
import enum
import hashlib
import json
import queue
import re
import threading
from typing import Any
import uuid

import prometheus_client as prom
import sqlalchemy

from sky import sky_logging
from sky.serve import constants as serve_constants
from sky.serve import serve_state
from sky.utils.db import db_utils

RETENTION_DAYS = 60
MAX_QUERY_DAYS = RETENTION_DAYS
POSTGRES_STATEMENT_TIMEOUT_MS = 1000
MAX_PENDING_EVENTS = 4096
MAX_PENDING_TERMINAL_OBSERVATIONS = 1024
RETENTION_PRUNE_INTERVAL_SECONDS = 5 * 60
RETENTION_PRUNE_BATCH_SIZE = 1000
TERMINAL_STATUS_LOOKUP_TIMEOUT_SECONDS = 5
TERMINAL_OBSERVER_WORKERS = 2

DELIVERY_METRIC_NAME = 'sky_serve_ordinary_launch_handoff_delivery_total'
DELIVERY_OUTCOMES = (
    'event_enqueued',
    'event_queue_dropped',
    'event_persisted',
    'event_backend_unavailable',
    'event_write_failed',
    'event_provenance_rejected',
    'event_provenance_check_failed',
    'terminal_observation_enqueued',
    'terminal_observation_queue_dropped',
    'terminal_lookup_failed',
    'retention_prune_failed',
)
ORDINARY_LAUNCH_HANDOFF_DELIVERY = prom.Counter(
    DELIVERY_METRIC_NAME,
    'Fail-open ordinary-launch handoff telemetry delivery outcomes.',
    ('outcome',))


class EventKind(str, enum.Enum):
    """Closed ordinary-launch handoff observations."""

    REQUEST_PUBLISHED = 'request_published'
    CONTROLLER_START_NONTERMINAL = 'controller_start_nonterminal'
    RESTART_REDRIVE = 'restart_redrive'
    OWNER_LOSS_CANCEL_REQUESTED = 'owner_loss_cancel_requested'
    API_TERMINAL = 'api_terminal'
    SERVE_RESULT_PROJECTED = 'serve_result_projected'
    SERVICE_JOB_OBSERVED = 'service_job_observed'
    CLEANUP_RETRY_AFTER_ROUTE_EPOCH_CHANGE = (
        'cleanup_retry_after_route_epoch_change')


class TerminalStatus(str, enum.Enum):
    """Closed API request terminal states retained by R1 telemetry."""

    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


EVENT_KIND_VALUES = tuple(kind.value for kind in EventKind)
_EVENT_KIND_SQL = ', '.join(f"'{value}'" for value in EVENT_KIND_VALUES)
TERMINAL_STATUS_VALUES = tuple(status.value for status in TerminalStatus)
_TERMINAL_STATUS_SQL = ', '.join(
    f"'{value}'" for value in TERMINAL_STATUS_VALUES)
_SHA256_RE = re.compile(r'[0-9a-f]{64}')

metadata = sqlalchemy.MetaData()

serve_ordinary_launch_handoff_events_table = sqlalchemy.Table(
    'serve_ordinary_launch_handoff_events',
    metadata,
    sqlalchemy.Column('event_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.text('CURRENT_TIMESTAMP')),
    sqlalchemy.Column('event_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_record_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('controller_route_epoch',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('ordinary_request_id', sqlalchemy.Text),
    sqlalchemy.Column('service_job_id', sqlalchemy.BigInteger),
    sqlalchemy.Column('terminal_status', sqlalchemy.Text),
    sqlalchemy.Column('input_digest', sqlalchemy.Text),
    sqlalchemy.CheckConstraint(f'event_kind IN ({_EVENT_KIND_SQL})',
                               name='serve_ordinary_launch_event_kind'),
    sqlalchemy.CheckConstraint('length(service_name) > 0',
                               name='serve_ordinary_launch_service_name'),
    sqlalchemy.CheckConstraint('service_version > 0',
                               name='serve_ordinary_launch_service_version'),
    sqlalchemy.CheckConstraint('replica_id > 0',
                               name='serve_ordinary_launch_replica_id'),
    sqlalchemy.CheckConstraint(
        'ordinary_request_id IS NULL OR length(ordinary_request_id) > 0',
        name='serve_ordinary_launch_request_id'),
    sqlalchemy.CheckConstraint('service_job_id IS NULL OR service_job_id > 0',
                               name='serve_ordinary_launch_service_job_id'),
    sqlalchemy.CheckConstraint(
        "(event_kind = 'api_terminal' AND "
        'terminal_status IS NOT NULL AND '
        f'terminal_status IN ({_TERMINAL_STATUS_SQL})) OR '
        "(event_kind <> 'api_terminal' AND terminal_status IS NULL)",
        name='serve_ordinary_launch_terminal_status'),
    sqlalchemy.CheckConstraint(
        'input_digest IS NULL OR length(input_digest) = 64',
        name='serve_ordinary_launch_input_digest'),
)
sqlalchemy.Index('serve_ordinary_launch_handoff_record_idx',
                 serve_ordinary_launch_handoff_events_table.c.replica_record_id,
                 serve_ordinary_launch_handoff_events_table.c.observed_at,
                 serve_ordinary_launch_handoff_events_table.c.event_id)
sqlalchemy.Index(
    'serve_ordinary_launch_handoff_request_idx',
    serve_ordinary_launch_handoff_events_table.c.ordinary_request_id,
    postgresql_where=(serve_ordinary_launch_handoff_events_table.c.
                      ordinary_request_id.isnot(None)))
sqlalchemy.Index('serve_ordinary_launch_handoff_retention_idx',
                 serve_ordinary_launch_handoff_events_table.c.observed_at)

logger = sky_logging.init_logger(__name__)


@dataclasses.dataclass(frozen=True)
class _Event:
    """Validated immutable values for one asynchronous insert."""

    event_id: uuid.UUID
    event_kind: str
    service_name: str
    service_version: int
    replica_id: int
    replica_record_id: uuid.UUID
    controller_route_epoch: uuid.UUID
    ordinary_request_id: str | None
    service_job_id: int | None
    terminal_status: str | None
    input_digest: str | None

    def insert_values(self) -> dict[str, Any]:
        """Return insert fields; observed_at intentionally uses DB time."""
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class _PendingEvent:
    """One event plus optional server-validated publication provenance."""

    event: _Event
    required_launch_fence: dict[str, Any] | None = None


_pending_events: 'queue.Queue[_PendingEvent]' = queue.Queue(
    maxsize=MAX_PENDING_EVENTS)
_pending_terminal_observations: 'queue.Queue[_TerminalObservation]' = (
    queue.Queue(maxsize=MAX_PENDING_TERMINAL_OBSERVATIONS))
_writer_lock = threading.Lock()
_writer_thread: threading.Thread | None = None
_observer_threads: list[threading.Thread] = []
_event_queue_drop_count = 0
_terminal_observation_queue_drop_count = 0
_write_failure_count = 0
_terminal_lookup_failure_count = 0
_backend_unavailable_count = 0
_retention_prune_failure_count = 0
_provenance_rejected_count = 0
_provenance_check_failure_count = 0


@dataclasses.dataclass(frozen=True)
class _TerminalObservation:
    """One fail-open status observation performed outside the launch path."""

    request_id: str
    lookup: Callable[[str], str | None]
    emit: Callable[[TerminalStatus], None]


def _postgres_engine() -> sqlalchemy.engine.Engine | None:
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    return engine


def _canonical_uuid(value: str, field_name: str) -> uuid.UUID:
    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(
            f'{field_name} must be a canonical UUID string.') from error
    if str(parsed) != value:
        raise ValueError(f'{field_name} must be a canonical UUID string.')
    return parsed


def redacted_input_digest(
    yaml_content: str,
    resources_override: dict[str, Any] | None,
) -> str | None:
    """Hash launch inputs, or fail open when diagnostic encoding is unsafe."""
    try:
        if not isinstance(yaml_content, str):
            raise ValueError('yaml_content must be text.')
        try:
            override = json.dumps(resources_override,
                                  sort_keys=True,
                                  separators=(',', ':'),
                                  ensure_ascii=True,
                                  allow_nan=False)
        except (TypeError, ValueError):
            # Overrides accepted by the existing launch path may contain typed
            # values. Their repr is used only inside this diagnostic hash.
            override = repr(resources_override)
        digest = hashlib.sha256()
        for value in (yaml_content, override):
            encoded = value.encode('utf-8')
            digest.update(len(encoded).to_bytes(8, byteorder='big'))
            digest.update(encoded)
        return digest.hexdigest()
    except Exception as error:  # pylint: disable=broad-except
        # Diagnostic serialization is not part of launch correctness. In
        # particular, arbitrary accepted typed overrides can raise from repr(),
        # and a str subclass can raise while encoding. Callers treat None as a
        # closed instruction to omit all telemetry for that launch attempt.
        logger.debug(
            'Omitting ordinary-launch telemetry because its redacted '
            'input digest could not be computed: %s', error)
        return None


def _event(
    *,
    event_kind: EventKind,
    service_name: str,
    service_version: int,
    replica_id: int,
    replica_record_id: str,
    controller_route_epoch: str,
    ordinary_request_id: str | None,
    service_job_id: int | None,
    input_digest: str | None,
    terminal_status: TerminalStatus | None = None,
) -> _Event:
    if not isinstance(event_kind, EventKind):
        raise ValueError('event_kind must be a closed EventKind.')
    if not isinstance(service_name, str) or not service_name:
        raise ValueError('service_name must be non-empty text.')
    if (isinstance(service_version, bool) or
            not isinstance(service_version, int) or service_version < 1):
        raise ValueError('service_version must be a positive integer.')
    if (isinstance(replica_id, bool) or not isinstance(replica_id, int) or
            replica_id < 1):
        raise ValueError('replica_id must be a positive integer.')
    if ordinary_request_id is not None and (not isinstance(
            ordinary_request_id, str) or not ordinary_request_id):
        raise ValueError('ordinary_request_id must be non-empty text.')
    if service_job_id is not None and (isinstance(service_job_id, bool) or
                                       not isinstance(service_job_id, int) or
                                       service_job_id < 1):
        raise ValueError('service_job_id must be a positive integer.')
    if event_kind == EventKind.API_TERMINAL:
        if not isinstance(terminal_status, TerminalStatus):
            raise ValueError('api_terminal requires a closed terminal status.')
    elif terminal_status is not None:
        raise ValueError('terminal_status is valid only for api_terminal.')
    if input_digest is not None and (not isinstance(input_digest, str) or
                                     _SHA256_RE.fullmatch(input_digest)
                                     is None):
        raise ValueError('input_digest must be lowercase SHA-256.')
    return _Event(
        event_id=uuid.uuid4(),
        event_kind=event_kind.value,
        service_name=service_name,
        service_version=service_version,
        replica_id=replica_id,
        replica_record_id=_canonical_uuid(replica_record_id,
                                          'replica_record_id'),
        controller_route_epoch=_canonical_uuid(controller_route_epoch,
                                               'controller_route_epoch'),
        ordinary_request_id=ordinary_request_id,
        service_job_id=service_job_id,
        terminal_status=(None
                         if terminal_status is None else terminal_status.value),
        input_digest=input_digest,
    )


def _write_event(event: _Event) -> bool:
    """Insert one event on PostgreSQL without coupling retention work."""
    engine = _postgres_engine()
    if engine is None:
        return False
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('SET LOCAL statement_timeout = '
                            f'{POSTGRES_STATEMENT_TIMEOUT_MS}'))
        connection.execute(
            serve_ordinary_launch_handoff_events_table.insert().values(
                **event.insert_values()))
    return True


_PRUNE_EXPIRED_SQL = sqlalchemy.text(f"""
DELETE FROM serve_ordinary_launch_handoff_events
WHERE event_id IN (
    SELECT event_id
    FROM serve_ordinary_launch_handoff_events
    WHERE observed_at < CURRENT_TIMESTAMP - INTERVAL '{RETENTION_DAYS} days'
    ORDER BY observed_at, event_id
    LIMIT :batch_size
)
""")


def _prune_expired_events() -> int:
    """Delete at most one bounded batch of expired PostgreSQL history."""
    engine = _postgres_engine()
    if engine is None:
        return 0
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('SET LOCAL statement_timeout = '
                            f'{POSTGRES_STATEMENT_TIMEOUT_MS}'))
        result = connection.execute(_PRUNE_EXPIRED_SQL,
                                    {'batch_size': RETENTION_PRUNE_BATCH_SIZE})
    return max(result.rowcount, 0)


def _record_delivery_outcome(outcome: str) -> None:
    """Record a fleet-visible outcome without affecting launch behavior."""
    if outcome not in DELIVERY_OUTCOMES:
        return
    try:
        ORDINARY_LAUNCH_HANDOFF_DELIVERY.labels(outcome=outcome).inc()
    except Exception:  # pylint: disable=broad-except
        # Prometheus is evidence about this fail-open diagnostic path.  A bad
        # registry or multiprocess directory cannot become a launch failure.
        logger.debug('Failed to record ordinary-launch delivery metric.',
                     exc_info=True)


def _log_write_failure(error: Exception) -> None:
    global _write_failure_count
    with _writer_lock:
        _write_failure_count += 1
        failure_count = _write_failure_count
    _record_delivery_outcome('event_write_failed')
    if failure_count == 1 or failure_count % 100 == 0:
        logger.warning(
            'Ordinary-launch handoff telemetry write failed; launch behavior '
            'is unchanged (failure %s): %s', failure_count, error)


def _log_terminal_lookup_failure(error: Exception) -> None:
    global _terminal_lookup_failure_count
    with _writer_lock:
        _terminal_lookup_failure_count += 1
        failure_count = _terminal_lookup_failure_count
    _record_delivery_outcome('terminal_lookup_failed')
    if failure_count == 1 or failure_count % 100 == 0:
        logger.warning(
            'Ordinary-launch terminal status lookup failed; launch behavior '
            'is unchanged (failure %s): %s', failure_count, error)


def _record_backend_unavailable() -> None:
    global _backend_unavailable_count
    with _writer_lock:
        _backend_unavailable_count += 1
        unavailable_count = _backend_unavailable_count
    _record_delivery_outcome('event_backend_unavailable')
    if unavailable_count == 1 or unavailable_count % 100 == 0:
        logger.warning(
            'Ordinary-launch handoff telemetry requires PostgreSQL; dropping '
            'one diagnostic event without changing launch behavior '
            '(unavailable %s).', unavailable_count)


def _log_retention_prune_failure(error: Exception) -> None:
    global _retention_prune_failure_count
    with _writer_lock:
        _retention_prune_failure_count += 1
        failure_count = _retention_prune_failure_count
    _record_delivery_outcome('retention_prune_failed')
    if failure_count == 1 or failure_count % 100 == 0:
        logger.warning(
            'Ordinary-launch handoff retention pruning failed; event writes '
            'and launch behavior are unchanged (failure %s): %s', failure_count,
            error)


def _record_provenance_rejected() -> None:
    global _provenance_rejected_count
    with _writer_lock:
        _provenance_rejected_count += 1
        rejected_count = _provenance_rejected_count
    _record_delivery_outcome('event_provenance_rejected')
    if rejected_count == 1 or rejected_count % 100 == 0:
        logger.warning(
            'Ordinary-launch publication telemetry rejected a stale or invalid '
            'service-owner fence (rejection %s).', rejected_count)


def _record_provenance_check_failure(error: Exception) -> None:
    global _provenance_check_failure_count
    with _writer_lock:
        _provenance_check_failure_count += 1
        failure_count = _provenance_check_failure_count
    _record_delivery_outcome('event_provenance_check_failed')
    if failure_count == 1 or failure_count % 100 == 0:
        logger.warning(
            'Ordinary-launch publication telemetry could not validate its '
            'service-owner fence; dropping evidence without changing launch '
            'behavior (failure %s): %s', failure_count, error)


def _pending_event_provenance_holds(pending: _PendingEvent) -> bool:
    """Validate API-side publication provenance outside the request path."""
    launch_fence = pending.required_launch_fence
    if launch_fence is None:
        return True
    try:
        authorized = serve_state.service_replica_launch_fence_holds(
            launch_fence)
    except Exception as error:  # pylint: disable=broad-except
        _record_provenance_check_failure(error)
        return False
    if not authorized:
        _record_provenance_rejected()
        return False
    return True


def _writer_loop() -> None:
    while True:
        pending = _pending_events.get()
        try:
            if not _pending_event_provenance_holds(pending):
                continue
            if _write_event(pending.event):
                _record_delivery_outcome('event_persisted')
            else:
                _record_backend_unavailable()
        except Exception as error:  # pylint: disable=broad-except
            _log_write_failure(error)
        finally:
            _pending_events.task_done()


async def retention_daemon() -> None:
    """Delete one bounded batch per distributed-singleton cadence."""
    while True:
        try:
            await asyncio.sleep(RETENTION_PRUNE_INTERVAL_SECONDS)
            await asyncio.to_thread(_prune_expired_events)
        except asyncio.CancelledError:
            logger.info('Ordinary-launch handoff retention daemon cancelled.')
            raise
        except Exception as error:  # pylint: disable=broad-except
            _log_retention_prune_failure(error)


def _process_terminal_observation(observation: _TerminalObservation) -> None:
    """Look up and emit one closed status, swallowing diagnostic failures."""
    try:
        status_value = observation.lookup(observation.request_id)
    except Exception as error:  # pylint: disable=broad-except
        _log_terminal_lookup_failure(error)
        return
    try:
        terminal_status = TerminalStatus(status_value)
    except (TypeError, ValueError):
        # Missing and active statuses are observations, but they are not
        # terminal evidence and must never be classified as such.
        return
    try:
        observation.emit(terminal_status)
    except Exception as error:  # pylint: disable=broad-except
        _log_terminal_lookup_failure(error)


def _terminal_observer_loop(stop_event: threading.Event | None = None) -> None:
    while stop_event is None or not stop_event.is_set():
        try:
            observation = _pending_terminal_observations.get(
                timeout=0.05 if stop_event is not None else None)
        except queue.Empty:
            continue
        try:
            _process_terminal_observation(observation)
        finally:
            _pending_terminal_observations.task_done()


def _ensure_writer() -> None:
    global _writer_thread
    with _writer_lock:
        if _writer_thread is not None and _writer_thread.is_alive():
            return
        _writer_thread = threading.Thread(
            target=_writer_loop,
            name='serve-ordinary-launch-handoff-writer',
            daemon=True)
        _writer_thread.start()


def _ensure_terminal_observer() -> None:
    with _writer_lock:
        _observer_threads[:] = [
            thread for thread in _observer_threads if thread.is_alive()
        ]
        while len(_observer_threads) < TERMINAL_OBSERVER_WORKERS:
            worker_number = len(_observer_threads) + 1
            observer_thread = threading.Thread(
                target=_terminal_observer_loop,
                name=('serve-ordinary-launch-terminal-observer-'
                      f'{worker_number}'),
                daemon=True)
            _observer_threads.append(observer_thread)
            observer_thread.start()


def _record_queue_drop(*, terminal_observation: bool) -> None:
    global _event_queue_drop_count, _terminal_observation_queue_drop_count
    with _writer_lock:
        if terminal_observation:
            _terminal_observation_queue_drop_count += 1
        else:
            _event_queue_drop_count += 1
        drop_count = (_event_queue_drop_count +
                      _terminal_observation_queue_drop_count)
    queue_name = ('terminal-observation' if terminal_observation else 'event')
    _record_delivery_outcome('terminal_observation_queue_dropped'
                             if terminal_observation else 'event_queue_dropped')
    if drop_count == 1 or drop_count % 100 == 0:
        logger.warning(
            'Ordinary-launch handoff telemetry %s queue is full; dropping '
            'one diagnostic item without changing launch behavior (drop %s).',
            queue_name, drop_count)


def _enqueue_event(
    event: _Event,
    *,
    required_launch_fence: dict[str, Any] | None = None,
) -> bool:
    try:
        _ensure_writer()
        _pending_events.put_nowait(
            _PendingEvent(event=event,
                          required_launch_fence=required_launch_fence))
    except queue.Full:
        _record_queue_drop(terminal_observation=False)
        return False
    except Exception as error:  # pylint: disable=broad-except
        _log_write_failure(error)
        return False
    _record_delivery_outcome('event_enqueued')
    return True


def observe_terminal_nonblocking(
    request_id: str,
    *,
    lookup: Callable[[str], str | None],
    emit: Callable[[TerminalStatus], None],
) -> bool:
    """Queue an exact request-status observation without delaying a launch.

    Lookup failures, nonterminal states, callback failures, and queue pressure
    are diagnostic only.  They cannot change the launch state machine.
    """
    if (not isinstance(request_id, str) or not request_id or
            not callable(lookup) or not callable(emit)):
        return False
    try:
        _ensure_terminal_observer()
        _pending_terminal_observations.put_nowait(
            _TerminalObservation(request_id=request_id,
                                 lookup=lookup,
                                 emit=emit))
    except queue.Full:
        _record_queue_drop(terminal_observation=True)
        return False
    except Exception as error:  # pylint: disable=broad-except
        _log_terminal_lookup_failure(error)
        return False
    _record_delivery_outcome('terminal_observation_enqueued')
    return True


def emit_event(
    *,
    event_kind: EventKind,
    service_name: str,
    service_version: int,
    replica_id: int,
    replica_record_id: str,
    controller_route_epoch: str,
    ordinary_request_id: str | None = None,
    service_job_id: int | None = None,
    terminal_status: TerminalStatus | None = None,
    input_digest: str | None = None,
) -> bool:
    """Non-blockingly enqueue one diagnostic event.

    Invalid diagnostic inputs fail open.  This function must never affect the
    ordinary launch state machine, including through validation errors.
    """
    try:
        event = _event(event_kind=event_kind,
                       service_name=service_name,
                       service_version=service_version,
                       replica_id=replica_id,
                       replica_record_id=replica_record_id,
                       controller_route_epoch=controller_route_epoch,
                       ordinary_request_id=ordinary_request_id,
                       service_job_id=service_job_id,
                       terminal_status=terminal_status,
                       input_digest=input_digest)
        return _enqueue_event(event)
    except Exception as error:  # pylint: disable=broad-except
        logger.debug('Dropping invalid ordinary-launch diagnostic event: %s',
                     error)
        return False


def emit_verified_request_publication(
    *,
    service_name: str,
    service_version: int,
    replica_id: int,
    replica_record_id: str,
    controller_route_epoch: str,
    ordinary_request_id: str,
    input_digest: str,
    launch_fence: dict[str, Any],
) -> bool:
    """Queue API publication only with exact durable-owner provenance.

    Structural checks happen synchronously, but the fresh PostgreSQL authority
    read runs in the event writer.  Consequently an invalid, stale, or
    unavailable fence drops only telemetry and never delays the HTTP response.
    """
    try:
        if not isinstance(launch_fence, dict) or not all(
                key in launch_fence
                for key in serve_constants.REPLICA_LAUNCH_FENCE_KEYS):
            raise ValueError('request publication requires a complete fence.')
        closed_fence = {
            key: launch_fence[key]
            for key in serve_constants.REPLICA_LAUNCH_FENCE_KEYS
        }
        if (closed_fence[serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY]
                != service_name or closed_fence[
                    serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY]
                != service_version):
            raise ValueError(
                'request publication identity does not match its launch fence.')
        event = _event(event_kind=EventKind.REQUEST_PUBLISHED,
                       service_name=service_name,
                       service_version=service_version,
                       replica_id=replica_id,
                       replica_record_id=replica_record_id,
                       controller_route_epoch=controller_route_epoch,
                       ordinary_request_id=ordinary_request_id,
                       service_job_id=None,
                       input_digest=input_digest)
        return _enqueue_event(event, required_launch_fence=closed_fence)
    except Exception as error:  # pylint: disable=broad-except
        logger.debug('Dropping invalid ordinary-launch publication context: %s',
                     error)
        return False


_SUMMARY_SQL = sqlalchemy.text("""
WITH window_events AS (
    SELECT *
    FROM serve_ordinary_launch_handoff_events
    WHERE observed_at >= CURRENT_TIMESTAMP - (:days * INTERVAL '1 day')
      AND observed_at <= CURRENT_TIMESTAMP
),
projections AS (
    SELECT replica_record_id, MIN(observed_at) AS first_projection_at
    FROM window_events
    WHERE event_kind = 'serve_result_projected'
    GROUP BY replica_record_id
),
multi_request_records AS (
    SELECT event.replica_record_id
    FROM window_events AS event
    LEFT JOIN projections AS projection USING (replica_record_id)
    WHERE event.event_kind = 'request_published'
      AND event.ordinary_request_id IS NOT NULL
      AND (projection.first_projection_at IS NULL OR
           event.observed_at < projection.first_projection_at)
    GROUP BY event.replica_record_id
    HAVING COUNT(DISTINCT event.ordinary_request_id) > 1
),
redrives AS (
    SELECT redrive.*,
           (
               SELECT publication.ordinary_request_id
               FROM window_events AS publication
               WHERE publication.replica_record_id = redrive.replica_record_id
                 AND publication.event_kind = 'request_published'
                 AND publication.ordinary_request_id IS NOT NULL
                 AND publication.observed_at < redrive.observed_at
               ORDER BY publication.observed_at DESC,
                        publication.event_id DESC
               LIMIT 1
           ) AS predecessor_request_id
    FROM window_events AS redrive
    WHERE redrive.event_kind = 'restart_redrive'
),
redrive_classes AS (
    SELECT redrive.event_id,
           EXISTS (
               SELECT 1
               FROM window_events AS terminal
               WHERE terminal.replica_record_id = redrive.replica_record_id
                 AND terminal.event_kind = 'api_terminal'
                 AND terminal.ordinary_request_id =
                     redrive.predecessor_request_id
                 AND terminal.observed_at < redrive.observed_at
           ) AS predecessor_terminal,
           EXISTS (
               SELECT 1
               FROM window_events AS projection
               WHERE projection.replica_record_id =
                     redrive.replica_record_id
                 AND projection.event_kind = 'serve_result_projected'
                 AND projection.observed_at < redrive.observed_at
           ) AS predecessor_projected,
           redrive.predecessor_request_id IS NOT NULL AS has_predecessor
    FROM redrives AS redrive
),
duplicate_service_jobs AS (
    SELECT replica_record_id
    FROM window_events
    WHERE event_kind = 'service_job_observed'
      AND service_job_id IS NOT NULL
    GROUP BY replica_record_id
    HAVING COUNT(DISTINCT service_job_id) > 1
)
SELECT
    (SELECT COUNT(*) FROM window_events) AS observed_events,
    (SELECT COUNT(DISTINCT replica_record_id) FROM window_events
     WHERE event_kind = 'request_published') AS eligible_ordinary_launches,
    (SELECT COUNT(*) FROM (
         SELECT DISTINCT service_name, controller_route_epoch
         FROM window_events
         WHERE event_kind = 'controller_start_nonterminal'
     ) AS controller_starts) AS
        controller_starts_during_nonterminal_launches,
    (SELECT COUNT(*) FROM multi_request_records) AS
        replica_records_with_multiple_requests_before_projection,
    (SELECT COUNT(*) FROM redrive_classes
     WHERE has_predecessor AND NOT predecessor_terminal) AS
        restart_redrives_with_predecessor_status_unknown,
    (SELECT COUNT(*) FROM redrive_classes
     WHERE predecessor_terminal AND NOT predecessor_projected) AS
        restart_redrives_with_terminal_unprojected_predecessor,
    (SELECT COUNT(*) FROM redrive_classes
     WHERE NOT has_predecessor) AS
        restart_redrives_without_observed_predecessor,
    (SELECT COUNT(*) FROM duplicate_service_jobs) AS
        replica_records_with_duplicate_service_jobs,
    (SELECT COUNT(*) FROM (
         SELECT DISTINCT replica_record_id, ordinary_request_id
         FROM window_events
         WHERE event_kind = 'owner_loss_cancel_requested'
     ) AS cancellation_requests) AS owner_loss_cancellation_requests,
    (SELECT COUNT(*) FROM window_events
     WHERE event_kind = 'cleanup_retry_after_route_epoch_change') AS
        cleanup_retries_after_route_epoch_change
""")


def _process_local_delivery_summary() -> dict[str, Any]:
    """Snapshot lossy-delivery diagnostics scoped to this Python process."""
    with _writer_lock:
        event_queue_drops = _event_queue_drop_count
        terminal_observation_queue_drops = (
            _terminal_observation_queue_drop_count)
        writer_failures = _write_failure_count
        terminal_lookup_failures = _terminal_lookup_failure_count
        backend_unavailable = _backend_unavailable_count
        retention_prune_failures = _retention_prune_failure_count
        provenance_rejections = _provenance_rejected_count
        provenance_check_failures = _provenance_check_failure_count
    return {
        'scope': 'current_process_since_module_import',
        'queue_drops': (event_queue_drops + terminal_observation_queue_drops),
        'event_queue_drops': event_queue_drops,
        'terminal_observation_queue_drops': (terminal_observation_queue_drops),
        'writer_failures': writer_failures,
        'terminal_lookup_failures': terminal_lookup_failures,
        'backend_unavailable': backend_unavailable,
        'retention_prune_failures': retention_prune_failures,
        'provenance_rejections': provenance_rejections,
        'provenance_check_failures': provenance_check_failures,
        'pending_events': _pending_events.qsize(),
        'pending_terminal_observations':
            (_pending_terminal_observations.qsize()),
    }


def get_summary(*, days: int = RETENTION_DAYS) -> dict[str, Any]:
    """Return lower-bound evidence counters for the retained history window."""
    if (isinstance(days, bool) or not isinstance(days, int) or days < 1 or
            days > MAX_QUERY_DAYS):
        raise ValueError(f'days must be an integer from 1 to {MAX_QUERY_DAYS}.')
    engine = _postgres_engine()
    if engine is None:
        return {
            'available': False,
            'retention_days': RETENTION_DAYS,
            'evidence_is_lower_bound': True,
            'fleet_delivery_metric': DELIVERY_METRIC_NAME,
            'fleet_delivery_outcomes': list(DELIVERY_OUTCOMES),
            'process_local_delivery': _process_local_delivery_summary(),
        }
    with engine.connect() as connection:
        row = connection.execute(_SUMMARY_SQL, {'days': days}).mappings().one()
        clock = connection.execute(
            sqlalchemy.text('SELECT CURRENT_TIMESTAMP')).scalar_one()
    counters = {str(key): int(value) for key, value in row.items()}
    return {
        'available': True,
        'retention_days': RETENTION_DAYS,
        'evidence_is_lower_bound': True,
        'fleet_delivery_metric': DELIVERY_METRIC_NAME,
        'fleet_delivery_outcomes': list(DELIVERY_OUTCOMES),
        'window_start': (clock - datetime.timedelta(days=days)).timestamp(),
        'window_end': clock.timestamp(),
        'process_local_delivery': _process_local_delivery_summary(),
        **counters,
    }
