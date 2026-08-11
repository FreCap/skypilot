"""Diagnostic PostgreSQL history for ordinary SkyServe launch handoffs.

This module is deliberately observability-only.  Callers enqueue immutable
events without waiting for PostgreSQL; neither a dropped event nor a writer
failure can authorize, delay, cancel, retry, or project a replica launch.
"""

import dataclasses
import datetime
import enum
import hashlib
import json
import queue
import re
import threading
from typing import Any, Callable
import uuid

import sqlalchemy

from sky import sky_logging
from sky.serve import serve_state
from sky.utils.db import db_utils

RETENTION_DAYS = 60
MAX_QUERY_DAYS = RETENTION_DAYS
POSTGRES_STATEMENT_TIMEOUT_MS = 1000
MAX_PENDING_EVENTS = 4096
MAX_PENDING_TERMINAL_OBSERVATIONS = 1024


class EventKind(str, enum.Enum):
    """Closed ordinary-launch handoff observations."""

    REQUEST_PUBLISHED = 'request_published'
    CONTROLLER_START_NONTERMINAL = 'controller_start_nonterminal'
    RESTART_REDRIVE = 'restart_redrive'
    OWNER_LOSS_CANCELLED = 'owner_loss_cancelled'
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


_pending_events: 'queue.Queue[_Event]' = queue.Queue(maxsize=MAX_PENDING_EVENTS)
_pending_terminal_observations: 'queue.Queue[_TerminalObservation]' = (
    queue.Queue(maxsize=MAX_PENDING_TERMINAL_OBSERVATIONS))
_writer_lock = threading.Lock()
_writer_thread: threading.Thread | None = None
_observer_thread: threading.Thread | None = None
_event_queue_drop_count = 0
_terminal_observation_queue_drop_count = 0
_write_failure_count = 0
_terminal_lookup_failure_count = 0


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
) -> str:
    """Hash ordinary launch inputs without retaining either input payload."""
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
        # values.  Their repr is used only inside this one-way diagnostic hash.
        override = repr(resources_override)
    digest = hashlib.sha256()
    for value in (yaml_content, override):
        encoded = value.encode('utf-8')
        digest.update(len(encoded).to_bytes(8, byteorder='big'))
        digest.update(encoded)
    return digest.hexdigest()


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
    """Insert one event and prune expired history on PostgreSQL only."""
    engine = _postgres_engine()
    if engine is None:
        return False
    table = serve_ordinary_launch_handoff_events_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('SET LOCAL statement_timeout = '
                            f'{POSTGRES_STATEMENT_TIMEOUT_MS}'))
        connection.execute(table.insert().values(**event.insert_values()))
        connection.execute(
            sqlalchemy.delete(table).where(table.c.observed_at < sqlalchemy.
                                           text('CURRENT_TIMESTAMP - INTERVAL '
                                                f"'{RETENTION_DAYS} days'")))
    return True


def _log_write_failure(error: Exception) -> None:
    global _write_failure_count
    with _writer_lock:
        _write_failure_count += 1
        failure_count = _write_failure_count
    if failure_count == 1 or failure_count % 100 == 0:
        logger.warning(
            'Ordinary-launch handoff telemetry write failed; launch behavior '
            'is unchanged (failure %s): %s', failure_count, error)


def _log_terminal_lookup_failure(error: Exception) -> None:
    global _terminal_lookup_failure_count
    with _writer_lock:
        _terminal_lookup_failure_count += 1
        failure_count = _terminal_lookup_failure_count
    if failure_count == 1 or failure_count % 100 == 0:
        logger.warning(
            'Ordinary-launch terminal status lookup failed; launch behavior '
            'is unchanged (failure %s): %s', failure_count, error)


def _writer_loop() -> None:
    while True:
        event = _pending_events.get()
        try:
            _write_event(event)
        except Exception as error:  # pylint: disable=broad-except
            _log_write_failure(error)
        finally:
            _pending_events.task_done()


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


def _terminal_observer_loop() -> None:
    while True:
        observation = _pending_terminal_observations.get()
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
    global _observer_thread
    with _writer_lock:
        if _observer_thread is not None and _observer_thread.is_alive():
            return
        _observer_thread = threading.Thread(
            target=_terminal_observer_loop,
            name='serve-ordinary-launch-terminal-observer',
            daemon=True)
        _observer_thread.start()


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
    if drop_count == 1 or drop_count % 100 == 0:
        logger.warning(
            'Ordinary-launch handoff telemetry %s queue is full; dropping '
            'one diagnostic item without changing launch behavior (drop %s).',
            queue_name, drop_count)


def _enqueue_event(event: _Event) -> bool:
    try:
        _ensure_writer()
        _pending_events.put_nowait(event)
    except queue.Full:
        _record_queue_drop(terminal_observation=False)
        return False
    except Exception as error:  # pylint: disable=broad-except
        _log_write_failure(error)
        return False
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
    (SELECT COUNT(DISTINCT replica_record_id) FROM window_events
     WHERE event_kind = 'request_published') AS eligible_ordinary_launches,
    (SELECT COUNT(*) FROM window_events
     WHERE event_kind = 'controller_start_nonterminal') AS
        controller_starts_during_nonterminal_launches,
    (SELECT COUNT(*) FROM multi_request_records) AS
        replica_records_with_multiple_requests_before_projection,
    (SELECT COUNT(*) FROM redrive_classes
     WHERE has_predecessor AND NOT predecessor_terminal) AS
        restart_redrives_with_active_predecessor,
    (SELECT COUNT(*) FROM redrive_classes
     WHERE predecessor_terminal AND NOT predecessor_projected) AS
        restart_redrives_with_terminal_unprojected_predecessor,
    (SELECT COUNT(*) FROM duplicate_service_jobs) AS
        replica_records_with_duplicate_service_jobs,
    (SELECT COUNT(*) FROM window_events
     WHERE event_kind = 'owner_loss_cancelled') AS owner_loss_cancellations,
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
    return {
        'scope': 'current_process_since_module_import',
        'queue_drops': (event_queue_drops + terminal_observation_queue_drops),
        'event_queue_drops': event_queue_drops,
        'terminal_observation_queue_drops': (terminal_observation_queue_drops),
        'writer_failures': writer_failures,
        'terminal_lookup_failures': terminal_lookup_failures,
        'pending_events': _pending_events.qsize(),
        'pending_terminal_observations':
            (_pending_terminal_observations.qsize()),
    }


def get_summary(*, days: int = RETENTION_DAYS) -> dict[str, Any]:
    """Return the evidence-gate counters for the retained history window."""
    if (isinstance(days, bool) or not isinstance(days, int) or days < 1 or
            days > MAX_QUERY_DAYS):
        raise ValueError(f'days must be an integer from 1 to {MAX_QUERY_DAYS}.')
    engine = _postgres_engine()
    if engine is None:
        return {
            'available': False,
            'retention_days': RETENTION_DAYS,
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
        'window_start': (clock - datetime.timedelta(days=days)).timestamp(),
        'window_end': clock.timestamp(),
        'process_local_delivery': _process_local_delivery_summary(),
        **counters,
    }
