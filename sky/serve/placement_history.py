"""Bounded PostgreSQL history for SkyServe placement decisions."""

import base64
import datetime
import json
import math
import re
import threading
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky import sky_logging
from sky.serve import serve_state
from sky.utils.db import db_utils

RETENTION_HOURS = 24
MAX_EVENTS_PER_REQUEST = 64
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 50
ERROR_SUMMARY_MAX_LENGTH = 500
POSTGRES_STATEMENT_TIMEOUT_MS = 1000

metadata = sqlalchemy.MetaData()

serve_placement_events_table = sqlalchemy.Table(
    'serve_placement_events',
    metadata,
    sqlalchemy.Column('event_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('attempt_ordinal', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('outcome', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider', sqlalchemy.Text),
    sqlalchemy.Column('region', sqlalchemy.Text),
    sqlalchemy.Column('zone', sqlalchemy.Text),
    sqlalchemy.Column('instance_type', sqlalchemy.Text),
    sqlalchemy.Column(
        'accelerators',
        sqlalchemy.JSON().with_variant(postgresql.JSONB(), 'postgresql')),
    sqlalchemy.Column('use_spot', sqlalchemy.Boolean),
    sqlalchemy.Column('num_nodes', sqlalchemy.Integer),
    sqlalchemy.Column('hourly_price', sqlalchemy.Float),
    sqlalchemy.Column('price_source', sqlalchemy.Text),
    sqlalchemy.Column('error_code', sqlalchemy.Text),
    sqlalchemy.Column('error_summary', sqlalchemy.Text),
    sqlalchemy.CheckConstraint('attempt_ordinal >= 0',
                               name='serve_placement_event_ordinal'),
    sqlalchemy.CheckConstraint('num_nodes IS NULL OR num_nodes > 0',
                               name='serve_placement_event_nodes'),
)
sqlalchemy.Index('serve_placement_events_lookup_idx',
                 serve_placement_events_table.c.service_name,
                 serve_placement_events_table.c.service_hash,
                 serve_placement_events_table.c.observed_at.desc(),
                 serve_placement_events_table.c.event_id.desc())
sqlalchemy.Index('serve_placement_events_retention_idx',
                 serve_placement_events_table.c.observed_at)

logger = sky_logging.init_logger(__name__)

_ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
_buffer_lock = threading.Lock()
_request_events: list[dict[str, Any]] = []
_attempt_count = 0
_buffer_truncated = False


def _postgres_engine() -> sqlalchemy.engine.Engine | None:
    """Return the central PostgreSQL engine, or None when unsupported."""
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    return engine


def _utc_datetime(timestamp: float | None = None) -> datetime.datetime:
    if timestamp is None:
        return datetime.datetime.now(datetime.timezone.utc)
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def reset_request_buffer() -> None:
    """Start a fresh process-local buffer for one API request execution."""
    global _attempt_count, _buffer_truncated
    with _buffer_lock:
        _request_events.clear()
        _attempt_count = 0
        _buffer_truncated = False


def _bounded_error_summary(error_summary: str | None) -> str | None:
    if not error_summary:
        return None
    summary = _ANSI_ESCAPE_RE.sub('', error_summary)
    summary = ' '.join(summary.split())
    if not summary:
        return None
    return summary[:ERROR_SUMMARY_MAX_LENGTH]


def record_event(
    *,
    service_name: str,
    service_hash: str,
    request_id: str,
    cluster_name: str,
    outcome: str,
    replica_id: int | None = None,
    provider: str | None = None,
    region: str | None = None,
    zone: str | None = None,
    instance_type: str | None = None,
    accelerators: dict[str, int] | None = None,
    use_spot: bool | None = None,
    num_nodes: int | None = None,
    hourly_price: float | None = None,
    error_code: str | None = None,
    error_summary: str | None = None,
    timestamp: float | None = None,
) -> bool:
    """Append one normalized event without performing any database I/O."""
    if not service_name or not service_hash or not request_id or not cluster_name:
        return False
    if hourly_price is not None and (not math.isfinite(hourly_price) or
                                     hourly_price < 0):
        hourly_price = None

    global _attempt_count, _buffer_truncated
    with _buffer_lock:
        attempt_ordinal = _attempt_count
        _attempt_count += 1
        if len(_request_events) >= MAX_EVENTS_PER_REQUEST:
            _buffer_truncated = True
            return False
        _request_events.append({
            'event_id': uuid.uuid4().hex,
            'service_name': service_name,
            'service_hash': service_hash,
            'request_id': request_id,
            'replica_id': replica_id,
            'cluster_name': cluster_name,
            'attempt_ordinal': attempt_ordinal,
            'observed_at': _utc_datetime(timestamp),
            'outcome': outcome,
            'provider': provider,
            'region': region,
            'zone': zone,
            'instance_type': instance_type,
            'accelerators': accelerators,
            'use_spot': use_spot,
            'num_nodes': num_nodes,
            'hourly_price': hourly_price,
            'price_source':
                ('catalog_at_decision' if hourly_price is not None else None),
            'error_code': error_code,
            'error_summary': _bounded_error_summary(error_summary),
        })
    return True


def _drain_request_buffer() -> tuple[list[dict[str, Any]], bool]:
    global _buffer_truncated
    with _buffer_lock:
        events = list(_request_events)
        truncated = _buffer_truncated
        _request_events.clear()
        _buffer_truncated = False
    return events, truncated


def flush_request_buffer() -> int:
    """Persist the current request buffer once; observability stays optional."""
    events, truncated = _drain_request_buffer()
    if truncated:
        logger.warning('Placement history capped at %s events for one request.',
                       MAX_EVENTS_PER_REQUEST)
    if not events:
        return 0
    engine = _postgres_engine()
    if engine is None:
        return 0
    cutoff = _utc_datetime() - datetime.timedelta(hours=RETENTION_HOURS)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('SET LOCAL statement_timeout = '
                            f'{POSTGRES_STATEMENT_TIMEOUT_MS}'))
        insert = postgresql.insert(serve_placement_events_table).values(events)
        connection.execute(
            insert.on_conflict_do_nothing(
                index_elements=[serve_placement_events_table.c.event_id]))
        connection.execute(
            sqlalchemy.delete(serve_placement_events_table).where(
                serve_placement_events_table.c.observed_at < cutoff))
    return len(events)


def _encode_cursor(observed_at: datetime.datetime, event_id: str) -> str:
    payload = json.dumps([observed_at.timestamp(), event_id],
                         separators=(',', ':')).encode('utf-8')
    return base64.urlsafe_b64encode(payload).decode('ascii').rstrip('=')


def _decode_cursor(cursor: str) -> tuple[datetime.datetime, str]:
    if not isinstance(cursor, str) or not cursor or len(cursor) > 512:
        raise ValueError('cursor must be a non-empty string.')
    try:
        padding = '=' * (-len(cursor) % 4)
        decoded = base64.urlsafe_b64decode(cursor + padding)
        timestamp, event_id = json.loads(decoded)
        if (not isinstance(timestamp,
                           (int, float)) or isinstance(timestamp, bool) or
                not isinstance(event_id, str) or not event_id):
            raise ValueError
        timestamp = float(timestamp)
        if not math.isfinite(timestamp) or len(event_id) > 128:
            raise ValueError
        return _utc_datetime(timestamp), event_id
    except (ValueError, TypeError, OverflowError, json.JSONDecodeError) as e:
        raise ValueError('Invalid placement-history cursor.') from e


def _empty_history(available: bool) -> dict[str, Any]:
    return {
        'available': available,
        'retention_hours': RETENTION_HOURS,
        'outcome_counts': {},
        'events': [],
        'next_cursor': None,
    }


def get_history(
    service_name: str,
    service_hash: str,
    *,
    hours: int = RETENTION_HOURS,
    limit: int = DEFAULT_PAGE_SIZE,
    cursor: str | None = None,
    timestamp: float | None = None,
) -> dict[str, Any]:
    """Return one newest-first page for an exact service incarnation."""
    if (not isinstance(hours, int) or isinstance(hours, bool) or hours < 1 or
            hours > RETENTION_HOURS):
        raise ValueError(f'hours must be an integer from 1 to '
                         f'{RETENTION_HOURS}.')
    if (not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or
            limit > MAX_PAGE_SIZE):
        raise ValueError(f'limit must be an integer from 1 to '
                         f'{MAX_PAGE_SIZE}.')
    engine = _postgres_engine()
    if engine is None:
        return _empty_history(False)

    observed_at = _utc_datetime(timestamp)
    window_start = observed_at - datetime.timedelta(hours=hours)
    table = serve_placement_events_table
    predicates = [
        table.c.service_name == service_name,
        table.c.service_hash == service_hash,
        table.c.observed_at >= window_start,
        table.c.observed_at <= observed_at,
    ]
    if cursor is not None:
        cursor_time, cursor_event_id = _decode_cursor(cursor)
        predicates.append(
            sqlalchemy.or_(
                table.c.observed_at < cursor_time,
                sqlalchemy.and_(table.c.observed_at == cursor_time,
                                table.c.event_id < cursor_event_id)))

    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(table).where(*predicates).order_by(
                table.c.observed_at.desc(),
                table.c.event_id.desc()).limit(limit + 1)).mappings().all()
        count_rows = session.execute(
            sqlalchemy.select(
                table.c.outcome,
                sqlalchemy.func.count().label(  # pylint: disable=not-callable
                    'count')).where(
                        table.c.service_name == service_name,
                        table.c.service_hash == service_hash,
                        table.c.observed_at >= window_start, table.c.observed_at
                        <= observed_at).group_by(table.c.outcome)).all()

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    events = []
    for row in page_rows:
        events.append({
            key: (row[key].timestamp() if key == 'observed_at' else row[key])
            for key in row.keys()
            if key != 'service_hash'
        })
    next_cursor = None
    if has_more and page_rows:
        last = page_rows[-1]
        next_cursor = _encode_cursor(last['observed_at'], last['event_id'])
    return {
        'available': True,
        'retention_hours': RETENTION_HOURS,
        'window_start': window_start.timestamp(),
        'window_end': observed_at.timestamp(),
        'outcome_counts': {
            outcome: int(count) for outcome, count in count_rows
        },
        'events': events,
        'next_cursor': next_cursor,
    }
