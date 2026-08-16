"""PostgreSQL-backed aggregate history for SkyServe status and requests."""

from collections.abc import Collection
import datetime
from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky import sky_logging
from sky.serve import constants
from sky.serve import serve_state
from sky.utils import common_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

DEFAULT_HISTORY_HOURS = 12
RETENTION_HOURS = 72
BUCKET_SECONDS = 60
REQUEST_CLASSIFICATION_PROTOCOL_VERSION = 1
ACCELERATOR_BREAKDOWN_CAPACITY_SEMANTICS_VERSION = 2
STATUS_HISTORY_SECTIONS = frozenset(
    {'requests', 'replicas', 'prediction', 'autoscaler'})

metadata = sqlalchemy.MetaData()

serve_replica_status_history_table = sqlalchemy.Table(
    'serve_replica_status_history',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      primary_key=True),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('ready_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('ready_reserved_count', sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('provisioning_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('not_ready_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('errored_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('preempted_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('stopping_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('total_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('logical_ready_count', sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column('logical_ready_reserved_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('logical_provisioning_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('logical_not_ready_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('logical_errored_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('logical_preempted_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('logical_stopping_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('logical_total_count', sqlalchemy.Integer, nullable=True),
    sqlalchemy.CheckConstraint(
        'ready_count >= 0 AND provisioning_count >= 0 AND '
        'not_ready_count >= 0 AND errored_count >= 0 AND '
        'preempted_count >= 0 AND stopping_count >= 0 AND total_count >= 0',
        name='serve_replica_status_history_nonnegative'),
    sqlalchemy.CheckConstraint(
        'total_count = ready_count + provisioning_count + '
        'not_ready_count + errored_count + preempted_count + stopping_count',
        name='serve_replica_status_history_total'),
    sqlalchemy.CheckConstraint(
        'ready_reserved_count IS NULL OR '
        '(ready_reserved_count >= 0 AND '
        'ready_reserved_count <= ready_count)',
        name='serve_replica_status_history_reserved_ready'),
    sqlalchemy.CheckConstraint(
        '(logical_ready_count IS NULL AND '
        'logical_ready_reserved_count IS NULL AND '
        'logical_provisioning_count IS NULL AND '
        'logical_not_ready_count IS NULL AND '
        'logical_errored_count IS NULL AND '
        'logical_preempted_count IS NULL AND '
        'logical_stopping_count IS NULL AND '
        'logical_total_count IS NULL) OR '
        '(logical_ready_count >= 0 AND '
        'logical_ready_reserved_count >= 0 AND '
        'logical_ready_reserved_count <= logical_ready_count AND '
        'logical_provisioning_count >= 0 AND '
        'logical_not_ready_count >= 0 AND '
        'logical_errored_count >= 0 AND '
        'logical_preempted_count >= 0 AND '
        'logical_stopping_count >= 0 AND '
        'logical_total_count = logical_ready_count + '
        'logical_provisioning_count + logical_not_ready_count + '
        'logical_errored_count + logical_preempted_count + '
        'logical_stopping_count)',
        name='serve_replica_status_history_logical_counts'),
)
sqlalchemy.Index('serve_replica_status_history_lookup_idx',
                 serve_replica_status_history_table.c.service_name,
                 serve_replica_status_history_table.c.service_hash,
                 serve_replica_status_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_replica_status_history_bucket_idx',
                 serve_replica_status_history_table.c.bucket_start)

serve_request_activity_history_table = sqlalchemy.Table(
    'serve_request_activity_history',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('reporter_session_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      primary_key=True),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('request_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('rejected_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default=sqlalchemy.text('0')),
    sqlalchemy.Column('rejection_count_available',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('classified_request_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.Column('counted_rejected_count',
                      sqlalchemy.Integer,
                      nullable=True),
    sqlalchemy.CheckConstraint(
        'request_count >= 0',
        name='serve_request_activity_history_nonnegative'),
    sqlalchemy.CheckConstraint(
        'rejected_count >= 0',
        name='serve_request_activity_history_rejected_nonnegative'),
    sqlalchemy.CheckConstraint(
        '(classified_request_count IS NULL AND '
        'counted_rejected_count IS NULL) OR '
        '(classified_request_count IS NOT NULL AND '
        'counted_rejected_count IS NOT NULL AND '
        'classified_request_count >= 0 AND counted_rejected_count >= 0 AND '
        'counted_rejected_count <= classified_request_count)',
        name='serve_request_activity_history_classified_pair'),
)
sqlalchemy.Index('serve_request_activity_history_lookup_idx',
                 serve_request_activity_history_table.c.service_name,
                 serve_request_activity_history_table.c.service_hash,
                 serve_request_activity_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_request_activity_history_bucket_idx',
                 serve_request_activity_history_table.c.bucket_start)

serve_request_activity_daily_table = sqlalchemy.Table(
    'serve_request_activity_daily',
    metadata,
    sqlalchemy.Column('day_start',
                      sqlalchemy.DateTime(timezone=True),
                      primary_key=True),
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('first_bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('last_bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('request_count', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('classified_request_count',
                      sqlalchemy.BigInteger,
                      nullable=True),
    sqlalchemy.Column('counted_rejected_count',
                      sqlalchemy.BigInteger,
                      nullable=True),
    sqlalchemy.Column('classified_first_bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=True),
    sqlalchemy.Column('classified_last_bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=True),
    sqlalchemy.Column('classification_incomplete',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.CheckConstraint('request_count >= 0',
                               name='serve_request_activity_daily_nonnegative'),
    sqlalchemy.CheckConstraint(
        '(classified_request_count IS NULL AND '
        'counted_rejected_count IS NULL AND '
        'classified_first_bucket_start IS NULL AND '
        'classified_last_bucket_start IS NULL) OR '
        '(classified_request_count IS NOT NULL AND '
        'counted_rejected_count IS NOT NULL AND '
        'classified_request_count >= 0 AND counted_rejected_count >= 0 AND '
        'counted_rejected_count <= classified_request_count AND '
        'classified_first_bucket_start IS NOT NULL AND '
        'classified_last_bucket_start IS NOT NULL AND '
        'classified_first_bucket_start <= classified_last_bucket_start)',
        name='serve_request_activity_daily_classified_pair'),
)
sqlalchemy.Index('serve_request_activity_daily_day_idx',
                 serve_request_activity_daily_table.c.day_start)
sqlalchemy.Index('serve_request_activity_daily_service_day_idx',
                 serve_request_activity_daily_table.c.service_name,
                 serve_request_activity_daily_table.c.day_start)

_RESPONSE_TIME_ARRAY_COLUMNS = tuple(
    sqlalchemy.Column(f'status_{status_class}_counts',
                      postgresql.ARRAY(sqlalchemy.Integer),
                      nullable=False)
    for status_class in constants.LB_RESPONSE_TIME_STATUS_CLASSES)
_RESPONSE_TIME_ARRAY_CONSTRAINTS = tuple(
    constraint for status_class in constants.LB_RESPONSE_TIME_STATUS_CLASSES
    for constraint in (
        sqlalchemy.CheckConstraint(
            f'cardinality(status_{status_class}_counts) = '
            f'{constants.LB_RESPONSE_TIME_BUCKET_COUNT}',
            name=f'serve_response_time_history_{status_class}_length'),
        sqlalchemy.CheckConstraint(
            f'0 <= ALL(status_{status_class}_counts)',
            name=f'serve_response_time_history_{status_class}_nonnegative'),
    ))

serve_response_time_history_table = sqlalchemy.Table(
    'serve_response_time_history',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('reporter_session_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      primary_key=True),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('response_count', sqlalchemy.Integer, nullable=False),
    *_RESPONSE_TIME_ARRAY_COLUMNS,
    sqlalchemy.CheckConstraint(
        'response_count >= 0',
        name='serve_response_time_history_response_count_nonnegative'),
    *_RESPONSE_TIME_ARRAY_CONSTRAINTS,
)
sqlalchemy.Index('serve_response_time_history_lookup_idx',
                 serve_response_time_history_table.c.service_name,
                 serve_response_time_history_table.c.service_hash,
                 serve_response_time_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_response_time_history_bucket_idx',
                 serve_response_time_history_table.c.bucket_start)

_PREDICTION_TIME_ARRAY_COLUMNS = tuple(
    sqlalchemy.Column(f'{outcome}_counts',
                      postgresql.ARRAY(sqlalchemy.Integer),
                      nullable=False)
    for outcome in constants.LB_PREDICTION_TIME_OUTCOMES)
_PREDICTION_TIME_ARRAY_CONSTRAINTS = tuple(
    constraint for outcome in constants.LB_PREDICTION_TIME_OUTCOMES
    for constraint in (
        sqlalchemy.CheckConstraint(
            f'cardinality({outcome}_counts) = '
            f'{constants.LB_PREDICTION_TIME_BUCKET_COUNT}',
            name=f'serve_prediction_time_history_{outcome}_length'),
        sqlalchemy.CheckConstraint(
            f'0 <= ALL({outcome}_counts)',
            name=f'serve_prediction_time_history_{outcome}_nonnegative'),
    ))

serve_prediction_time_history_table = sqlalchemy.Table(
    'serve_prediction_time_history',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('reporter_session_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      primary_key=True),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('prediction_count', sqlalchemy.Integer, nullable=False),
    *_PREDICTION_TIME_ARRAY_COLUMNS,
    sqlalchemy.CheckConstraint(
        'prediction_count >= 0',
        name='serve_prediction_time_history_prediction_count_nonnegative'),
    *_PREDICTION_TIME_ARRAY_CONSTRAINTS,
)
sqlalchemy.Index('serve_prediction_time_history_lookup_idx',
                 serve_prediction_time_history_table.c.service_name,
                 serve_prediction_time_history_table.c.service_hash,
                 serve_prediction_time_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_prediction_time_history_bucket_idx',
                 serve_prediction_time_history_table.c.bucket_start)

serve_autoscaler_history_table = sqlalchemy.Table(
    'serve_autoscaler_history',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('bucket_start',
                      sqlalchemy.DateTime(timezone=True),
                      primary_key=True),
    sqlalchemy.Column('observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('controller_session_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_unit', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('demand_target', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('capacity_target', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('ready_capacity', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('provisioning_capacity',
                      sqlalchemy.Integer,
                      nullable=False),
    sqlalchemy.Column('total_capacity', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('peak_in_flight', sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column('peak_queue_depth', sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column('accelerator_breakdown',
                      postgresql.JSONB,
                      nullable=False,
                      server_default=sqlalchemy.text("'{}'::jsonb")),
    sqlalchemy.Column('accelerator_breakdown_observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=True),
    sqlalchemy.CheckConstraint(
        'version >= 1 AND demand_target >= 0 AND capacity_target >= 0 AND '
        'ready_capacity >= 0 AND provisioning_capacity >= 0 AND '
        'total_capacity >= 0 AND (peak_in_flight IS NULL OR '
        'peak_in_flight >= 0) AND (peak_queue_depth IS NULL OR '
        'peak_queue_depth >= 0)',
        name='serve_autoscaler_history_nonnegative'),
    sqlalchemy.CheckConstraint('capacity_target >= demand_target',
                               name='serve_autoscaler_history_capacity_target'),
)
sqlalchemy.Index('serve_autoscaler_history_lookup_idx',
                 serve_autoscaler_history_table.c.service_name,
                 serve_autoscaler_history_table.c.service_hash,
                 serve_autoscaler_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_autoscaler_history_bucket_idx',
                 serve_autoscaler_history_table.c.bucket_start)

_COUNT_COLUMNS = (
    'ready_count',
    'provisioning_count',
    'not_ready_count',
    'errored_count',
    'preempted_count',
    'stopping_count',
)
_LOGICAL_COUNT_COLUMNS = tuple(f'logical_{column}' for column in _COUNT_COLUMNS)
_OPTIONAL_STATUS_COLUMNS = (
    'ready_reserved_count',
    *_LOGICAL_COUNT_COLUMNS,
    'logical_ready_reserved_count',
    'logical_total_count',
)


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


def _status_bucket(status: str | None) -> str:
    if status == serve_state.ReplicaStatus.READY.value:
        return 'ready_count'
    if status in {
            serve_state.ReplicaStatus.PENDING.value,
            serve_state.ReplicaStatus.PROVISIONING.value,
            serve_state.ReplicaStatus.STARTING.value,
    }:
        return 'provisioning_count'
    if status == serve_state.ReplicaStatus.NOT_READY.value:
        return 'not_ready_count'
    if status == serve_state.ReplicaStatus.PREEMPTED.value:
        return 'preempted_count'
    if status == serve_state.ReplicaStatus.SHUTTING_DOWN.value:
        return 'stopping_count'
    # This covers ReplicaStatus.failed_statuses(), UNKNOWN, and legacy/null
    # values. Unknown physical rows must not disappear from the total.
    return 'errored_count'


def _snapshot_query() -> sqlalchemy.Select:
    """One compact physical and logical snapshot of every non-pool service."""
    services = serve_state.services_table
    replicas = serve_state.replicas_table
    version = sqlalchemy.func.coalesce(replicas.c.version,
                                       services.c.current_version,
                                       constants.INITIAL_VERSION)
    # Latest-version failures are retained for diagnostics after cleanup.
    # Once teardown succeeds, the row no longer represents a physical machine.
    live_replica = sqlalchemy.and_(
        replicas.c.service_name == services.c.name,
        replicas.c.sky_down_status.is_distinct_from(
            common_utils.ProcessStatus.SUCCEEDED.value))
    raw_planned_capacity = replicas.c.replica_state[
        'planned_capacity'].as_integer()
    planned_capacity = sqlalchemy.case(
        (raw_planned_capacity > 0, raw_planned_capacity), else_=1)
    has_replica = replicas.c.replica_id.is_not(None)
    reserved_fill = replicas.c.replica_state['reserved_fill'].as_boolean().is_(
        True)
    return (sqlalchemy.select(
        services.c.name,
        services.c.hash,
        version.label('version'),
        replicas.c.status,
        sqlalchemy.func.count(  # pylint: disable=not-callable
            replicas.c.replica_id).label('count'),
        sqlalchemy.func.coalesce(  # pylint: disable=not-callable
            sqlalchemy.func.sum(  # pylint: disable=not-callable
                sqlalchemy.case((has_replica, planned_capacity), else_=0)),
            0).label('logical_count'),
        sqlalchemy.func.coalesce(  # pylint: disable=not-callable
            sqlalchemy.func.sum(  # pylint: disable=not-callable
                sqlalchemy.case((reserved_fill, 1), else_=0)),
            0).label('reserved_count'),
        sqlalchemy.func.coalesce(  # pylint: disable=not-callable
            sqlalchemy.func.sum(  # pylint: disable=not-callable
                sqlalchemy.case((reserved_fill, planned_capacity), else_=0)),
            0).label('logical_reserved_count'),
    ).select_from(services.outerjoin(replicas, live_replica)).where(
        services.c.pool == 0,
        services.c.hash.is_not(None)).group_by(services.c.name, services.c.hash,
                                               version, replicas.c.status))


def _build_history_rows(
        rows: list[Any], observed_at: datetime.datetime,
        bucket_start: datetime.datetime) -> list[dict[str, Any]]:
    """Collapse status groups into one exhaustive row per service/version."""
    grouped: dict[tuple[str, str, int], dict[str, Any]] = {}
    for (service_name, service_hash, version, status, count, logical_count,
         reserved_count, logical_reserved_count) in rows:
        key = (service_name, service_hash, int(version))
        record = grouped.get(key)
        if record is None:
            record = {
                'service_name': service_name,
                'service_hash': service_hash,
                'version': int(version),
                'bucket_start': bucket_start,
                'observed_at': observed_at,
                **{
                    column: 0 for column in _COUNT_COLUMNS
                },
                'ready_reserved_count': 0,
                'total_count': 0,
                **{
                    column: 0 for column in _LOGICAL_COUNT_COLUMNS
                },
                'logical_ready_reserved_count': 0,
                'logical_total_count': 0,
            }
            grouped[key] = record
        count = int(count)
        if count:
            bucket = _status_bucket(status)
            record[bucket] += count
            record['total_count'] += count
            logical_bucket = f'logical_{bucket}'
            record[logical_bucket] += int(logical_count)
            record['logical_total_count'] += int(logical_count)
            if bucket == 'ready_count':
                record['ready_reserved_count'] += int(reserved_count)
                record['logical_ready_reserved_count'] += int(
                    logical_reserved_count)
    return list(grouped.values())


def record_status_snapshot(timestamp: float | None = None) -> int:
    """Persist one minute bucket from the latest normalized replica rows.

    Returns the number of service/version rows written. Non-PostgreSQL
    deployments return zero because central history is unsupported there.
    """
    engine = _postgres_engine()
    if engine is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    bucket_start = observed_at.replace(second=0, microsecond=0)
    with engine.begin() as connection:
        snapshot_rows = connection.execute(_snapshot_query()).fetchall()
        history_rows = _build_history_rows(snapshot_rows, observed_at,
                                           bucket_start)
        if history_rows:
            insert = postgresql.insert(
                serve_replica_status_history_table).values(history_rows)
            excluded = insert.excluded
            update_values = {
                'observed_at': excluded.observed_at,
                **{
                    column: getattr(excluded, column) for column in _COUNT_COLUMNS
                },
                **{
                    column: getattr(excluded, column) for column in _OPTIONAL_STATUS_COLUMNS
                },
                'total_count': excluded.total_count,
            }
            connection.execute(
                insert.on_conflict_do_update(
                    index_elements=[
                        serve_replica_status_history_table.c.service_name,
                        serve_replica_status_history_table.c.service_hash,
                        serve_replica_status_history_table.c.version,
                        serve_replica_status_history_table.c.bucket_start,
                    ],
                    set_=update_values,
                    where=(
                        excluded.observed_at
                        >= serve_replica_status_history_table.c.observed_at)))
        if bucket_start.minute == 0:
            cutoff = observed_at - datetime.timedelta(hours=RETENTION_HOURS)
            connection.execute(
                sqlalchemy.delete(serve_replica_status_history_table).where(
                    serve_replica_status_history_table.c.bucket_start < cutoff))
            connection.execute(
                sqlalchemy.delete(serve_request_activity_history_table).where(
                    serve_request_activity_history_table.c.bucket_start <
                    cutoff))
            connection.execute(
                sqlalchemy.delete(serve_response_time_history_table).where(
                    serve_response_time_history_table.c.bucket_start < cutoff))
            connection.execute(
                sqlalchemy.delete(serve_prediction_time_history_table).where(
                    serve_prediction_time_history_table.c.bucket_start <
                    cutoff))
            connection.execute(
                sqlalchemy.delete(serve_autoscaler_history_table).where(
                    serve_autoscaler_history_table.c.bucket_start < cutoff))
    return len(history_rows)


def rollup_request_activity_daily(timestamp: float | None = None) -> int:
    """Monotonically materialize available minute counters into UTC days.

    The caller intentionally runs this before hourly raw-history pruning and
    isolates failures from status snapshots. Recomputing all retained raw rows
    provides a bounded initial backfill and incorporates late counter updates.
    """
    engine = _postgres_engine()
    if engine is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    request_history = serve_request_activity_history_table
    daily = serve_request_activity_daily_table
    utc_bucket_start = sqlalchemy.func.timezone('UTC',
                                                request_history.c.bucket_start)
    day_start = sqlalchemy.func.timezone(
        'UTC', sqlalchemy.func.date_trunc('day',
                                          utc_bucket_start)).label('day_start')
    classification_supported = sqlalchemy.and_(
        request_history.c.classified_request_count.is_not(None),
        request_history.c.counted_rejected_count.is_not(None))
    classified_request_count = sqlalchemy.func.sum(
        sqlalchemy.cast(
            request_history.c.classified_request_count,
            sqlalchemy.BigInteger)).filter(classification_supported).label(
                'classified_request_count')
    counted_rejected_count = sqlalchemy.func.sum(
        sqlalchemy.cast(
            request_history.c.counted_rejected_count,
            sqlalchemy.BigInteger)).filter(classification_supported).label(
                'counted_rejected_count')
    classified_first_bucket_start = sqlalchemy.func.min(
        request_history.c.bucket_start).filter(classification_supported).label(
            'classified_first_bucket_start')
    classified_last_bucket_start = sqlalchemy.func.max(
        request_history.c.bucket_start).filter(classification_supported).label(
            'classified_last_bucket_start')
    classification_incomplete = sqlalchemy.func.bool_or(
        sqlalchemy.and_(request_history.c.request_count > 0,
                        sqlalchemy.not_(classification_supported))).label(
                            'classification_incomplete')
    query = (sqlalchemy.select(
        day_start,
        request_history.c.service_name,
        request_history.c.service_hash,
        sqlalchemy.func.min(
            request_history.c.bucket_start).label('first_bucket_start'),
        sqlalchemy.func.max(
            request_history.c.bucket_start).label('last_bucket_start'),
        sqlalchemy.func.sum(
            sqlalchemy.cast(request_history.c.request_count,
                            sqlalchemy.BigInteger)).label('request_count'),
        classified_request_count,
        counted_rejected_count,
        classified_first_bucket_start,
        classified_last_bucket_start,
        classification_incomplete,
    ).group_by(day_start, request_history.c.service_name,
               request_history.c.service_hash))

    with engine.begin() as connection:
        has_daily_rows = connection.execute(
            sqlalchemy.select(sqlalchemy.literal(1)).select_from(daily).limit(
                1)).first() is not None
        # The first run backfills all retained raw data. Hourly full passes run
        # before raw pruning. Between them, only the current day needs
        # recomputation, except during UTC midnight's one-hour late-report
        # window when the complete previous day is still mutable.
        if has_daily_rows and observed_at.minute != 0:
            current_day = observed_at.replace(hour=0,
                                              minute=0,
                                              second=0,
                                              microsecond=0)
            cutoff = (current_day - datetime.timedelta(days=1)
                      if observed_at.hour == 0 else current_day)
            query = query.where(request_history.c.bucket_start >= cutoff)
        rows = connection.execute(query).mappings().all()
        if not rows:
            return 0
        values = [{
            **dict(row),
            'request_count': int(row['request_count']),
            'classified_request_count':
                (int(row['classified_request_count'])
                 if row['classified_request_count'] is not None else None),
            'counted_rejected_count':
                (int(row['counted_rejected_count'])
                 if row['counted_rejected_count'] is not None else None),
            'classification_incomplete': bool(row['classification_incomplete']),
            'observed_at': observed_at,
        } for row in rows]
        insert = postgresql.insert(serve_request_activity_daily_table).values(
            values)
        excluded = insert.excluded
        connection.execute(
            insert.on_conflict_do_update(
                index_elements=[
                    daily.c.day_start,
                    daily.c.service_name,
                    daily.c.service_hash,
                ],
                set_={
                    'first_bucket_start': sqlalchemy.func.least(
                        daily.c.first_bucket_start,
                        excluded.first_bucket_start),
                    'last_bucket_start': sqlalchemy.func.greatest(
                        daily.c.last_bucket_start, excluded.last_bucket_start),
                    'request_count': sqlalchemy.func.greatest(
                        daily.c.request_count, excluded.request_count),
                    'classified_request_count': _greatest_nullable(
                        daily.c.classified_request_count,
                        excluded.classified_request_count),
                    'counted_rejected_count': _greatest_nullable(
                        daily.c.counted_rejected_count,
                        excluded.counted_rejected_count),
                    'classified_first_bucket_start': _least_nullable(
                        daily.c.classified_first_bucket_start,
                        excluded.classified_first_bucket_start),
                    'classified_last_bucket_start': _greatest_nullable(
                        daily.c.classified_last_bucket_start,
                        excluded.classified_last_bucket_start),
                    'classification_incomplete': sqlalchemy.or_(
                        daily.c.classification_incomplete,
                        excluded.classification_incomplete),
                    'observed_at': sqlalchemy.func.greatest(
                        daily.c.observed_at, excluded.observed_at),
                }))
    return len(values)


def get_daily_request_summary(
    engine: sqlalchemy.engine.Engine,
    first_day_start: int,
    last_day_start: int,
    days: list[dict[str, Any]],
    table_limit: int,
    chart_limit: int,
) -> dict[str, Any]:
    """Return bounded daily service request aggregates for a UTC range."""
    non_rejected_empty = {
        'available': False,
        'definition': 'non_rejected_inbound_requests',
        'coverage_start_utc': None,
        'coverage': 'unavailable',
        'complete_by_day': [False for _ in days],
        'total_request_count': 0,
        'services': [],
        'series': [],
    }
    empty = {
        'available': False,
        'definition': 'admitted_inbound_requests',
        'coverage_start_utc': None,
        'total_request_count': 0,
        'services': [],
        'series': [],
        'non_rejected': non_rejected_empty,
    }
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return empty

    daily = serve_request_activity_daily_table
    first_day = datetime.datetime.fromtimestamp(first_day_start,
                                                datetime.timezone.utc)
    end_exclusive = datetime.datetime.fromtimestamp(
        last_day_start + 24 * 60 * 60, datetime.timezone.utc)
    base_filter = sqlalchemy.and_(daily.c.day_start >= first_day,
                                  daily.c.day_start < end_exclusive)
    count_sum = sqlalchemy.func.sum(daily.c.request_count)
    classification_supported = sqlalchemy.and_(
        daily.c.classified_request_count.is_not(None),
        daily.c.counted_rejected_count.is_not(None))
    classified_sum = sqlalchemy.func.sum(
        daily.c.classified_request_count).filter(classification_supported)
    counted_rejected_sum = sqlalchemy.func.sum(
        daily.c.counted_rejected_count).filter(classification_supported)
    service_day_incomplete = sqlalchemy.func.bool_or(
        sqlalchemy.or_(
            daily.c.classification_incomplete,
            sqlalchemy.and_(daily.c.request_count > 0,
                            sqlalchemy.not_(classification_supported))))
    try:
        with engine.connect() as connection:
            earliest_day = sqlalchemy.select(
                sqlalchemy.func.min(daily.c.day_start)).scalar_subquery()
            coverage_start = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.min(daily.c.first_bucket_start)).
                where(daily.c.day_start == earliest_day)).scalar_one_or_none()
            service_rows = connection.execute(
                sqlalchemy.select(
                    daily.c.service_name,
                    count_sum.label('request_count'),
                ).where(base_filter).group_by(daily.c.service_name).order_by(
                    sqlalchemy.desc('request_count'),
                    daily.c.service_name.asc()).limit(table_limit)).fetchall()
            top_service_names = [
                row.service_name for row in service_rows[:chart_limit]
            ]
            total_rows = connection.execute(
                sqlalchemy.select(
                    daily.c.day_start,
                    count_sum.label('request_count'),
                ).where(base_filter).group_by(daily.c.day_start)).fetchall()
            daily_service_rows = []
            if top_service_names:
                daily_service_rows = connection.execute(
                    sqlalchemy.select(
                        daily.c.day_start,
                        daily.c.service_name,
                        count_sum.label('request_count'),
                    ).where(
                        sqlalchemy.and_(
                            base_filter,
                            daily.c.service_name.in_(
                                top_service_names))).group_by(
                                    daily.c.day_start,
                                    daily.c.service_name)).fetchall()
            classified_earliest_day = sqlalchemy.select(
                sqlalchemy.func.min(daily.c.day_start)).where(
                    daily.c.classified_first_bucket_start.is_not(
                        None)).scalar_subquery()
            classified_coverage_start = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.min(
                        daily.c.classified_first_bucket_start)).where(
                            daily.c.day_start ==
                            classified_earliest_day)).scalar_one_or_none()
            service_day_rows = connection.execute(
                sqlalchemy.select(
                    daily.c.day_start,
                    daily.c.service_name,
                    count_sum.label('request_count'),
                    classified_sum.label('classified_request_count'),
                    counted_rejected_sum.label('counted_rejected_count'),
                    sqlalchemy.func.min(
                        daily.c.classified_first_bucket_start).filter(
                            classification_supported).label(
                                'classified_first_bucket_start'),
                    service_day_incomplete.label('classification_incomplete'),
                ).where(base_filter).group_by(daily.c.day_start,
                                              daily.c.service_name)).fetchall()
            observed_service_names = sorted(
                {str(row.service_name) for row in service_day_rows})
            service_coverage_rows = []
            if observed_service_names:
                service_coverage_rows = connection.execute(
                    sqlalchemy.select(
                        daily.c.service_name,
                        sqlalchemy.func.min(
                            daily.c.classified_first_bucket_start).label(
                                'coverage_start'),
                    ).where(
                        sqlalchemy.and_(
                            daily.c.service_name.in_(observed_service_names),
                            daily.c.classified_first_bucket_start.is_not(None),
                        )).group_by(daily.c.service_name)).fetchall()
    except sqlalchemy.exc.SQLAlchemyError:
        # During a rolling API-server upgrade the reader may briefly run
        # before the Serve migration has created the optional table.
        logger.exception('Failed to read daily Serve request history.')
        return empty

    day_starts = [int(day['day_start_utc']) for day in days]
    totals_by_day = {
        int(row.day_start.timestamp()): int(row.request_count or 0)
        for row in total_rows
    }
    counts_by_service_day = {
        (row.service_name, int(row.day_start.timestamp())): int(
            row.request_count or 0) for row in daily_service_rows
    }
    displayed_by_day = dict.fromkeys(day_starts, 0)
    series = []
    for service_name in top_service_names:
        request_count_by_day = []
        for day_start_epoch in day_starts:
            count = counts_by_service_day.get((service_name, day_start_epoch),
                                              0)
            request_count_by_day.append(count)
            displayed_by_day[day_start_epoch] += count
        series.append({
            'service_name': service_name,
            'request_count_by_day': request_count_by_day,
        })
    other_by_day = [
        max(
            0,
            totals_by_day.get(day_start_epoch, 0) -
            displayed_by_day[day_start_epoch]) for day_start_epoch in day_starts
    ]
    if any(other_by_day):
        series.append({
            'is_other': True,
            'request_count_by_day': other_by_day,
        })

    classified_coverage_start_utc = (int(classified_coverage_start.timestamp())
                                     if classified_coverage_start is not None
                                     else None)
    service_coverage_starts = {
        str(row.service_name): int(row.coverage_start.timestamp())
        for row in service_coverage_rows
        if row.coverage_start is not None
    }
    observed_by_service_day = {
        (str(row.service_name), int(row.day_start.timestamp())): row
        for row in service_day_rows
    }

    seconds_per_day = 24 * 60 * 60

    def first_complete_day(coverage_start_utc: int | None) -> int | None:
        if coverage_start_utc is None:
            return None
        coverage_day = coverage_start_utc // seconds_per_day * seconds_per_day
        if coverage_start_utc > coverage_day:
            coverage_day += seconds_per_day
        return coverage_day

    complete_by_service: dict[str, list[bool]] = {}
    counts_by_service: dict[str, list[int | None]] = {}
    observed_days_by_service: dict[str, set[int]] = {}
    for service_name in observed_service_names:
        coverage_day = first_complete_day(
            service_coverage_starts.get(service_name))
        complete_cells = []
        count_cells: list[int | None] = []
        observed_days = set()
        for day_start_epoch in day_starts:
            row = observed_by_service_day.get((service_name, day_start_epoch))
            if row is not None:
                observed_days.add(day_start_epoch)
            complete = coverage_day is not None and day_start_epoch >= coverage_day
            if row is not None:
                pair_available = (row.classified_request_count is not None and
                                  row.counted_rejected_count is not None)
                pair_required = int(row.request_count or 0) > 0
                complete = (complete and
                            not bool(row.classification_incomplete) and
                            (not pair_required or pair_available))
            complete_cells.append(complete)
            if not complete:
                count_cells.append(None)
            elif row is None:
                count_cells.append(0)
            elif row.classified_request_count is None:
                count_cells.append(0)
            else:
                count_cells.append(
                    max(
                        0,
                        int(row.classified_request_count) -
                        int(row.counted_rejected_count)))
        complete_by_service[service_name] = complete_cells
        counts_by_service[service_name] = count_cells
        observed_days_by_service[service_name] = observed_days

    service_records: list[dict[str, Any]] = []
    for service_name in observed_service_names:
        complete_cells = complete_by_service[service_name]
        count_cells = counts_by_service[service_name]
        complete_count = sum(1 for complete in complete_cells if complete)
        if complete_count == 0:
            service_coverage = 'unavailable'
        elif complete_count == len(complete_cells):
            service_coverage = 'complete'
        else:
            service_coverage = 'partial'
        service_records.append({
            'service_name': service_name,
            'request_count': sum(count or 0 for count in count_cells),
            'coverage': service_coverage,
            'complete_by_day': complete_cells,
        })
    service_records.sort(key=lambda service: (-int(service['request_count']),
                                              str(service['service_name'])))

    global_first_complete_day = first_complete_day(
        classified_coverage_start_utc)
    classified_complete_by_day = []
    for index, day_start_epoch in enumerate(day_starts):
        complete = (global_first_complete_day is not None and
                    day_start_epoch >= global_first_complete_day)
        if complete:
            complete = all(
                complete_by_service[service_name][index]
                for service_name in observed_service_names
                if day_start_epoch in observed_days_by_service[service_name])
        classified_complete_by_day.append(complete)

    has_complete_cell = (any(classified_complete_by_day) or any(
        any(complete_cells) for complete_cells in complete_by_service.values()))
    if not has_complete_cell:
        classified_coverage = 'unavailable'
    elif all(classified_complete_by_day):
        classified_coverage = 'complete'
    else:
        classified_coverage = 'partial'

    classified_top_services = service_records[:table_limit]
    classified_top_names = [
        str(service['service_name'])
        for service in service_records[:chart_limit]
    ]
    classified_series: list[dict[str, Any]] = [{
        'service_name': service_name,
        'request_count_by_day': counts_by_service[service_name],
    } for service_name in classified_top_names]
    remainder_names = [
        service_name for service_name in observed_service_names
        if service_name not in set(classified_top_names)
    ]
    other_counts: list[int | None] = []
    for index, day_start_epoch in enumerate(day_starts):
        incomplete_remainder = any(
            day_start_epoch in observed_days_by_service[service_name] and
            not complete_by_service[service_name][index]
            for service_name in remainder_names)
        if incomplete_remainder:
            other_counts.append(None)
        else:
            other_counts.append(
                sum((counts_by_service[service_name][index] or 0)
                    for service_name in remainder_names))
    if any(count is None or count > 0 for count in other_counts):
        classified_series.append({
            'is_other': True,
            'request_count_by_day': other_counts,
        })

    non_rejected = {
        'available': has_complete_cell,
        'definition': 'non_rejected_inbound_requests',
        'coverage_start_utc': classified_coverage_start_utc,
        'coverage': classified_coverage,
        'complete_by_day': classified_complete_by_day,
        'total_request_count': sum(
            int(service['request_count']) for service in service_records),
        'services': classified_top_services,
        'series': classified_series,
    }

    return {
        'available': True,
        'definition': 'admitted_inbound_requests',
        'coverage_start_utc': int(coverage_start.timestamp())
                              if coverage_start else None,
        'total_request_count': sum(totals_by_day.values()),
        'services': [{
            'service_name': row.service_name,
            'request_count': int(row.request_count or 0),
        } for row in service_rows],
        'series': series,
        'non_rejected': non_rejected,
    }


def _request_history_rows(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    request_history: dict[str, Any],
    observed_at: datetime.datetime,
) -> list[dict[str, Any]]:
    """Validate one LB report and convert it to minute-bucket DB rows."""
    if not isinstance(request_history, dict):
        raise ValueError('request_history must be an object.')
    if request_history.get('bucket_seconds') != BUCKET_SECONDS:
        raise ValueError(f'request_history bucket_seconds must be '
                         f'{BUCKET_SECONDS}.')
    buckets = request_history.get('buckets')
    if not isinstance(buckets, list):
        raise ValueError('request_history buckets must be a list.')
    max_buckets = constants.LB_REQUEST_HISTORY_MAX_BUCKETS
    if len(buckets) > max_buckets:
        raise ValueError(f'request_history may contain at most '
                         f'{max_buckets} buckets.')

    current_bucket = observed_at.replace(second=0, microsecond=0)
    oldest_bucket = current_bucket - datetime.timedelta(
        seconds=constants.LB_REQUEST_HISTORY_WINDOW_SECONDS +
        2 * BUCKET_SECONDS)
    newest_bucket = current_bucket + datetime.timedelta(seconds=2 *
                                                        BUCKET_SECONDS)
    seen_bucket_starts: set[int] = set()
    rows = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise ValueError('request_history bucket must be an object.')
        bucket_start = bucket.get('bucket_start')
        request_count = bucket.get('request_count')
        rejection_count_available = 'rejected_count' in bucket
        rejected_count = bucket.get('rejected_count', 0)
        if (not isinstance(bucket_start, int) or
                isinstance(bucket_start, bool) or
                bucket_start % BUCKET_SECONDS != 0):
            raise ValueError('request_history bucket_start must be an aligned '
                             'integer epoch timestamp.')
        if bucket_start in seen_bucket_starts:
            raise ValueError('request_history bucket_start must be unique.')
        seen_bucket_starts.add(bucket_start)
        if (not isinstance(request_count, int) or
                isinstance(request_count, bool) or request_count < 0):
            raise ValueError('request_history request_count must be a '
                             'nonnegative integer.')
        if (not isinstance(rejected_count, int) or
                isinstance(rejected_count, bool) or rejected_count < 0):
            raise ValueError('request_history rejected_count must be a '
                             'nonnegative integer.')
        if request_count == 0 and rejected_count == 0:
            raise ValueError('request_history bucket must contain a request '
                             'or rejection count.')
        bucket_datetime = _utc_datetime(bucket_start)
        if not oldest_bucket <= bucket_datetime <= newest_bucket:
            raise ValueError('request_history bucket_start is outside the '
                             'accepted recent window.')
        rows.append({
            'service_name': service_name,
            'service_hash': service_hash,
            'reporter_session_id': reporter_session_id,
            'bucket_start': bucket_datetime,
            'observed_at': observed_at,
            'request_count': request_count,
            'rejected_count': rejected_count,
            'rejection_count_available': rejection_count_available,
        })
    return rows


def record_request_activity(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    request_history: dict[str, Any] | None,
    timestamp: float | None = None,
) -> int:
    """Persist cumulative minute arrival counters from one LB process.

    Exact counters are idempotent across retries. A higher count for the same
    service incarnation, reporter process, and minute replaces a lower one;
    stale or out-of-order reports can therefore never decrement history.
    Non-PostgreSQL deployments accept and drop history by returning zero.
    """
    if request_history is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    rows = _request_history_rows(service_name, service_hash,
                                 reporter_session_id, request_history,
                                 observed_at)
    engine = _postgres_engine()
    if engine is None or not rows:
        return 0
    with engine.begin() as connection:
        insert = postgresql.insert(serve_request_activity_history_table).values(
            rows)
        excluded = insert.excluded
        rejection_available = (
            serve_request_activity_history_table.c.rejection_count_available)
        connection.execute(
            insert.on_conflict_do_update(
                index_elements=[
                    serve_request_activity_history_table.c.service_name,
                    serve_request_activity_history_table.c.service_hash,
                    serve_request_activity_history_table.c.reporter_session_id,
                    serve_request_activity_history_table.c.bucket_start,
                ],
                set_={
                    'observed_at': sqlalchemy.func.greatest(
                        serve_request_activity_history_table.c.observed_at,
                        excluded.observed_at),
                    'request_count': sqlalchemy.func.greatest(
                        serve_request_activity_history_table.c.request_count,
                        excluded.request_count),
                    'rejected_count': sqlalchemy.func.greatest(
                        serve_request_activity_history_table.c.rejected_count,
                        excluded.rejected_count),
                    'rejection_count_available': sqlalchemy.or_(
                        rejection_available,
                        excluded.rejection_count_available),
                }))
    return len(rows)


def validate_request_activity_history(
    request_history: dict[str, Any],
    timestamp: float | None = None,
) -> None:
    """Validate an arrival-history envelope without reading or writing."""
    _request_history_rows('', '', '', request_history, _utc_datetime(timestamp))


def _greatest_nullable(left: Any, right: Any) -> Any:
    """Return a SQL expression that preserves null only when both are null."""
    return sqlalchemy.case((left.is_(None), right), (right.is_(None), left),
                           else_=sqlalchemy.func.greatest(left, right))


def _least_nullable(left: Any, right: Any) -> Any:
    """Return a SQL expression that preserves null only when both are null."""
    return sqlalchemy.case((left.is_(None), right), (right.is_(None), left),
                           else_=sqlalchemy.func.least(left, right))


def _request_classification_rows(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    request_classification_history: dict[str, Any],
    observed_at: datetime.datetime,
) -> list[dict[str, Any]]:
    """Validate one independent terminal-classification minute snapshot."""
    if not isinstance(request_classification_history, dict):
        raise ValueError('request_classification_history must be an object.')
    classification_version = request_classification_history.get(
        'classification_version')
    if (not isinstance(classification_version, int) or
            isinstance(classification_version, bool) or
            classification_version != REQUEST_CLASSIFICATION_PROTOCOL_VERSION):
        raise ValueError(
            'request_classification_history classification_version '
            'is unsupported.')
    if request_classification_history.get('bucket_seconds') != BUCKET_SECONDS:
        raise ValueError(
            'request_classification_history bucket_seconds must be '
            f'{BUCKET_SECONDS}.')
    buckets = request_classification_history.get('buckets')
    if not isinstance(buckets, list):
        raise ValueError('request_classification_history buckets must be a '
                         'list.')
    max_buckets = constants.LB_REQUEST_HISTORY_MAX_BUCKETS
    if len(buckets) > max_buckets:
        raise ValueError('request_classification_history may contain at most '
                         f'{max_buckets} buckets.')

    current_bucket = observed_at.replace(second=0, microsecond=0)
    oldest_bucket = current_bucket - datetime.timedelta(
        seconds=constants.LB_REQUEST_HISTORY_WINDOW_SECONDS +
        2 * BUCKET_SECONDS)
    newest_bucket = current_bucket + datetime.timedelta(seconds=2 *
                                                        BUCKET_SECONDS)
    seen_bucket_starts: set[int] = set()
    rows = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise ValueError('request_classification_history bucket must be an '
                             'object.')
        bucket_start = bucket.get('bucket_start')
        classified_request_count = bucket.get('classified_request_count')
        counted_rejected_count = bucket.get('counted_rejected_count')
        if (not isinstance(bucket_start, int) or
                isinstance(bucket_start, bool) or
                bucket_start % BUCKET_SECONDS != 0):
            raise ValueError('request_classification_history bucket_start must '
                             'be an aligned integer epoch timestamp.')
        if bucket_start in seen_bucket_starts:
            raise ValueError('request_classification_history bucket_start must '
                             'be unique.')
        seen_bucket_starts.add(bucket_start)
        if (not isinstance(classified_request_count, int) or
                isinstance(classified_request_count, bool) or
                classified_request_count < 0):
            raise ValueError(
                'request_classification_history classified_request_count must '
                'be a nonnegative integer.')
        if (not isinstance(counted_rejected_count, int) or
                isinstance(counted_rejected_count, bool) or
                counted_rejected_count < 0):
            raise ValueError(
                'request_classification_history counted_rejected_count must '
                'be a nonnegative integer.')
        if counted_rejected_count > classified_request_count:
            raise ValueError('request_classification_history '
                             'counted_rejected_count cannot exceed '
                             'classified_request_count.')
        if classified_request_count == 0:
            raise ValueError('request_classification_history bucket must '
                             'contain a classified request.')
        bucket_datetime = _utc_datetime(bucket_start)
        if not oldest_bucket <= bucket_datetime <= newest_bucket:
            raise ValueError('request_classification_history bucket_start is '
                             'outside the accepted recent window.')
        rows.append({
            'service_name': service_name,
            'service_hash': service_hash,
            'reporter_session_id': reporter_session_id,
            'bucket_start': bucket_datetime,
            'observed_at': observed_at,
            'request_count': 0,
            'rejected_count': 0,
            'rejection_count_available': True,
            'classified_request_count': classified_request_count,
            'counted_rejected_count': counted_rejected_count,
        })
    return rows


def validate_request_classification_history(
    request_classification_history: dict[str, Any],
    timestamp: float | None = None,
) -> None:
    """Validate a classification envelope without reading or writing state."""
    _request_classification_rows('', '', '', request_classification_history,
                                 _utc_datetime(timestamp))


def record_request_classification(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    request_classification_history: dict[str, Any] | None,
    timestamp: float | None = None,
    request_history: dict[str, Any] | None = None,
) -> int:
    """Atomically persist support evidence and terminal classifications."""
    if request_classification_history is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    classification_rows = _request_classification_rows(
        service_name, service_hash, reporter_session_id,
        request_classification_history, observed_at)
    support_rows = []
    if request_history is not None:
        support_rows = _request_history_rows(service_name, service_hash,
                                             reporter_session_id,
                                             request_history, observed_at)
        for support_row in support_rows:
            support_row['request_count'] = 0
            support_row['rejected_count'] = 0
            support_row['rejection_count_available'] = True
            support_row['classified_request_count'] = 0
            support_row['counted_rejected_count'] = 0

    # A bucket can exist in both snapshots. Merge before INSERT because one
    # PostgreSQL ON CONFLICT statement cannot affect the same key twice.
    rows_by_bucket = {row['bucket_start']: row for row in support_rows}
    for classification_row in classification_rows:
        bucket_start = classification_row['bucket_start']
        existing_row = rows_by_bucket.get(bucket_start)
        if existing_row is None:
            rows_by_bucket[bucket_start] = classification_row
            continue
        existing_row['classified_request_count'] = max(
            int(existing_row['classified_request_count']),
            int(classification_row['classified_request_count']))
        existing_row['counted_rejected_count'] = max(
            int(existing_row['counted_rejected_count']),
            int(classification_row['counted_rejected_count']))
    rows = list(rows_by_bucket.values())
    engine = _postgres_engine()
    if engine is None or not rows:
        return 0
    history = serve_request_activity_history_table
    with engine.begin() as connection:
        insert = postgresql.insert(history).values(rows)
        excluded = insert.excluded
        connection.execute(
            insert.on_conflict_do_update(
                index_elements=[
                    history.c.service_name,
                    history.c.service_hash,
                    history.c.reporter_session_id,
                    history.c.bucket_start,
                ],
                set_={
                    'observed_at': sqlalchemy.func.greatest(
                        history.c.observed_at, excluded.observed_at),
                    'request_count': sqlalchemy.func.greatest(
                        history.c.request_count, excluded.request_count),
                    'rejected_count': sqlalchemy.func.greatest(
                        history.c.rejected_count, excluded.rejected_count),
                    'rejection_count_available': sqlalchemy.or_(
                        history.c.rejection_count_available,
                        excluded.rejection_count_available),
                    'classified_request_count': _greatest_nullable(
                        history.c.classified_request_count,
                        excluded.classified_request_count),
                    'counted_rejected_count': _greatest_nullable(
                        history.c.counted_rejected_count,
                        excluded.counted_rejected_count),
                }))
    return len(rows)


def _response_time_history_rows(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    response_time_history: dict[str, Any],
    observed_at: datetime.datetime,
) -> list[dict[str, Any]]:
    """Validate one LB report and convert it to reporter-minute rows."""
    if not isinstance(response_time_history, dict):
        raise ValueError('response_time_history must be an object.')
    if response_time_history.get('bucket_seconds') != BUCKET_SECONDS:
        raise ValueError('response_time_history bucket_seconds must be '
                         f'{BUCKET_SECONDS}.')
    if (response_time_history.get('histogram_version')
            != constants.LB_RESPONSE_TIME_HISTOGRAM_VERSION):
        raise ValueError('response_time_history histogram_version is '
                         'unsupported.')
    buckets = response_time_history.get('buckets')
    if not isinstance(buckets, list):
        raise ValueError('response_time_history buckets must be a list.')
    if len(buckets) > constants.LB_REQUEST_HISTORY_MAX_BUCKETS:
        raise ValueError('response_time_history contains too many buckets.')

    current_bucket = observed_at.replace(second=0, microsecond=0)
    oldest_bucket = current_bucket - datetime.timedelta(
        seconds=constants.LB_REQUEST_HISTORY_WINDOW_SECONDS +
        2 * BUCKET_SECONDS)
    newest_bucket = current_bucket + datetime.timedelta(seconds=2 *
                                                        BUCKET_SECONDS)
    seen_bucket_starts: set[int] = set()
    rows = []
    expected_classes = set(constants.LB_RESPONSE_TIME_STATUS_CLASSES)
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise ValueError('response_time_history bucket must be an object.')
        bucket_start = bucket.get('bucket_start')
        if (not isinstance(bucket_start, int) or
                isinstance(bucket_start, bool) or
                bucket_start % BUCKET_SECONDS != 0):
            raise ValueError('response_time_history bucket_start must be an '
                             'aligned integer epoch timestamp.')
        if bucket_start in seen_bucket_starts:
            raise ValueError('response_time_history bucket_start must be '
                             'unique.')
        seen_bucket_starts.add(bucket_start)
        bucket_datetime = _utc_datetime(bucket_start)
        if not oldest_bucket <= bucket_datetime <= newest_bucket:
            raise ValueError('response_time_history bucket_start is outside '
                             'the accepted recent window.')

        status_class_counts = bucket.get('status_class_counts')
        if not isinstance(status_class_counts, dict):
            raise ValueError('status_class_counts must be an object.')
        if not set(status_class_counts).issubset(expected_classes):
            raise ValueError('status_class_counts contains an unsupported '
                             'HTTP status class.')
        row = {
            'service_name': service_name,
            'service_hash': service_hash,
            'reporter_session_id': reporter_session_id,
            'bucket_start': bucket_datetime,
            'observed_at': observed_at,
        }
        response_count = 0
        for status_class in constants.LB_RESPONSE_TIME_STATUS_CLASSES:
            counts = status_class_counts.get(
                status_class, [0] * constants.LB_RESPONSE_TIME_BUCKET_COUNT)
            if (not isinstance(counts, list) or
                    len(counts) != constants.LB_RESPONSE_TIME_BUCKET_COUNT or
                    any(not isinstance(count, int) or isinstance(count, bool) or
                        count < 0 for count in counts)):
                raise ValueError(f'{status_class} response histogram must '
                                 'contain the fixed number of nonnegative '
                                 'integer buckets.')
            response_count += sum(counts)
            row[f'status_{status_class}_counts'] = counts
        if response_count == 0:
            raise ValueError('response_time_history bucket must contain a '
                             'completed response.')
        row['response_count'] = response_count
        rows.append(row)
    return rows


def record_response_times(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    response_time_history: dict[str, Any] | None,
    timestamp: float | None = None,
) -> int:
    """Persist cumulative minute histograms from one LB process."""
    if response_time_history is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    rows = _response_time_history_rows(service_name, service_hash,
                                       reporter_session_id,
                                       response_time_history, observed_at)
    engine = _postgres_engine()
    if engine is None or not rows:
        return 0
    table = serve_response_time_history_table
    with engine.begin() as connection:
        insert = postgresql.insert(table).values(rows)
        excluded = insert.excluded
        connection.execute(
            insert.on_conflict_do_update(
                index_elements=[
                    table.c.service_name,
                    table.c.service_hash,
                    table.c.reporter_session_id,
                    table.c.bucket_start,
                ],
                set_={
                    'observed_at': sqlalchemy.func.greatest(
                        table.c.observed_at, excluded.observed_at),
                    'response_count': excluded.response_count,
                    **{
                        f'status_{status_class}_counts': getattr(
                            excluded, f'status_{status_class}_counts') for status_class in constants.LB_RESPONSE_TIME_STATUS_CLASSES
                    },
                },
                where=excluded.response_count >= table.c.response_count))
    return len(rows)


def _prediction_time_history_rows(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    prediction_time_history: dict[str, Any],
    observed_at: datetime.datetime,
) -> list[dict[str, Any]]:
    """Validate one LB prediction report and build reporter-minute rows."""
    if not isinstance(prediction_time_history, dict):
        raise ValueError('prediction_time_history must be an object.')
    if prediction_time_history.get('bucket_seconds') != BUCKET_SECONDS:
        raise ValueError('prediction_time_history bucket_seconds must be '
                         f'{BUCKET_SECONDS}.')
    if (prediction_time_history.get('histogram_version')
            != constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION):
        raise ValueError('prediction_time_history histogram_version is '
                         'unsupported.')
    buckets = prediction_time_history.get('buckets')
    if not isinstance(buckets, list):
        raise ValueError('prediction_time_history buckets must be a list.')
    if len(buckets) > constants.LB_REQUEST_HISTORY_MAX_BUCKETS:
        raise ValueError('prediction_time_history contains too many buckets.')

    current_bucket = observed_at.replace(second=0, microsecond=0)
    oldest_bucket = current_bucket - datetime.timedelta(
        seconds=constants.LB_REQUEST_HISTORY_WINDOW_SECONDS +
        2 * BUCKET_SECONDS)
    newest_bucket = current_bucket + datetime.timedelta(seconds=2 *
                                                        BUCKET_SECONDS)
    seen_bucket_starts: set[int] = set()
    rows = []
    expected_outcomes = set(constants.LB_PREDICTION_TIME_OUTCOMES)
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise ValueError(
                'prediction_time_history bucket must be an object.')
        bucket_start = bucket.get('bucket_start')
        if (not isinstance(bucket_start, int) or
                isinstance(bucket_start, bool) or
                bucket_start % BUCKET_SECONDS != 0):
            raise ValueError('prediction_time_history bucket_start must be an '
                             'aligned integer epoch timestamp.')
        if bucket_start in seen_bucket_starts:
            raise ValueError('prediction_time_history bucket_start must be '
                             'unique.')
        seen_bucket_starts.add(bucket_start)
        bucket_datetime = _utc_datetime(bucket_start)
        if not oldest_bucket <= bucket_datetime <= newest_bucket:
            raise ValueError('prediction_time_history bucket_start is outside '
                             'the accepted recent window.')

        outcome_counts = bucket.get('outcome_counts')
        if not isinstance(outcome_counts, dict):
            raise ValueError('outcome_counts must be an object.')
        if not set(outcome_counts).issubset(expected_outcomes):
            raise ValueError('outcome_counts contains an unsupported '
                             'prediction outcome.')
        row = {
            'service_name': service_name,
            'service_hash': service_hash,
            'reporter_session_id': reporter_session_id,
            'bucket_start': bucket_datetime,
            'observed_at': observed_at,
        }
        prediction_count = 0
        for outcome in constants.LB_PREDICTION_TIME_OUTCOMES:
            counts = outcome_counts.get(
                outcome, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
            if (not isinstance(counts, list) or
                    len(counts) != constants.LB_PREDICTION_TIME_BUCKET_COUNT or
                    any(not isinstance(count, int) or isinstance(count, bool) or
                        count < 0 for count in counts)):
                raise ValueError(f'{outcome} prediction histogram must '
                                 'contain the fixed number of nonnegative '
                                 'integer buckets.')
            prediction_count += sum(counts)
            row[f'{outcome}_counts'] = counts
        if prediction_count == 0:
            raise ValueError('prediction_time_history bucket must contain a '
                             'completed prediction.')
        row['prediction_count'] = prediction_count
        rows.append(row)
    return rows


def record_prediction_times(
    service_name: str,
    service_hash: str,
    reporter_session_id: str,
    prediction_time_history: dict[str, Any] | None,
    timestamp: float | None = None,
) -> int:
    """Persist cumulative prediction histograms from one LB process."""
    if prediction_time_history is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    rows = _prediction_time_history_rows(service_name, service_hash,
                                         reporter_session_id,
                                         prediction_time_history, observed_at)
    engine = _postgres_engine()
    if engine is None or not rows:
        return 0
    table = serve_prediction_time_history_table
    with engine.begin() as connection:
        insert = postgresql.insert(table).values(rows)
        excluded = insert.excluded
        connection.execute(
            insert.
            on_conflict_do_update(index_elements=[
                table.c.service_name,
                table.c.service_hash,
                table.c.reporter_session_id,
                table.c.bucket_start,
            ],
                                  set_={
                                      'observed_at': sqlalchemy.func.greatest(
                                          table.c.observed_at,
                                          excluded.observed_at),
                                      'prediction_count':
                                          excluded.prediction_count,
                                      **{
                                          f'{outcome}_counts': getattr(
                                              excluded, f'{outcome}_counts') for outcome in constants.LB_PREDICTION_TIME_OUTCOMES
                                      },
                                  },
                                  where=excluded.prediction_count
                                  >= table.c.prediction_count))
    return len(rows)


def validate_prediction_time_history(
    prediction_time_history: dict[str, Any],
    timestamp: float | None = None,
) -> None:
    """Validate a prediction histogram without reading or writing state."""
    _prediction_time_history_rows('', '', '', prediction_time_history,
                                  _utc_datetime(timestamp))


def _nonnegative_int(value: Any,
                     field: str,
                     *,
                     nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if (not isinstance(value, int) or isinstance(value, bool) or value < 0):
        suffix = ' or null' if nullable else ''
        raise ValueError(f'{field} must be a nonnegative integer{suffix}.')
    return value


_ACCELERATOR_BREAKDOWN_MAP_FIELDS = (
    'min_replicas',
    'demand_target',
    'ready_capacity',
    'provisioning_capacity',
    'total_capacity',
    'zero_cost_ready_capacity',
    'fill_target',
    'free_reserved_slots',
)

_OPTIONAL_ACCELERATOR_BREAKDOWN_MAP_FIELDS = (
    'warm_retention_target',
    'cold_launch_authority',
)


def _normalize_accelerator_breakdown(
        value: dict[str, Any] | None) -> dict[str, Any]:
    """Validate one bounded, exact-card history payload."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError('accelerator_breakdown must be an object or null.')
    configured = value.get('configured_accelerators')
    if (not isinstance(configured, list) or not configured or
            len(configured) > constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS or
            not all(isinstance(card, str) and card for card in configured) or
            len({card.casefold() for card in configured}) != len(configured)):
        raise ValueError('configured_accelerators must be distinct non-empty '
                         'exact card identifiers within the supported bound.')
    configured_set = set(configured)
    result: dict[str, Any] = {
        'version': constants.LB_REQUEST_ACCELERATORS_VERSION,
        'configured_accelerators': configured,
    }
    if 'capacity_semantics_version' in value:
        capacity_semantics_version = value['capacity_semantics_version']
        if (not isinstance(capacity_semantics_version, int) or
                isinstance(capacity_semantics_version, bool) or
                capacity_semantics_version < 1):
            raise ValueError(
                'capacity_semantics_version must be a positive integer.')
        result['capacity_semantics_version'] = capacity_semantics_version
    for field in _ACCELERATOR_BREAKDOWN_MAP_FIELDS:
        raw_mapping = value.get(field, {})
        if not isinstance(raw_mapping, dict):
            raise ValueError(f'{field} must be an exact-card count object.')
        if not set(raw_mapping).issubset(configured_set):
            raise ValueError(f'{field} contains an unconfigured accelerator.')
        normalized = {}
        for card in configured:
            raw_count = raw_mapping.get(card, 0)
            count = _nonnegative_int(raw_count, f'{field}[{card}]')
            assert count is not None
            normalized[card] = count
        result[field] = normalized
    for field in _OPTIONAL_ACCELERATOR_BREAKDOWN_MAP_FIELDS:
        if field not in value:
            continue
        raw_mapping = value[field]
        if not isinstance(raw_mapping, dict):
            raise ValueError(f'{field} must be an exact-card count object.')
        if not set(raw_mapping).issubset(configured_set):
            raise ValueError(f'{field} contains an unconfigured accelerator.')
        normalized = {}
        for card in configured:
            count = _nonnegative_int(raw_mapping.get(card, 0),
                                     f'{field}[{card}]')
            assert count is not None
            normalized[card] = count
        result[field] = normalized
    return result


def _accelerator_breakdown_aggregate_observation(
    *,
    controller_session_id: str,
    version: int,
    replica_unit: str,
    demand_target: int,
    capacity_target: int,
    ready_capacity: int,
    provisioning_capacity: int,
    total_capacity: int,
) -> dict[str, Any]:
    """Return the aggregate fields that an exact-card map explains."""
    return {
        'controller_session_id': controller_session_id,
        'version': version,
        'replica_unit': replica_unit,
        'demand_target': demand_target,
        'capacity_target': capacity_target,
        'ready_capacity': ready_capacity,
        'provisioning_capacity': provisioning_capacity,
        'total_capacity': total_capacity,
    }


def record_autoscaler_snapshot(
    service_name: str,
    service_hash: str,
    controller_session_id: str,
    *,
    version: int,
    replica_unit: str,
    demand_target: int,
    capacity_target: int,
    ready_capacity: int,
    provisioning_capacity: int,
    total_capacity: int,
    peak_in_flight: int | None = None,
    peak_queue_depth: int | None = None,
    accelerator_breakdown: dict[str, Any] | None = None,
    timestamp: float | None = None,
) -> int:
    """Persist one controller-authored autoscaler observation.

    Latest target/capacity fields win within a minute while pressure gauges
    retain their peak. Non-PostgreSQL deployments accept and drop the sample.
    """
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(service_hash, str) or not service_hash):
        raise ValueError('service_name and service_hash must be non-empty.')
    if (not isinstance(controller_session_id, str) or
            len(controller_session_id) != 32 or
            any(character not in '0123456789abcdef'
                for character in controller_session_id)):
        raise ValueError('controller_session_id must be a lowercase hex UUID.')
    if (not isinstance(version, int) or isinstance(version, bool) or
            version < 1):
        raise ValueError('version must be a positive integer.')
    if replica_unit not in {'physical_backend', 'logical_slot'}:
        raise ValueError('replica_unit must identify physical or logical '
                         'capacity.')
    demand_target = _nonnegative_int(demand_target, 'demand_target')
    capacity_target = _nonnegative_int(capacity_target, 'capacity_target')
    ready_capacity = _nonnegative_int(ready_capacity, 'ready_capacity')
    provisioning_capacity = _nonnegative_int(provisioning_capacity,
                                             'provisioning_capacity')
    total_capacity = _nonnegative_int(total_capacity, 'total_capacity')
    peak_in_flight = _nonnegative_int(peak_in_flight,
                                      'peak_in_flight',
                                      nullable=True)
    peak_queue_depth = _nonnegative_int(peak_queue_depth,
                                        'peak_queue_depth',
                                        nullable=True)
    accelerator_breakdown = _normalize_accelerator_breakdown(
        accelerator_breakdown)
    assert demand_target is not None
    assert capacity_target is not None
    assert ready_capacity is not None
    assert provisioning_capacity is not None
    assert total_capacity is not None
    if capacity_target < demand_target:
        raise ValueError('capacity_target must be at least demand_target.')
    if accelerator_breakdown:
        accelerator_breakdown['_aggregate_observation'] = (
            _accelerator_breakdown_aggregate_observation(
                controller_session_id=controller_session_id,
                version=version,
                replica_unit=replica_unit,
                demand_target=demand_target,
                capacity_target=capacity_target,
                ready_capacity=ready_capacity,
                provisioning_capacity=provisioning_capacity,
                total_capacity=total_capacity,
            ))

    engine = _postgres_engine()
    if engine is None:
        return 0
    observed_at = _utc_datetime(timestamp)
    bucket_start = observed_at.replace(second=0, microsecond=0)
    row = {
        'service_name': service_name,
        'service_hash': service_hash,
        'bucket_start': bucket_start,
        'observed_at': observed_at,
        'controller_session_id': controller_session_id,
        'version': version,
        'replica_unit': replica_unit,
        'demand_target': demand_target,
        'capacity_target': capacity_target,
        'ready_capacity': ready_capacity,
        'provisioning_capacity': provisioning_capacity,
        'total_capacity': total_capacity,
        'peak_in_flight': peak_in_flight,
        'peak_queue_depth': peak_queue_depth,
        'accelerator_breakdown': accelerator_breakdown,
        'accelerator_breakdown_observed_at': observed_at,
    }
    table = serve_autoscaler_history_table
    with engine.begin() as connection:
        insert = postgresql.insert(table).values(row)
        excluded = insert.excluded
        newest = excluded.observed_at >= table.c.observed_at

        def latest(column: str) -> Any:
            return sqlalchemy.case((newest, getattr(excluded, column)),
                                   else_=getattr(table.c, column))

        def peak(column: str) -> Any:
            existing = getattr(table.c, column)
            incoming = getattr(excluded, column)
            return sqlalchemy.case(
                (existing.is_(None), incoming), (incoming.is_(None), existing),
                else_=sqlalchemy.func.greatest(existing, incoming))

        connection.execute(
            insert.on_conflict_do_update(
                index_elements=[
                    table.c.service_name,
                    table.c.service_hash,
                    table.c.bucket_start,
                ],
                set_={
                    'observed_at': sqlalchemy.func.greatest(
                        table.c.observed_at, excluded.observed_at),
                    'controller_session_id': latest('controller_session_id'),
                    'version': latest('version'),
                    'replica_unit': latest('replica_unit'),
                    'demand_target': latest('demand_target'),
                    'capacity_target': latest('capacity_target'),
                    'ready_capacity': latest('ready_capacity'),
                    'provisioning_capacity': latest('provisioning_capacity'),
                    'total_capacity': latest('total_capacity'),
                    'peak_in_flight': peak('peak_in_flight'),
                    'peak_queue_depth': peak('peak_queue_depth'),
                    'accelerator_breakdown': latest('accelerator_breakdown'),
                    'accelerator_breakdown_observed_at':
                        latest('accelerator_breakdown_observed_at'),
                }))
    return 1


def _normalize_status_history_sections(
    sections: Collection[str] | None,) -> frozenset[str]:
    if sections is None:
        return STATUS_HISTORY_SECTIONS
    if isinstance(sections, str):
        raise ValueError('sections must be a collection of section names, not '
                         'a string.')
    try:
        requested_sections = frozenset(sections)
    except TypeError as e:
        raise ValueError('sections must contain only section names.') from e
    if any(not isinstance(section, str) for section in requested_sections):
        raise ValueError('sections must contain only string section names.')
    invalid_sections = requested_sections - STATUS_HISTORY_SECTIONS
    if not requested_sections or invalid_sections:
        expected = ', '.join(sorted(STATUS_HISTORY_SECTIONS))
        raise ValueError('sections must contain at least one of '
                         f'{expected}; got {sorted(invalid_sections)!r}.')
    return requested_sections


def unavailable_status_history(reason: str,
                               sections: Collection[str] | None = None
                              ) -> dict[str, Any]:
    """Build a stable unavailable response for selected history sections."""
    requested_sections = _normalize_status_history_sections(sections)
    response: dict[str, Any] = {
        'available': False,
        'reason': reason,
        'bucket_seconds': BUCKET_SECONDS,
        'retention_hours': RETENTION_HOURS,
    }
    if 'replicas' in requested_sections:
        response['samples'] = []
    if 'requests' in requested_sections:
        response.update({
            'request_samples': [],
            'rejection_history_available': False,
            'request_window_seconds':
                constants.LB_REQUEST_HISTORY_WINDOW_SECONDS,
            'requests_last_hour': 0,
        })
    if 'prediction' in requested_sections:
        response.update({
            'prediction_time_samples': [],
            'prediction_time_histogram_version':
                constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION,
            'prediction_time_bucket_upper_bounds_seconds': list(
                constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS),
        })
    if 'autoscaler' in requested_sections:
        response['autoscaler_samples'] = []
    return response


def get_status_history(
        service_name: str,
        hours: int = DEFAULT_HISTORY_HOURS,
        version: int | None = None,
        timestamp: float | None = None,
        expected_service_hash: str | None = None,
        sections: Collection[str] | None = None) -> dict[str, Any]:
    """Return ordered aggregate history for the current service incarnation."""
    if (not isinstance(hours, int) or isinstance(hours, bool) or hours < 1 or
            hours > RETENTION_HOURS):
        raise ValueError(f'hours must be an integer from 1 to '
                         f'{RETENTION_HOURS}, got {hours!r}.')
    if version is not None and (not isinstance(version, int) or
                                isinstance(version, bool) or version < 1):
        raise ValueError(f'version must be a positive integer, got '
                         f'{version!r}.')
    if expected_service_hash is not None and (not isinstance(
            expected_service_hash, str) or not expected_service_hash):
        raise ValueError('expected_service_hash must be a non-empty string, '
                         f'got {expected_service_hash!r}.')
    requested_sections = _normalize_status_history_sections(sections)

    engine = _postgres_engine()
    if engine is None:
        return unavailable_status_history('postgres_required',
                                          requested_sections)

    observed_at = _utc_datetime(timestamp)
    window_start = observed_at - datetime.timedelta(hours=hours)
    services = serve_state.services_table
    history = serve_replica_status_history_table
    with orm.Session(engine) as session:
        service_hash = session.execute(
            sqlalchemy.select(services.c.hash).where(
                services.c.name == service_name,
                services.c.pool == 0)).scalar_one_or_none()
        if service_hash is None:
            return unavailable_status_history('service_not_found',
                                              requested_sections)
        if (expected_service_hash is not None and
                service_hash != expected_service_hash):
            return unavailable_status_history('service_hash_mismatch',
                                              requested_sections)
        rows = []
        request_rows = []
        prediction_rows = []
        autoscaler_rows = []
        if 'replicas' in requested_sections:
            predicates = [
                history.c.service_name == service_name,
                history.c.service_hash == service_hash,
                history.c.bucket_start >= window_start,
                history.c.bucket_start <= observed_at,
            ]
            if version is not None:
                predicates.append(history.c.version == version)
            rows = session.execute(
                sqlalchemy.select(history).where(*predicates).order_by(
                    history.c.bucket_start,
                    history.c.version)).mappings().all()
        if 'requests' in requested_sections:
            request_history = serve_request_activity_history_table
            request_rows = session.execute(
                sqlalchemy.select(
                    request_history.c.bucket_start,
                    sqlalchemy.func.sum(  # pylint: disable=not-callable
                        request_history.c.request_count).label('request_count'),
                    sqlalchemy.func.sum(  # pylint: disable=not-callable
                        request_history.c.rejected_count).label(
                            'rejected_count'),
                    sqlalchemy.func.bool_and(  # pylint: disable=not-callable
                        request_history.c.rejection_count_available).label(
                            'rejection_count_available'),
                    sqlalchemy.func.bool_or(  # pylint: disable=not-callable
                        request_history.c.rejection_count_available).label(
                            'rejection_count_supported'),
                ).where(
                    request_history.c.service_name == service_name,
                    request_history.c.service_hash == service_hash,
                    request_history.c.bucket_start >= window_start,
                    request_history.c.bucket_start <= observed_at,
                ).group_by(request_history.c.bucket_start).order_by(
                    request_history.c.bucket_start)).all()
        if 'prediction' in requested_sections:
            prediction_history = serve_prediction_time_history_table
            prediction_rows = session.execute(
                sqlalchemy.select(prediction_history).where(
                    prediction_history.c.service_name == service_name,
                    prediction_history.c.service_hash == service_hash,
                    prediction_history.c.bucket_start >= window_start,
                    prediction_history.c.bucket_start <= observed_at,
                ).order_by(prediction_history.c.bucket_start)).mappings().all()
        if 'autoscaler' in requested_sections:
            autoscaler_history = serve_autoscaler_history_table
            autoscaler_rows = session.execute(
                sqlalchemy.select(autoscaler_history).where(
                    autoscaler_history.c.service_name == service_name,
                    autoscaler_history.c.service_hash == service_hash,
                    autoscaler_history.c.bucket_start >= window_start,
                    autoscaler_history.c.bucket_start <= observed_at,
                ).order_by(autoscaler_history.c.bucket_start)).mappings().all()

    samples = []
    for row in rows:
        samples.append({
            'timestamp': row['bucket_start'].timestamp(),
            'observed_at': row['observed_at'].timestamp(),
            'version': row['version'],
            **{
                column: row[column] for column in _COUNT_COLUMNS
            },
            **{
                column: row[column] for column in _OPTIONAL_STATUS_COLUMNS
            },
            'total_count': row['total_count'],
        })
    request_samples = []
    for row in request_rows:
        rejected_count = (int(row.rejected_count)
                          if row.rejection_count_available else None)
        request_samples.append({
            'timestamp': row.bucket_start.timestamp(),
            'request_count': int(row.request_count),
            'rejected_count': rejected_count,
        })
    prediction_time_by_bucket: dict[datetime.datetime, dict[str,
                                                            list[int]]] = {}
    for row in prediction_rows:
        aggregate = prediction_time_by_bucket.setdefault(
            row['bucket_start'], {
                outcome: [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT
                for outcome in constants.LB_PREDICTION_TIME_OUTCOMES
            })
        for outcome in constants.LB_PREDICTION_TIME_OUTCOMES:
            counts = row[f'{outcome}_counts']
            aggregate[outcome] = [
                existing + int(incoming)
                for existing, incoming in zip(aggregate[outcome], counts)
            ]
    prediction_time_samples = []
    for bucket_start in sorted(prediction_time_by_bucket):
        counts_by_outcome = prediction_time_by_bucket[bucket_start]
        prediction_time_samples.append({
            'timestamp': bucket_start.timestamp(),
            'outcome_counts': {
                outcome: counts
                for outcome, counts in counts_by_outcome.items()
                if any(counts)
            },
        })

    def exact_breakdown(row: Any) -> dict[str, Any] | None:
        breakdown = row['accelerator_breakdown']
        if (not isinstance(breakdown, dict) or not breakdown or
                row['accelerator_breakdown_observed_at'] != row['observed_at']):
            return None
        expected_aggregate = _accelerator_breakdown_aggregate_observation(
            controller_session_id=row['controller_session_id'],
            version=row['version'],
            replica_unit=row['replica_unit'],
            demand_target=row['demand_target'],
            capacity_target=row['capacity_target'],
            ready_capacity=row['ready_capacity'],
            provisioning_capacity=row['provisioning_capacity'],
            total_capacity=row['total_capacity'],
        )
        if breakdown.get('_aggregate_observation') != expected_aggregate:
            return None
        serialized = dict(breakdown)
        serialized.pop('_aggregate_observation', None)
        return serialized

    autoscaler_samples = [{
        'timestamp': row['bucket_start'].timestamp(),
        'observed_at': row['observed_at'].timestamp(),
        'controller_session_id': row['controller_session_id'],
        'version': row['version'],
        'replica_unit': row['replica_unit'],
        'demand_target': row['demand_target'],
        'capacity_target': row['capacity_target'],
        'ready_capacity': row['ready_capacity'],
        'provisioning_capacity': row['provisioning_capacity'],
        'total_capacity': row['total_capacity'],
        'peak_in_flight': row['peak_in_flight'],
        'peak_queue_depth': row['peak_queue_depth'],
        'accelerator_breakdown': exact_breakdown(row),
    } for row in autoscaler_rows]
    current_bucket = observed_at.replace(second=0, microsecond=0)
    request_window_start = current_bucket - datetime.timedelta(
        seconds=constants.LB_REQUEST_HISTORY_WINDOW_SECONDS - BUCKET_SECONDS)
    requests_last_hour = sum(
        sample['request_count']
        for sample in request_samples
        if sample['timestamp'] >= request_window_start.timestamp())
    response: dict[str, Any] = {
        'available': True,
        'service_hash': service_hash,
        'bucket_seconds': BUCKET_SECONDS,
        'retention_hours': RETENTION_HOURS,
        'window_start': window_start.timestamp(),
        'window_end': observed_at.timestamp(),
    }
    if 'replicas' in requested_sections:
        response['samples'] = samples
    if 'requests' in requested_sections:
        response.update({
            'request_samples': request_samples,
            'rejection_history_available': any(
                row.rejection_count_supported for row in request_rows),
            'request_window_seconds':
                constants.LB_REQUEST_HISTORY_WINDOW_SECONDS,
            'requests_last_hour': requests_last_hour,
        })
    if 'prediction' in requested_sections:
        response.update({
            'prediction_time_samples': prediction_time_samples,
            'prediction_time_histogram_version':
                constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION,
            'prediction_time_bucket_upper_bounds_seconds': list(
                constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS),
        })
    if 'autoscaler' in requested_sections:
        response['autoscaler_samples'] = autoscaler_samples
    return response
