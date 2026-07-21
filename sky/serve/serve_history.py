"""PostgreSQL-backed aggregate history for SkyServe status and requests."""

import datetime
from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.serve import constants
from sky.serve import serve_state
from sky.utils import common_utils
from sky.utils.db import db_utils

DEFAULT_HISTORY_HOURS = 12
RETENTION_HOURS = 72
BUCKET_SECONDS = 60

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
    sqlalchemy.CheckConstraint(
        'request_count >= 0',
        name='serve_request_activity_history_nonnegative'),
    sqlalchemy.CheckConstraint(
        'rejected_count >= 0',
        name='serve_request_activity_history_rejected_nonnegative'),
)
sqlalchemy.Index('serve_request_activity_history_lookup_idx',
                 serve_request_activity_history_table.c.service_name,
                 serve_request_activity_history_table.c.service_hash,
                 serve_request_activity_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_request_activity_history_bucket_idx',
                 serve_request_activity_history_table.c.bucket_start)

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
                sqlalchemy.delete(serve_autoscaler_history_table).where(
                    serve_autoscaler_history_table.c.bucket_start < cutoff))
    return len(history_rows)


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
    assert demand_target is not None
    assert capacity_target is not None
    assert ready_capacity is not None
    assert provisioning_capacity is not None
    assert total_capacity is not None
    if capacity_target < demand_target:
        raise ValueError('capacity_target must be at least demand_target.')

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
                }))
    return 1


def get_status_history(
        service_name: str,
        hours: int = DEFAULT_HISTORY_HOURS,
        version: int | None = None,
        timestamp: float | None = None,
        expected_service_hash: str | None = None) -> dict[str, Any]:
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

    engine = _postgres_engine()
    if engine is None:
        return {
            'available': False,
            'bucket_seconds': BUCKET_SECONDS,
            'retention_hours': RETENTION_HOURS,
            'samples': [],
            'request_samples': [],
            'autoscaler_samples': [],
            'rejection_history_available': False,
            'request_window_seconds':
                constants.LB_REQUEST_HISTORY_WINDOW_SECONDS,
            'requests_last_hour': 0,
        }

    observed_at = _utc_datetime(timestamp)
    window_start = observed_at - datetime.timedelta(hours=hours)
    services = serve_state.services_table
    history = serve_replica_status_history_table
    with orm.Session(engine) as session:
        service_predicates = [
            services.c.name == service_name,
            services.c.pool == 0,
        ]
        if expected_service_hash is not None:
            service_predicates.append(services.c.hash == expected_service_hash)
        service_hash = session.execute(
            sqlalchemy.select(services.c.hash).where(
                *service_predicates)).scalar_one_or_none()
        if service_hash is None:
            return {
                'available': False,
                'bucket_seconds': BUCKET_SECONDS,
                'retention_hours': RETENTION_HOURS,
                'samples': [],
                'request_samples': [],
                'autoscaler_samples': [],
                'rejection_history_available': False,
                'request_window_seconds':
                    constants.LB_REQUEST_HISTORY_WINDOW_SECONDS,
                'requests_last_hour': 0,
            }
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
                history.c.bucket_start, history.c.version)).mappings().all()
        request_history = serve_request_activity_history_table
        request_rows = session.execute(
            sqlalchemy.select(
                request_history.c.bucket_start,
                sqlalchemy.func.sum(  # pylint: disable=not-callable
                    request_history.c.request_count).label('request_count'),
                sqlalchemy.func.sum(  # pylint: disable=not-callable
                    request_history.c.rejected_count).label('rejected_count'),
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
    } for row in autoscaler_rows]
    current_bucket = observed_at.replace(second=0, microsecond=0)
    request_window_start = current_bucket - datetime.timedelta(
        seconds=constants.LB_REQUEST_HISTORY_WINDOW_SECONDS - BUCKET_SECONDS)
    requests_last_hour = sum(
        sample['request_count']
        for sample in request_samples
        if sample['timestamp'] >= request_window_start.timestamp())
    return {
        'available': True,
        'service_hash': service_hash,
        'bucket_seconds': BUCKET_SECONDS,
        'retention_hours': RETENTION_HOURS,
        'window_start': window_start.timestamp(),
        'window_end': observed_at.timestamp(),
        'samples': samples,
        'request_samples': request_samples,
        'autoscaler_samples': autoscaler_samples,
        'rejection_history_available': any(
            row.rejection_count_supported for row in request_rows),
        'request_window_seconds': constants.LB_REQUEST_HISTORY_WINDOW_SECONDS,
        'requests_last_hour': requests_last_hour,
    }
