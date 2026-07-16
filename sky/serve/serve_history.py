"""PostgreSQL-backed aggregate history for SkyServe machine statuses."""

import datetime
from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.serve import constants
from sky.serve import serve_state
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
    sqlalchemy.Column('provisioning_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('not_ready_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('errored_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('preempted_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('stopping_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('total_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.CheckConstraint(
        'ready_count >= 0 AND provisioning_count >= 0 AND '
        'not_ready_count >= 0 AND errored_count >= 0 AND '
        'preempted_count >= 0 AND stopping_count >= 0 AND total_count >= 0',
        name='serve_replica_status_history_nonnegative'),
    sqlalchemy.CheckConstraint(
        'total_count = ready_count + provisioning_count + '
        'not_ready_count + errored_count + preempted_count + stopping_count',
        name='serve_replica_status_history_total'),
)
sqlalchemy.Index('serve_replica_status_history_lookup_idx',
                 serve_replica_status_history_table.c.service_name,
                 serve_replica_status_history_table.c.service_hash,
                 serve_replica_status_history_table.c.bucket_start.desc())
sqlalchemy.Index('serve_replica_status_history_bucket_idx',
                 serve_replica_status_history_table.c.bucket_start)

_COUNT_COLUMNS = (
    'ready_count',
    'provisioning_count',
    'not_ready_count',
    'errored_count',
    'preempted_count',
    'stopping_count',
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
    """One compact snapshot of every non-pool service and physical row."""
    services = serve_state.services_table
    replicas = serve_state.replicas_table
    version = sqlalchemy.func.coalesce(replicas.c.version,
                                       services.c.current_version,
                                       constants.INITIAL_VERSION)
    return (sqlalchemy.select(
        services.c.name,
        services.c.hash,
        version.label('version'),
        replicas.c.status,
        sqlalchemy.func.count(  # pylint: disable=not-callable
            replicas.c.replica_id).label('count'),
    ).select_from(
        services.outerjoin(replicas,
                           replicas.c.service_name == services.c.name)).where(
                               services.c.pool == 0,
                               services.c.hash.is_not(None)).group_by(
                                   services.c.name, services.c.hash, version,
                                   replicas.c.status))


def _build_history_rows(
        rows: list[Any], observed_at: datetime.datetime,
        bucket_start: datetime.datetime) -> list[dict[str, Any]]:
    """Collapse status groups into one exhaustive row per service/version."""
    grouped: dict[tuple[str, str, int], dict[str, Any]] = {}
    for service_name, service_hash, version, status, count in rows:
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
                'total_count': 0,
            }
            grouped[key] = record
        count = int(count)
        if count:
            record[_status_bucket(status)] += count
            record['total_count'] += count
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
    return len(history_rows)


def get_status_history(service_name: str,
                       hours: int = DEFAULT_HISTORY_HOURS,
                       version: int | None = None,
                       timestamp: float | None = None) -> dict[str, Any]:
    """Return ordered aggregate history for the current service incarnation."""
    if (not isinstance(hours, int) or isinstance(hours, bool) or hours < 1 or
            hours > RETENTION_HOURS):
        raise ValueError(f'hours must be an integer from 1 to '
                         f'{RETENTION_HOURS}, got {hours!r}.')
    if version is not None and (not isinstance(version, int) or
                                isinstance(version, bool) or version < 1):
        raise ValueError(f'version must be a positive integer, got '
                         f'{version!r}.')

    engine = _postgres_engine()
    if engine is None:
        return {
            'available': False,
            'bucket_seconds': BUCKET_SECONDS,
            'retention_hours': RETENTION_HOURS,
            'samples': [],
        }

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
            return {
                'available': False,
                'bucket_seconds': BUCKET_SECONDS,
                'retention_hours': RETENTION_HOURS,
                'samples': [],
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

    samples = []
    for row in rows:
        samples.append({
            'timestamp': row['bucket_start'].timestamp(),
            'observed_at': row['observed_at'].timestamp(),
            'version': row['version'],
            **{
                column: row[column] for column in _COUNT_COLUMNS
            },
            'total_count': row['total_count'],
        })
    return {
        'available': True,
        'service_hash': service_hash,
        'bucket_seconds': BUCKET_SECONDS,
        'retention_hours': RETENTION_HOURS,
        'window_start': window_start.timestamp(),
        'window_end': observed_at.timestamp(),
        'samples': samples,
    }
