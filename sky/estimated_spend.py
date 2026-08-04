"""Eventually consistent, best-effort compute spend estimates."""

import asyncio
from collections.abc import Iterable
from collections.abc import Mapping
import datetime
import enum
import time
from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import estimated_spend_cost
from sky import global_user_state
from sky import sky_logging
from sky.serve import serve_history
from sky.utils import locks
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

SECONDS_PER_DAY = estimated_spend_cost.SECONDS_PER_DAY
SECONDS_PER_HOUR = estimated_spend_cost.SECONDS_PER_HOUR
DEFAULT_LOOKBACK_DAYS = 30
MAX_LOOKBACK_DAYS = 90
ROLLUP_LOOKBACK_DAYS = 90
REFRESH_INTERVAL_SECONDS = 5 * 60
ACTIVE_RECOMPUTE_DAYS = 2
BACKFILL_BATCH_SIZE = 100
CHANGED_BATCH_SIZE = 100
ACTIVE_ROW_LIMIT = 2000
PERSIST_BATCH_SIZE = 10
PRUNE_BATCH_SIZE = 1000
ROLLUP_LOCK_ID = 'estimated-spend-rollup'
_STATE_ID = 1
_HASH_END_SENTINEL = '\uffff'
GROUP_TABLE_LIMIT = 50
GROUP_CHART_LIMIT = 8
DRILLDOWN_DEFAULT_LIMIT = 50
DRILLDOWN_MAX_LIMIT = 100
# Serve's placement catalog values Kubernetes locations as reserved or
# already-paid capacity. Keep this exception scoped to the service ratio:
# global spend continues to report Kubernetes time as excluded.
_SERVICE_ZERO_COST_EXCLUSION_REASONS = ('kubernetes',)


class GroupBy(str, enum.Enum):
    """Supported estimated-spend dashboard breakdowns."""

    JOB = 'job'
    USER = 'user'
    PURCHASE_OPTION = 'purchase_option'


class SpendDrilldownLevel(str, enum.Enum):
    """Supported levels in the spend-attribution hierarchy."""

    OWNER = 'owner'
    WORKLOAD = 'workload'
    TASK = 'task'
    CLUSTER = 'cluster'


class InvalidDateRangeError(ValueError):
    """The requested estimated-spend UTC date range is invalid."""


class InvalidDrilldownScopeError(ValueError):
    """The requested spend-attribution hierarchy scope is invalid."""


def _utc_day_start(timestamp: int) -> int:
    return estimated_spend_cost.utc_day_start(timestamp)


def _utc_date_from_day_start(day_start: int) -> datetime.date:
    return datetime.datetime.fromtimestamp(day_start,
                                           tz=datetime.timezone.utc).date()


def _utc_day_start_from_date(date: datetime.date) -> int:
    return int(
        datetime.datetime.combine(date,
                                  datetime.time.min,
                                  tzinfo=datetime.timezone.utc).timestamp())


def _resolve_query_range(
    days: int,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    now: int,
) -> tuple[int, int, int]:
    """Resolve a lookback or exact UTC date range to inclusive day bounds."""
    current_day = _utc_day_start(now)
    if (start_date is None) != (end_date is None):
        raise InvalidDateRangeError(
            'start_date and end_date must be provided together')

    if start_date is None:
        requested_days = max(1, min(int(days), MAX_LOOKBACK_DAYS))
        first_day = current_day - (requested_days - 1) * SECONDS_PER_DAY
        return first_day, current_day, requested_days

    assert end_date is not None
    first_day = _utc_day_start_from_date(start_date)
    last_day = _utc_day_start_from_date(end_date)
    earliest_day = current_day - (MAX_LOOKBACK_DAYS - 1) * SECONDS_PER_DAY
    if first_day > last_day:
        raise InvalidDateRangeError('start_date must be on or before end_date')
    if last_day > current_day:
        raise InvalidDateRangeError(
            'end_date cannot be after the current UTC date')
    if first_day < earliest_day:
        raise InvalidDateRangeError(
            f'start_date must be within the last {MAX_LOOKBACK_DAYS} UTC days')

    requested_days = (last_day - first_day) // SECONDS_PER_DAY + 1
    if requested_days > MAX_LOOKBACK_DAYS:
        raise InvalidDateRangeError(
            f'date range cannot exceed {MAX_LOOKBACK_DAYS} UTC days')
    return first_day, last_day, requested_days


def _split_interval_by_utc_day(start: int, end: int) -> dict[int, int]:
    """Split a half-open epoch interval into UTC-day overlap seconds."""
    return estimated_spend_cost.split_interval_by_utc_day(start, end)


def _safe_unpickle(value: Any) -> Any:
    return estimated_spend_cost.safe_unpickle(value)


def _resource_cloud(resources: Any) -> str | None:
    return estimated_spend_cost.resource_cloud(resources)


def _get_pricing(
        resources: Any, cloud: str | None, num_nodes: int,
        rate_cache: dict[str, float]) -> tuple[float | None, str | None]:
    """Return total cluster hourly rate and an exclusion reason."""
    return estimated_spend_cost.get_pricing(resources, cloud, num_nodes,
                                            rate_cache)


def estimate_hourly_cost(
    resources: Any,
    num_nodes: int = 1,
    rate_cache: dict[str, float] | None = None
) -> tuple[float | None, str | None]:
    """Return a current-catalog hourly estimate and exclusion reason."""
    if rate_cache is None:
        rate_cache = {}
    return _get_pricing(resources, _resource_cloud(resources), num_nodes,
                        rate_cache)


def _build_daily_rows(source: Mapping[str,
                                      Any], as_of: int, recompute_start: int,
                      rate_cache: dict[str, float]) -> list[dict[str, Any]]:
    """Materialize one cluster-history row over a bounded time window."""
    return estimated_spend_cost.build_daily_rows(source, as_of, recompute_start,
                                                 rate_cache)


def _source_columns() -> list[Any]:
    table = global_user_state.cluster_history_table
    return [
        table.c.cluster_hash,
        table.c.name,
        table.c.num_nodes,
        table.c.launched_resources,
        table.c.usage_intervals,
        table.c.user_hash,
        table.c.workspace,
        table.c.last_activity_time,
        table.c.launched_at,
        table.c.cloud,
        table.c.region,
        table.c.is_managed,
        table.c.workload_type,
        table.c.workload_id,
        table.c.workload_task_id,
        table.c.usage_updated_at,
    ]


def _rows_as_mappings(rows: Iterable[Any]) -> list[Mapping[str, Any]]:
    return [row._mapping for row in rows]  # pylint: disable=protected-access


def _get_state(session: orm.Session) -> Mapping[str, Any]:
    table = global_user_state.estimated_spend_state_table
    row = session.execute(
        sqlalchemy.select(table).where(
            table.c.singleton_id == _STATE_ID)).mappings().first()
    if row is not None:
        return row
    session.execute(sqlalchemy.insert(table).values(singleton_id=_STATE_ID))
    session.commit()
    row = session.execute(
        sqlalchemy.select(table).where(
            table.c.singleton_id == _STATE_ID)).mappings().one()
    return row


def _fetch_source_rows(
    session: orm.Session,
    state: Mapping[str, Any],
    as_of: int,
    cutoff: int,
) -> tuple[list[tuple[Mapping[str, Any], int]], dict[str, Any]]:
    """Fetch active, changed, and one bounded historical backfill batch."""
    history = global_user_state.cluster_history_table
    clusters = global_user_state.cluster_table
    columns = _source_columns()
    joined = history.outerjoin(
        clusters, history.c.cluster_hash == clusters.c.cluster_hash)
    sources: dict[str, tuple[Mapping[str, Any], int]] = {}

    active_cursor_hash = state.get('active_cursor_hash')
    active_conditions = [clusters.c.status != 'STOPPED']
    if active_cursor_hash is not None:
        active_conditions.append(history.c.cluster_hash > active_cursor_hash)
    active_query = (sqlalchemy.select(*columns).select_from(joined).where(
        sqlalchemy.and_(*active_conditions)).order_by(
            history.c.cluster_hash.asc()).limit(ACTIVE_ROW_LIMIT))
    active_start = max(
        cutoff,
        _utc_day_start(as_of) - ACTIVE_RECOMPUTE_DAYS * SECONDS_PER_DAY)
    active_rows = _rows_as_mappings(session.execute(active_query).fetchall())
    for row in active_rows:
        sources[row['cluster_hash']] = (row, active_start)
    next_active_cursor_hash = (active_rows[-1]['cluster_hash'] if
                               len(active_rows) >= ACTIVE_ROW_LIMIT else None)

    watermark = int(state.get('source_watermark') or 0)
    watermark_hash = state.get('source_watermark_hash') or ''
    # Leave the current second for the next sweep. A lifecycle write committed
    # after this SELECT can otherwise carry the same integer timestamp and be
    # skipped when the watermark advances.
    changed_upper_bound = max(0, as_of - 1)
    changed_query = (sqlalchemy.select(*columns).where(
        sqlalchemy.and_(
            sqlalchemy.or_(
                history.c.usage_updated_at > watermark,
                sqlalchemy.and_(history.c.usage_updated_at == watermark,
                                history.c.cluster_hash > watermark_hash)),
            history.c.usage_updated_at <= changed_upper_bound)).order_by(
                history.c.usage_updated_at.asc(),
                history.c.cluster_hash.asc()).limit(CHANGED_BATCH_SIZE))
    changed_rows = _rows_as_mappings(session.execute(changed_query).fetchall())
    for row in changed_rows:
        existing = sources.get(row['cluster_hash'])
        recompute_start = cutoff
        if existing is None or recompute_start < existing[1]:
            sources[row['cluster_hash']] = (row, recompute_start)

    if len(changed_rows) >= CHANGED_BATCH_SIZE:
        last_updated = int(changed_rows[-1].get('usage_updated_at') or 0)
        next_watermark = last_updated
        next_watermark_hash = changed_rows[-1]['cluster_hash']
    else:
        next_watermark = changed_upper_bound
        next_watermark_hash = _HASH_END_SENTINEL

    backfill_complete = bool(state.get('backfill_complete'))
    backfill_rows: list[Mapping[str, Any]] = []
    backfill_cursor_launched_at = state.get('backfill_cursor_launched_at')
    backfill_cursor_hash = state.get('backfill_cursor_hash')
    if not backfill_complete:
        sort_time = sqlalchemy.func.coalesce(history.c.launched_at,
                                             history.c.last_activity_time, 0)
        backfill_conditions = [history.c.last_activity_time >= cutoff]
        if backfill_cursor_launched_at is not None:
            backfill_conditions.append(
                sqlalchemy.or_(
                    sort_time > backfill_cursor_launched_at,
                    sqlalchemy.and_(
                        sort_time == backfill_cursor_launched_at,
                        history.c.cluster_hash > (backfill_cursor_hash or ''))))
        backfill_query = (sqlalchemy.select(*columns).where(
            sqlalchemy.and_(*backfill_conditions)).order_by(
                sort_time.asc(),
                history.c.cluster_hash.asc()).limit(BACKFILL_BATCH_SIZE))
        backfill_rows = _rows_as_mappings(
            session.execute(backfill_query).fetchall())
        for row in backfill_rows:
            existing = sources.get(row['cluster_hash'])
            if existing is None or cutoff < existing[1]:
                sources[row['cluster_hash']] = (row, cutoff)
        if backfill_rows:
            last_backfill = backfill_rows[-1]
            cursor_time = last_backfill.get('launched_at')
            if cursor_time is None:
                cursor_time = last_backfill.get('last_activity_time')
            backfill_cursor_launched_at = int(cursor_time or 0)
            backfill_cursor_hash = last_backfill['cluster_hash']
        if len(backfill_rows) < BACKFILL_BATCH_SIZE:
            backfill_complete = True

    state_updates = {
        'source_watermark': next_watermark,
        'source_watermark_hash': next_watermark_hash,
        'active_cursor_hash': next_active_cursor_hash,
        'backfill_cursor_launched_at': backfill_cursor_launched_at,
        'backfill_cursor_hash': backfill_cursor_hash,
        'backfill_complete': backfill_complete,
        'coverage_start_utc': cutoff if backfill_complete else None,
    }
    return list(sources.values()), state_updates


def _insert_function(engine: sqlalchemy.engine.Engine):
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return sqlite.insert
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return postgresql.insert
    raise ValueError(f'Unsupported database dialect: {engine.dialect.name}')


def _persist_replacements(
    engine: sqlalchemy.engine.Engine,
    replacements: list[tuple[str, int, list[dict[str, Any]]]],
    as_of: int,
) -> None:
    table = global_user_state.estimated_spend_daily_table
    insert_func = _insert_function(engine)
    current_day = _utc_day_start(as_of)
    for offset in range(0, len(replacements), PERSIST_BATCH_SIZE):
        chunk = replacements[offset:offset + PERSIST_BATCH_SIZE]
        with orm.Session(engine) as session:
            for cluster_hash, recompute_start, rows in chunk:
                session.execute(
                    sqlalchemy.delete(table).where(
                        sqlalchemy.and_(
                            table.c.cluster_hash == cluster_hash,
                            table.c.day_start_utc
                            >= _utc_day_start(recompute_start),
                            table.c.day_start_utc <= current_day)))
                for row in rows:
                    insert_stmt = insert_func(table).values(**row)
                    update_values = {
                        column.name: getattr(insert_stmt.excluded, column.name)
                        for column in table.c
                        if column.name not in ('day_start_utc', 'cluster_hash')
                    }
                    session.execute(
                        insert_stmt.on_conflict_do_update(index_elements=[
                            table.c.day_start_utc, table.c.cluster_hash
                        ],
                                                          set_=update_values))
            session.commit()


def _prune_old_rows(engine: sqlalchemy.engine.Engine, cutoff: int) -> int:
    """Delete a bounded batch beyond the serving window."""
    table = global_user_state.estimated_spend_daily_table
    with orm.Session(engine) as session:
        keys = session.execute(
            sqlalchemy.select(table.c.day_start_utc,
                              table.c.cluster_hash).where(
                                  table.c.day_start_utc < cutoff).order_by(
                                      table.c.day_start_utc.asc()).limit(
                                          PRUNE_BATCH_SIZE)).fetchall()
        if not keys:
            return 0
        session.execute(
            sqlalchemy.delete(table).where(
                sqlalchemy.tuple_(table.c.day_start_utc,
                                  table.c.cluster_hash).in_(keys)))
        session.commit()
        return len(keys)


def _update_state(engine: sqlalchemy.engine.Engine, **values: Any) -> None:
    table = global_user_state.estimated_spend_state_table
    with orm.Session(engine) as session:
        _get_state(session)
        session.execute(
            sqlalchemy.update(table).where(
                table.c.singleton_id == _STATE_ID).values(**values))
        session.commit()


def run_rollup_once(
        now: int | None = None,
        lookback_days: int = ROLLUP_LOOKBACK_DAYS) -> dict[str, Any]:
    """Run one bounded, idempotent rollup sweep."""
    as_of = int(time.time()) if now is None else int(now)
    lookback_days = max(1, min(int(lookback_days), MAX_LOOKBACK_DAYS))
    cutoff = _utc_day_start(as_of - (lookback_days - 1) * SECONDS_PER_DAY)
    lock = locks.get_lock(ROLLUP_LOCK_ID, timeout=0)
    try:
        lock_proxy = lock.acquire(blocking=False)
    except locks.LockTimeout:
        return {'skipped': True, 'reason': 'lock-held', 'as_of': as_of}

    engine = global_user_state.initialize_and_get_db()
    with lock_proxy:
        _update_state(engine, last_started_at=as_of, last_error=None)
        try:
            with orm.Session(engine) as session:
                state = _get_state(session)
                sources, state_updates = _fetch_source_rows(
                    session, state, as_of, cutoff)

            rate_cache: dict[str, float] = {}
            replacements = []
            for source, recompute_start in sources:
                rows = _build_daily_rows(source, as_of, recompute_start,
                                         rate_cache)
                replacements.append(
                    (source['cluster_hash'], recompute_start, rows))
            _persist_replacements(engine, replacements, as_of)
            rows_pruned = _prune_old_rows(engine, cutoff)
            _update_state(engine,
                          last_success_at=as_of,
                          last_error=None,
                          **state_updates)
            return {
                'skipped': False,
                'as_of': as_of,
                'rows_processed': len(sources),
                'daily_rows_written': sum(
                    len(rows) for _, _, rows in replacements),
                'daily_rows_pruned': rows_pruned,
                'backfill_complete': state_updates['backfill_complete'],
            }
        except Exception as e:
            _update_state(engine, last_error=str(e))
            raise


async def rollup_daemon(
        refresh_interval_seconds: int = REFRESH_INTERVAL_SECONDS) -> None:
    """Refresh estimates forever without affecting API-server readiness."""
    logger.info('Starting estimated-spend rollup daemon')
    while True:
        try:
            result = await asyncio.to_thread(run_rollup_once)
            logger.debug(f'Estimated-spend rollup result: {result}')
        except Exception:  # pylint: disable=broad-except
            logger.exception('Estimated-spend rollup failed; keeping the '
                             'previous snapshot')
        await asyncio.sleep(refresh_interval_seconds)


def _sum_expression(column: Any) -> Any:
    return sqlalchemy.func.coalesce(sqlalchemy.func.sum(column), 0)


def _count_expression(column: Any | None = None) -> Any:
    if column is None:
        return sqlalchemy.func.count()  # pylint: disable=not-callable
    return sqlalchemy.func.count(column)  # pylint: disable=not-callable


def _aggregate_columns(
        daily: Any,
        zero_cost_exclusion_reasons: tuple[str, ...] = (),
) -> list[Any]:
    covered_condition = sqlalchemy.and_(daily.c.exclusion_reason.is_(None),
                                        daily.c.estimated_cost.is_not(None))
    excluded_condition = daily.c.exclusion_reason.is_not(None)
    if zero_cost_exclusion_reasons:
        known_zero_cost = daily.c.exclusion_reason.in_(
            zero_cost_exclusion_reasons)
        covered_condition = sqlalchemy.or_(covered_condition, known_zero_cost)
        excluded_condition = sqlalchemy.and_(excluded_condition,
                                             sqlalchemy.not_(known_zero_cost))
    priced_seconds = sqlalchemy.case(
        (covered_condition, daily.c.machine_seconds), else_=0)
    excluded_seconds = sqlalchemy.case(
        (excluded_condition, daily.c.machine_seconds), else_=0)
    spot_cost = sqlalchemy.case(
        (daily.c.use_spot.is_(True), daily.c.estimated_cost), else_=0)
    on_demand_cost = sqlalchemy.case(
        (daily.c.use_spot.is_(False), daily.c.estimated_cost), else_=0)
    return [
        _sum_expression(daily.c.estimated_cost).label('estimated_cost'),
        _sum_expression(spot_cost).label('spot_estimated_cost'),
        _sum_expression(on_demand_cost).label('on_demand_estimated_cost'),
        _sum_expression(priced_seconds).label('priced_machine_seconds'),
        _sum_expression(excluded_seconds).label('excluded_machine_seconds'),
    ]


def _purchase_option_expression(daily: Any) -> Any:
    return sqlalchemy.case((daily.c.use_spot.is_(True), 'spot'),
                           (daily.c.use_spot.is_(False), 'on_demand'),
                           else_='unknown')


def _row_to_breakdown(row: Any, key_names: tuple[str, ...]) -> dict[str, Any]:
    result = {name: getattr(row, name) for name in key_names}
    result.update({
        'estimated_cost': float(row.estimated_cost or 0),
        'spot_estimated_cost': float(row.spot_estimated_cost or 0),
        'on_demand_estimated_cost': float(row.on_demand_estimated_cost or 0),
        'priced_machine_seconds': int(row.priced_machine_seconds or 0),
        'excluded_machine_seconds': int(row.excluded_machine_seconds or 0),
    })
    return result


def _workload_type_expression(daily: Any) -> Any:
    """Return the evidenced logical workload type for dashboard grouping."""
    return sqlalchemy.case(
        (daily.c.workload_type == 'managed', 'managed_unattributed'),
        else_=daily.c.workload_type)


def _workload_id_expression(daily: Any) -> Any:
    """Hide cluster-derived IDs when a legacy managed parent is unproven."""
    return sqlalchemy.case((daily.c.workload_type == 'managed', None),
                           else_=daily.c.workload_id)


def _workload_key_expression(daily: Any) -> Any:
    workload_type = sqlalchemy.func.coalesce(_workload_type_expression(daily),
                                             'cluster')
    workload_id = sqlalchemy.func.coalesce(_workload_id_expression(daily), '')
    return workload_type + '\x1f' + workload_id


def _owner_scope_condition(daily: Any, owner_user_hash: str | None,
                           owner_unknown: bool) -> Any:
    if owner_unknown:
        return daily.c.user_hash.is_(None)
    assert owner_user_hash is not None
    return daily.c.user_hash == owner_user_hash


def _workload_scope_condition(daily: Any, workload_type: str,
                              workload_id: str | None) -> Any:
    if workload_type == 'managed_unattributed':
        if workload_id is not None:
            raise InvalidDrilldownScopeError(
                'managed_unattributed must not include workload_id')
        return daily.c.workload_type == 'managed'
    workload_id_condition = (daily.c.workload_id.is_(None) if workload_id
                             is None else daily.c.workload_id == workload_id)
    return sqlalchemy.and_(daily.c.workload_type == workload_type,
                           workload_id_condition)


def _validate_drilldown_scope(
    level: SpendDrilldownLevel,
    owner_user_hash: str | None,
    owner_unknown: bool,
    workload_type: str | None,
    workload_id: str | None,
    workload_task_id: int | None,
    offset: int,
    limit: int,
) -> None:
    if offset < 0:
        raise InvalidDrilldownScopeError('offset must be non-negative')
    if limit < 1 or limit > DRILLDOWN_MAX_LIMIT:
        raise InvalidDrilldownScopeError(
            f'limit must be between 1 and {DRILLDOWN_MAX_LIMIT}')

    has_owner_scope = owner_user_hash is not None or owner_unknown
    if owner_user_hash is not None and owner_unknown:
        raise InvalidDrilldownScopeError(
            'provide owner_user_hash or owner_unknown, not both')
    if level == SpendDrilldownLevel.OWNER:
        if (has_owner_scope or workload_type is not None or
                workload_id is not None or workload_task_id is not None):
            raise InvalidDrilldownScopeError(
                'owner level does not accept a parent scope')
        return

    if not has_owner_scope:
        raise InvalidDrilldownScopeError(
            'descendant levels require owner_user_hash or owner_unknown')
    if level == SpendDrilldownLevel.WORKLOAD:
        if (workload_type is not None or workload_id is not None or
                workload_task_id is not None):
            raise InvalidDrilldownScopeError(
                'workload level accepts only an owner scope')
        return

    if workload_type is None:
        raise InvalidDrilldownScopeError(
            'task and cluster levels require workload_type')
    if (workload_type != 'managed_unattributed' and workload_id is None):
        raise InvalidDrilldownScopeError(
            'task and cluster levels require workload_id')
    if level == SpendDrilldownLevel.TASK:
        if workload_task_id is not None:
            raise InvalidDrilldownScopeError(
                'task level does not accept workload_task_id')
        if workload_type == 'managed_unattributed':
            raise InvalidDrilldownScopeError(
                'legacy managed workloads do not have evidenced task parents')


def _paginated_group_rows(session: orm.Session, query: Any, offset: int,
                          limit: int) -> tuple[list[Any], int]:
    count_query = sqlalchemy.select(_count_expression()).select_from(
        query.order_by(None).subquery())
    total = int(session.scalar(count_query) or 0)
    rows = session.execute(query.offset(offset).limit(limit)).fetchall()
    return rows, total


def _drilldown_row(row: Any, key_names: tuple[str, ...],
                   extra_names: tuple[str, ...]) -> dict[str, Any]:
    result = _row_to_breakdown(row, key_names)
    for name in extra_names:
        value = getattr(row, name)
        if name.endswith('_count'):
            value = int(value or 0)
        result[name] = value
    return result


def get_estimated_spend_drilldown(
    level: str | SpendDrilldownLevel,
    days: int = DEFAULT_LOOKBACK_DAYS,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    owner_user_hash: str | None = None,
    owner_unknown: bool = False,
    workload_type: str | None = None,
    workload_id: str | None = None,
    workload_task_id: int | None = None,
    offset: int = 0,
    limit: int = DRILLDOWN_DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Read one bounded page of the spend-attribution hierarchy."""
    try:
        normalized_level = SpendDrilldownLevel(level)
    except ValueError as e:
        options = ', '.join(option.value for option in SpendDrilldownLevel)
        raise InvalidDrilldownScopeError(
            f'level must be one of: {options}') from e
    _validate_drilldown_scope(normalized_level, owner_user_hash, owner_unknown,
                              workload_type, workload_id, workload_task_id,
                              offset, limit)

    now = int(time.time())
    first_day, last_day, requested_days = _resolve_query_range(
        days, start_date, end_date, now)
    daily = global_user_state.estimated_spend_daily_table
    base_filter = sqlalchemy.and_(
        daily.c.day_start_utc >= first_day, daily.c.day_start_utc
        < last_day + SECONDS_PER_DAY)
    engine = global_user_state.initialize_and_get_db()

    with orm.Session(engine) as session:
        if normalized_level == SpendDrilldownLevel.OWNER:
            users = global_user_state.user_table
            workload_count = _count_expression(
                sqlalchemy.distinct(
                    _workload_key_expression(daily))).label('workload_count')
            cluster_count = _count_expression(
                sqlalchemy.distinct(
                    daily.c.cluster_hash)).label('cluster_count')
            query = (sqlalchemy.select(
                daily.c.user_hash.label('user_hash'),
                users.c.name.label('user_name'),
                workload_count,
                cluster_count,
                *_aggregate_columns(daily),
            ).select_from(
                daily.outerjoin(users, daily.c.user_hash == users.c.id)).where(
                    base_filter).group_by(
                        daily.c.user_hash, users.c.name).order_by(
                            sqlalchemy.desc('estimated_cost'),
                            sqlalchemy.func.coalesce(users.c.name, ''),
                            sqlalchemy.func.coalesce(daily.c.user_hash, '')))
            rows, total = _paginated_group_rows(session, query, offset, limit)
            response_rows = []
            for row in rows:
                result = _drilldown_row(row, ('user_hash', 'user_name'),
                                        ('workload_count', 'cluster_count'))
                result['owner_unknown'] = row.user_hash is None
                response_rows.append(result)
        else:
            assert owner_user_hash is not None or owner_unknown
            scoped_filter = sqlalchemy.and_(
                base_filter,
                _owner_scope_condition(daily, owner_user_hash, owner_unknown))
            if normalized_level == SpendDrilldownLevel.WORKLOAD:
                displayed_type = _workload_type_expression(daily)
                displayed_id = _workload_id_expression(daily)
                unknown_task_cluster = sqlalchemy.case(
                    (daily.c.workload_task_id.is_(None), daily.c.cluster_hash),
                    else_=None)
                query = (sqlalchemy.select(
                    displayed_type.label('workload_type'),
                    displayed_id.label('workload_id'),
                    _count_expression(
                        sqlalchemy.distinct(
                            daily.c.workload_task_id)).label('task_count'),
                    _count_expression(
                        sqlalchemy.distinct(unknown_task_cluster)).label(
                            'unknown_task_cluster_count'),
                    _count_expression(sqlalchemy.distinct(
                        daily.c.cluster_hash)).label('cluster_count'),
                    *_aggregate_columns(daily),
                ).where(scoped_filter).group_by(
                    displayed_type, displayed_id).order_by(
                        sqlalchemy.desc('estimated_cost'), displayed_type,
                        sqlalchemy.func.coalesce(displayed_id, '')))
                rows, total = _paginated_group_rows(session, query, offset,
                                                    limit)
                response_rows = [
                    _drilldown_row(row, ('workload_type', 'workload_id'),
                                   ('task_count', 'unknown_task_cluster_count',
                                    'cluster_count')) for row in rows
                ]
            else:
                assert workload_type is not None
                scoped_filter = sqlalchemy.and_(
                    scoped_filter,
                    _workload_scope_condition(daily, workload_type,
                                              workload_id))
                if normalized_level == SpendDrilldownLevel.TASK:
                    scoped_filter = sqlalchemy.and_(
                        scoped_filter, daily.c.workload_task_id.is_not(None))
                    query = (sqlalchemy.select(
                        daily.c.workload_task_id.label('workload_task_id'),
                        _count_expression(
                            sqlalchemy.distinct(
                                daily.c.cluster_hash)).label('cluster_count'),
                        *_aggregate_columns(daily),
                    ).where(scoped_filter).group_by(
                        daily.c.workload_task_id).order_by(
                            daily.c.workload_task_id.asc()))
                    rows, total = _paginated_group_rows(session, query, offset,
                                                        limit)
                    response_rows = [
                        _drilldown_row(row, ('workload_task_id',),
                                       ('cluster_count',)) for row in rows
                    ]
                else:
                    if workload_task_id is not None:
                        scoped_filter = sqlalchemy.and_(
                            scoped_filter,
                            daily.c.workload_task_id == workload_task_id)
                    query = (sqlalchemy.select(
                        daily.c.cluster_hash.label('cluster_hash'),
                        daily.c.cluster_name.label('cluster_name'),
                        daily.c.workspace.label('workspace'),
                        *_aggregate_columns(daily),
                    ).where(scoped_filter).group_by(
                        daily.c.cluster_hash, daily.c.cluster_name,
                        daily.c.workspace).order_by(
                            sqlalchemy.desc('estimated_cost'),
                            daily.c.cluster_name.asc(),
                            daily.c.cluster_hash.asc()))
                    rows, total = _paginated_group_rows(session, query, offset,
                                                        limit)
                    response_rows = [
                        _drilldown_row(
                            row, ('cluster_hash', 'cluster_name', 'workspace'),
                            ()) for row in rows
                    ]

    return {
        'level': normalized_level.value,
        'start_date': _utc_date_from_day_start(first_day).isoformat(),
        'end_date': _utc_date_from_day_start(last_day).isoformat(),
        'requested_days': requested_days,
        'rows': response_rows,
        'total': total,
        'offset': offset,
        'limit': limit,
        'has_more': offset + len(response_rows) < total,
    }


def _normalize_group_by(group_by: str | GroupBy) -> GroupBy:
    try:
        return GroupBy(group_by)
    except ValueError as e:
        options = ', '.join(option.value for option in GroupBy)
        raise ValueError(f'group_by must be one of: {options}') from e


def _group_key(group_by: GroupBy, row: Any) -> tuple[Any, ...]:
    if group_by == GroupBy.JOB:
        return (row.workload_type, row.workload_id)
    if group_by == GroupBy.USER:
        return (row.user_hash,)
    return (row.purchase_option,)


def _group_key_names(group_by: GroupBy) -> tuple[str, ...]:
    if group_by == GroupBy.JOB:
        return ('workload_type', 'workload_id')
    if group_by == GroupBy.USER:
        return ('user_hash', 'user_name')
    return ('purchase_option',)


def _get_group_rows(session: orm.Session, daily: Any, base_filter: Any,
                    workload_rows: list[Any], group_by: GroupBy) -> list[Any]:
    if group_by == GroupBy.JOB:
        return workload_rows

    if group_by == GroupBy.USER:
        users = global_user_state.user_table
        query = (sqlalchemy.select(
            daily.c.user_hash.label('user_hash'),
            users.c.name.label('user_name'),
            *_aggregate_columns(daily),
        ).select_from(daily.outerjoin(
            users,
            daily.c.user_hash == users.c.id)).where(base_filter).group_by(
                daily.c.user_hash, users.c.name).order_by(
                    sqlalchemy.desc('estimated_cost')).limit(GROUP_TABLE_LIMIT))
        return session.execute(query).fetchall()

    purchase_option = _purchase_option_expression(daily)
    query = (sqlalchemy.select(
        purchase_option.label('purchase_option'),
        *_aggregate_columns(daily),
    ).where(base_filter).group_by(purchase_option).order_by(
        sqlalchemy.desc('estimated_cost')))
    return session.execute(query).fetchall()


def _group_match_condition(daily: Any, group_by: GroupBy, row: Any) -> Any:
    if group_by == GroupBy.JOB:
        if row.workload_type == 'managed_unattributed':
            return daily.c.workload_type == 'managed'
        workload_id = row.workload_id
        workload_id_condition = (daily.c.workload_id.is_(None)
                                 if workload_id is None else daily.c.workload_id
                                 == workload_id)
        return sqlalchemy.and_(daily.c.workload_type == row.workload_type,
                               workload_id_condition)
    if group_by == GroupBy.USER:
        return (daily.c.user_hash.is_(None) if row.user_hash is None else
                daily.c.user_hash == row.user_hash)
    purchase_option = _purchase_option_expression(daily)
    return purchase_option == row.purchase_option


def _get_daily_group_rows(session: orm.Session, daily: Any, base_filter: Any,
                          group_by: GroupBy,
                          top_group_rows: list[Any]) -> list[Any]:
    if not top_group_rows:
        return []
    match_conditions = [
        _group_match_condition(daily, group_by, row) for row in top_group_rows
    ]
    if group_by == GroupBy.JOB:
        displayed_workload_type = _workload_type_expression(daily)
        displayed_workload_id = _workload_id_expression(daily)
        key_columns = [
            displayed_workload_type.label('workload_type'),
            displayed_workload_id.label('workload_id')
        ]
        group_columns = key_columns
    elif group_by == GroupBy.USER:
        key_columns = [daily.c.user_hash]
        group_columns = key_columns
    else:
        purchase_option = _purchase_option_expression(daily)
        key_columns = [purchase_option.label('purchase_option')]
        group_columns = [purchase_option]
    query = (sqlalchemy.select(
        daily.c.day_start_utc.label('day_start_utc'), *key_columns,
        _sum_expression(daily.c.estimated_cost).label('estimated_cost')).where(
            sqlalchemy.and_(base_filter,
                            sqlalchemy.or_(*match_conditions))).group_by(
                                daily.c.day_start_utc, *group_columns).order_by(
                                    daily.c.day_start_utc.asc()))
    return session.execute(query).fetchall()


def _build_series(group_by: GroupBy, top_group_rows: list[Any],
                  daily_group_rows: list[Any],
                  days_response: list[dict[str, Any]]) -> list[dict[str, Any]]:
    costs_by_group_and_day = {
        (_group_key(group_by, row), int(row.day_start_utc)): float(
            row.estimated_cost or 0) for row in daily_group_rows
    }
    displayed_by_day = {int(day['day_start_utc']): 0.0 for day in days_response}
    series = []
    for row in top_group_rows:
        key = _group_key(group_by, row)
        costs = []
        for day in days_response:
            day_start = int(day['day_start_utc'])
            cost = costs_by_group_and_day.get((key, day_start), 0.0)
            costs.append(cost)
            displayed_by_day[day_start] += cost
        identity = {
            name: getattr(row, name) for name in _group_key_names(group_by)
        }
        series.append({**identity, 'estimated_cost_by_day': costs})

    other_costs = []
    for day in days_response:
        day_start = int(day['day_start_utc'])
        other_costs.append(
            max(0.0,
                float(day['estimated_cost']) - displayed_by_day[day_start]))
    if any(cost > 1e-9 for cost in other_costs):
        series.append({
            'is_other': True,
            'estimated_cost_by_day': other_costs,
        })
    return series


def _get_service_request_and_cost_rows(
    session: orm.Session,
    spend_daily: Any,
    spend_filter: Any,
    first_day: int,
    last_day: int,
    service_names: list[str],
) -> tuple[list[Any], list[Any]]:
    """Read bounded daily request and cost rows for displayed services."""
    if not service_names:
        return [], []

    request_daily = serve_history.serve_request_activity_daily_table
    first_day_datetime = datetime.datetime.fromtimestamp(
        first_day, datetime.timezone.utc)
    end_exclusive_datetime = datetime.datetime.fromtimestamp(
        last_day + SECONDS_PER_DAY, datetime.timezone.utc)
    classification_supported = sqlalchemy.and_(
        request_daily.c.classified_request_count.is_not(None),
        request_daily.c.counted_rejected_count.is_not(None))
    classified_count = sqlalchemy.func.coalesce(
        sqlalchemy.func.sum(request_daily.c.classified_request_count).filter(
            classification_supported), 0)
    counted_rejected_count = sqlalchemy.func.coalesce(
        sqlalchemy.func.sum(request_daily.c.counted_rejected_count).filter(
            classification_supported), 0)
    request_rows = session.execute(
        sqlalchemy.select(
            request_daily.c.day_start,
            request_daily.c.service_name,
            (classified_count - counted_rejected_count).label('request_count'),
        ).where(
            sqlalchemy.and_(
                request_daily.c.day_start >= first_day_datetime,
                request_daily.c.day_start < end_exclusive_datetime,
                request_daily.c.service_name.in_(service_names),
            )).group_by(request_daily.c.day_start,
                        request_daily.c.service_name)).fetchall()

    cost_rows = session.execute(
        sqlalchemy.select(
            spend_daily.c.day_start_utc,
            spend_daily.c.workload_id.label('service_name'),
            *_aggregate_columns(spend_daily,
                                zero_cost_exclusion_reasons=(
                                    _SERVICE_ZERO_COST_EXCLUSION_REASONS)),
        ).where(
            sqlalchemy.and_(
                spend_filter,
                spend_daily.c.workload_type == 'service',
                spend_daily.c.workload_id.in_(service_names),
            )).group_by(spend_daily.c.day_start_utc,
                        spend_daily.c.workload_id)).fetchall()
    return request_rows, cost_rows


def _first_complete_coverage_day(coverage_start_utc: int | None) -> int | None:
    """Return the first UTC day wholly covered by a history source."""
    if coverage_start_utc is None:
        return None
    day_start = _utc_day_start(coverage_start_utc)
    if coverage_start_utc > day_start:
        day_start += SECONDS_PER_DAY
    return day_start


def _enrich_service_requests_with_costs(
    service_requests: dict[str, Any],
    days: list[dict[str, Any]],
    request_rows: list[Any],
    cost_rows: list[Any],
    spend_coverage_start_utc: int | None,
) -> None:
    """Attach aligned service compute cost and cost-per-request estimates."""
    if not service_requests.get('available'):
        return

    day_starts = [int(day['day_start_utc']) for day in days]
    request_counts = {
        (str(row.service_name), int(row.day_start.timestamp())): int(
            row.request_count or 0) for row in request_rows
    }
    costs = {
        (str(row.service_name), int(row.day_start_utc)): {
            'estimated_cost': float(row.estimated_cost or 0),
            'priced_machine_seconds': int(row.priced_machine_seconds or 0),
            'excluded_machine_seconds': int(row.excluded_machine_seconds or 0),
        } for row in cost_rows
    }
    spend_coverage_start = _first_complete_coverage_day(
        spend_coverage_start_utc)

    services_by_name = {
        str(service['service_name']): service
        for service in service_requests.get('services', [])
    }
    for service_name, service in services_by_name.items():
        complete_by_day = service.get('complete_by_day')
        if (not isinstance(complete_by_day, list) or
                len(complete_by_day) != len(day_starts)):
            complete_by_day = [False for _ in day_starts]
        included_days = [
            day_start
            for day_start, complete in zip(day_starts, complete_by_day)
            if complete and spend_coverage_start is not None and
            day_start >= spend_coverage_start
        ]
        ratio_coverage_start = min(included_days) if included_days else None
        ratio_request_count = 0
        estimated_cost = 0.0
        priced_machine_seconds = 0
        excluded_machine_seconds = 0
        for day_start, complete in zip(day_starts, complete_by_day):
            if (not complete or spend_coverage_start is None or
                    day_start < spend_coverage_start):
                continue
            ratio_request_count += request_counts.get((service_name, day_start),
                                                      0)
            day_cost = costs.get((service_name, day_start), {})
            estimated_cost += float(day_cost.get('estimated_cost', 0))
            priced_machine_seconds += int(
                day_cost.get('priced_machine_seconds', 0))
            excluded_machine_seconds += int(
                day_cost.get('excluded_machine_seconds', 0))

        if excluded_machine_seconds > 0:
            cost_coverage = 'partial'
        elif (ratio_coverage_start is None or ratio_request_count <= 0 or
              priced_machine_seconds <= 0):
            cost_coverage = 'unavailable'
        else:
            cost_coverage = 'complete'
        estimated_cost_per_request = None
        if cost_coverage == 'complete':
            estimated_cost_per_request = (estimated_cost / ratio_request_count)
        service.update({
            'estimated_cost': estimated_cost,
            'estimated_cost_per_request': estimated_cost_per_request,
            'ratio_request_count': ratio_request_count,
            'ratio_coverage_start_utc': ratio_coverage_start,
            'priced_machine_seconds': priced_machine_seconds,
            'excluded_machine_seconds': excluded_machine_seconds,
            'cost_coverage': cost_coverage,
        })

    for series in service_requests.get('series', []):
        if series.get('is_other'):
            continue
        service_name = str(series['service_name'])
        service = services_by_name.get(service_name, {})
        complete_by_day = service.get('complete_by_day')
        if (not isinstance(complete_by_day, list) or
                len(complete_by_day) != len(day_starts)):
            complete_by_day = [False for _ in day_starts]
        daily_costs = []
        daily_ratios = []
        for index, day_start in enumerate(day_starts):
            request_count = request_counts.get((service_name, day_start), 0)
            day_cost = costs.get((service_name, day_start), {})
            estimated_cost = float(day_cost.get('estimated_cost', 0))
            priced_seconds = int(day_cost.get('priced_machine_seconds', 0))
            excluded_seconds = int(day_cost.get('excluded_machine_seconds', 0))
            daily_costs.append(estimated_cost)
            ratio_available = (bool(complete_by_day[index]) and
                               spend_coverage_start is not None and
                               day_start >= spend_coverage_start and
                               request_count > 0 and priced_seconds > 0 and
                               excluded_seconds == 0)
            daily_ratios.append(estimated_cost /
                                request_count if ratio_available else None)
        series['estimated_cost_by_day'] = daily_costs
        series['estimated_cost_per_request_by_day'] = daily_ratios


def _mark_legacy_service_request_costs_unavailable(service_requests: dict[str,
                                                                          Any],
                                                   day_count: int) -> None:
    """Keep attempt counts compatible without exposing an invalid ratio."""
    for service in service_requests.get('services', []):
        service.update({
            'estimated_cost': 0.0,
            'estimated_cost_per_request': None,
            'ratio_request_count': 0,
            'ratio_coverage_start_utc': None,
            'priced_machine_seconds': 0,
            'excluded_machine_seconds': 0,
            'cost_coverage': 'unavailable',
        })
    for series in service_requests.get('series', []):
        if series.get('is_other'):
            continue
        series['estimated_cost_by_day'] = [0.0 for _ in range(day_count)]
        series['estimated_cost_per_request_by_day'] = [
            None for _ in range(day_count)
        ]


def get_estimated_spend(
    days: int = DEFAULT_LOOKBACK_DAYS,
    group_by: str | GroupBy = GroupBy.JOB,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, Any]:
    """Read the last materialized admin estimate using aggregate SQL only."""
    normalized_group_by = _normalize_group_by(group_by)
    now = int(time.time())
    first_day, last_day, requested_days = _resolve_query_range(
        days, start_date, end_date, now)
    daily = global_user_state.estimated_spend_daily_table
    state_table = global_user_state.estimated_spend_state_table

    engine = global_user_state.initialize_and_get_db()
    with orm.Session(engine) as session:
        state = session.execute(
            sqlalchemy.select(state_table).where(
                state_table.c.singleton_id == _STATE_ID)).mappings().first()

        base_filter = sqlalchemy.and_(
            daily.c.day_start_utc >= first_day, daily.c.day_start_utc
            < last_day + SECONDS_PER_DAY)
        displayed_workload_type = _workload_type_expression(daily)
        displayed_workload_id = _workload_id_expression(daily)
        day_rows = session.execute(
            sqlalchemy.select(
                daily.c.day_start_utc.label('day_start_utc'),
                *_aggregate_columns(daily),
            ).where(base_filter).group_by(daily.c.day_start_utc).order_by(
                daily.c.day_start_utc.asc())).fetchall()

        workload_rows = session.execute(
            sqlalchemy.select(
                displayed_workload_type.label('workload_type'),
                displayed_workload_id.label('workload_id'),
                *_aggregate_columns(daily),
            ).where(base_filter).group_by(
                displayed_workload_type, displayed_workload_id).order_by(
                    sqlalchemy.desc('estimated_cost'), displayed_workload_type,
                    sqlalchemy.func.coalesce(
                        displayed_workload_id,
                        '')).limit(GROUP_TABLE_LIMIT)).fetchall()

        cloud_rows = session.execute(
            sqlalchemy.select(
                daily.c.cloud.label('cloud'),
                *_aggregate_columns(daily),
            ).where(base_filter).group_by(daily.c.cloud).order_by(
                sqlalchemy.desc('estimated_cost'))).fetchall()

        reason_rows = session.execute(
            sqlalchemy.select(
                daily.c.exclusion_reason,
                _sum_expression(
                    daily.c.machine_seconds).label('machine_seconds')).where(
                        sqlalchemy.and_(
                            base_filter,
                            daily.c.exclusion_reason.is_not(None))).group_by(
                                daily.c.exclusion_reason)).fetchall()

        group_rows = _get_group_rows(session, daily, base_filter, workload_rows,
                                     normalized_group_by)
        top_group_rows = [
            row for row in group_rows if float(row.estimated_cost or 0) > 0
        ][:GROUP_CHART_LIMIT]
        daily_group_rows = _get_daily_group_rows(session, daily, base_filter,
                                                 normalized_group_by,
                                                 top_group_rows)

    by_day = {
        int(row.day_start_utc): {
            'estimated_cost': float(row.estimated_cost or 0),
            'spot_estimated_cost': float(row.spot_estimated_cost or 0),
            'on_demand_estimated_cost': float(row.on_demand_estimated_cost or
                                              0),
            'priced_machine_seconds': int(row.priced_machine_seconds or 0),
            'excluded_machine_seconds': int(row.excluded_machine_seconds or 0),
        } for row in day_rows
    }
    days_response: list[dict[str, Any]] = []
    for offset in range(requested_days):
        day_start = first_day + offset * SECONDS_PER_DAY
        values = by_day.get(
            day_start, {
                'estimated_cost': 0.0,
                'spot_estimated_cost': 0.0,
                'on_demand_estimated_cost': 0.0,
                'priced_machine_seconds': 0,
                'excluded_machine_seconds': 0,
            })
        days_response.append({
            'date': _utc_date_from_day_start(day_start).isoformat(),
            'day_start_utc': day_start,
            **values,
        })

    last_success = int(state['last_success_at'] or 0) if state else 0
    backfill_complete = bool(state['backfill_complete']) if state else False
    spend_coverage_start = (int(state['coverage_start_utc']) if state and
                            state['coverage_start_utc'] is not None else None)
    total_cost = sum(day['estimated_cost'] for day in days_response)
    total_priced_seconds = sum(
        day['priced_machine_seconds'] for day in days_response)
    total_excluded_seconds = sum(
        day['excluded_machine_seconds'] for day in days_response)
    service_requests = serve_history.get_daily_request_summary(
        engine=engine,
        first_day_start=first_day,
        last_day_start=last_day,
        days=days_response,
        table_limit=GROUP_TABLE_LIMIT,
        chart_limit=GROUP_CHART_LIMIT,
    )
    _mark_legacy_service_request_costs_unavailable(service_requests,
                                                   len(days_response))
    non_rejected_requests = service_requests.get('non_rejected')
    if (isinstance(non_rejected_requests, dict) and
            non_rejected_requests.get('available')):
        service_names = [
            str(service['service_name'])
            for service in non_rejected_requests.get('services', [])
        ]
        request_rows: list[Any] = []
        service_cost_rows: list[Any] = []
        try:
            with orm.Session(engine) as session:
                request_rows, service_cost_rows = (
                    _get_service_request_and_cost_rows(session, daily,
                                                       base_filter, first_day,
                                                       last_day, service_names))
        except sqlalchemy.exc.SQLAlchemyError:
            # Keep the independently useful request totals available during a
            # rolling upgrade or transient best-effort spend read failure.
            logger.exception('Failed to read daily service request costs.')
        _enrich_service_requests_with_costs(non_rejected_requests,
                                            days_response, request_rows,
                                            service_cost_rows,
                                            spend_coverage_start)
    return {
        'currency': 'USD',
        'basis': 'skypilot_catalog_payg_equivalent',
        'as_of': last_success or None,
        'last_successful_refresh_at': last_success or None,
        'stale': (not last_success or
                  now - last_success > REFRESH_INTERVAL_SECONDS * 2),
        'backfill_complete': backfill_complete,
        'coverage_start_utc': spend_coverage_start,
        'kubernetes_included': False,
        'reservation_adjustments_applied': False,
        'start_date': _utc_date_from_day_start(first_day).isoformat(),
        'end_date': _utc_date_from_day_start(last_day).isoformat(),
        'requested_days': requested_days,
        'totals': {
            'estimated_cost': total_cost,
            'priced_machine_seconds': total_priced_seconds,
            'excluded_machine_seconds': total_excluded_seconds,
        },
        'days': days_response,
        'workloads': [
            _row_to_breakdown(row, ('workload_type', 'workload_id'))
            for row in workload_rows
        ],
        'clouds': [_row_to_breakdown(row, ('cloud',)) for row in cloud_rows],
        'group_by': normalized_group_by.value,
        'groups': [
            _row_to_breakdown(row, _group_key_names(normalized_group_by))
            for row in group_rows
        ],
        'series': _build_series(normalized_group_by, top_group_rows,
                                daily_group_rows, days_response),
        'service_requests': service_requests,
        'excluded_by_reason': {
            str(row.exclusion_reason): int(row.machine_seconds or 0)
            for row in reason_rows
        },
        'last_error': state['last_error'] if state else None,
    }
