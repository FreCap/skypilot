"""Eventually consistent, best-effort compute spend estimates."""

import asyncio
import datetime
import enum
import math
import pickle
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import global_user_state
from sky import sky_logging
from sky.utils import locks
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

SECONDS_PER_DAY = 24 * 60 * 60
SECONDS_PER_HOUR = 60 * 60
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


class GroupBy(str, enum.Enum):
    """Supported estimated-spend dashboard breakdowns."""

    JOB = 'job'
    USER = 'user'
    PURCHASE_OPTION = 'purchase_option'


def _utc_day_start(timestamp: int) -> int:
    return int(timestamp) // SECONDS_PER_DAY * SECONDS_PER_DAY


def _split_interval_by_utc_day(start: int, end: int) -> Dict[int, int]:
    """Split a half-open epoch interval into UTC-day overlap seconds."""
    if end <= start:
        return {}
    overlaps: Dict[int, int] = {}
    cursor = start
    while cursor < end:
        day_start = _utc_day_start(cursor)
        next_day = day_start + SECONDS_PER_DAY
        overlap_end = min(end, next_day)
        overlaps[day_start] = overlaps.get(day_start, 0) + overlap_end - cursor
        cursor = overlap_end
    return overlaps


def _safe_unpickle(value: Any) -> Any:
    if value is None:
        return None
    try:
        return pickle.loads(value)
    # Legacy resource classes may move or disappear between server versions,
    # and custom reducers can raise exceptions outside pickle.PickleError. A
    # single history row must not prevent the durable watermark from advancing.
    except Exception:  # pylint: disable=broad-except
        return None


def _resource_cloud(resources: Any) -> Optional[str]:
    if resources is None:
        return None
    cloud = getattr(resources, 'cloud', None)
    return str(cloud) if cloud is not None else None


def _get_pricing(
        resources: Any, cloud: Optional[str], num_nodes: int,
        rate_cache: Dict[str, float]) -> Tuple[Optional[float], Optional[str]]:
    """Return total cluster hourly rate and an exclusion reason."""
    if cloud is not None and cloud.casefold() == 'kubernetes':
        return None, 'kubernetes'
    if resources is None:
        return None, 'unknown_price'
    cache_key = f'{resources!r}|nodes={num_nodes}'
    if cache_key in rate_cache:
        return rate_cache[cache_key], None
    try:
        hourly_rate = float(resources.get_cost(SECONDS_PER_HOUR) * num_nodes)
    except Exception:  # pylint: disable=broad-except
        return None, 'unknown_price'
    if not math.isfinite(hourly_rate) or hourly_rate < 0:
        return None, 'unknown_price'
    rate_cache[cache_key] = hourly_rate
    return hourly_rate, None


def _build_daily_rows(source: Mapping[str,
                                      Any], as_of: int, recompute_start: int,
                      rate_cache: Dict[str, float]) -> List[Dict[str, Any]]:
    """Materialize one cluster-history row over a bounded time window."""
    usage_intervals = _safe_unpickle(source.get('usage_intervals'))
    if not isinstance(usage_intervals, list):
        return []

    resources = _safe_unpickle(source.get('launched_resources'))
    try:
        num_nodes = int(source.get('num_nodes') or 1)
    except (TypeError, ValueError):
        num_nodes = 1
    if num_nodes <= 0:
        num_nodes = 1

    cloud = source.get('cloud') or _resource_cloud(resources)
    hourly_rate, exclusion_reason = _get_pricing(resources, cloud, num_nodes,
                                                 rate_cache)
    use_spot = (bool(getattr(resources, 'use_spot', False))
                if resources is not None else None)

    overlap_by_day: Dict[int, int] = {}
    for interval in usage_intervals:
        if not isinstance(interval, (tuple, list)) or len(interval) != 2:
            continue
        raw_start, raw_end = interval
        try:
            start = int(raw_start)
            end = as_of if raw_end is None else min(int(raw_end), as_of)
        except (TypeError, ValueError):
            continue
        start = max(start, recompute_start)
        for day_start, seconds in _split_interval_by_utc_day(start,
                                                             end).items():
            overlap_by_day[day_start] = overlap_by_day.get(day_start,
                                                           0) + seconds

    workload_type = source.get('workload_type')
    if not workload_type:
        workload_type = ('managed' if source.get('is_managed') else 'cluster')
    workload_id = source.get('workload_id') or source.get('name')
    rows = []
    for day_start, cluster_seconds in overlap_by_day.items():
        estimated_cost = None
        if hourly_rate is not None:
            estimated_cost = (hourly_rate * cluster_seconds / SECONDS_PER_HOUR)
        rows.append({
            'day_start_utc': day_start,
            'cluster_hash': source['cluster_hash'],
            'cluster_name': source['name'],
            'workload_type': workload_type,
            'workload_id': str(workload_id)
                           if workload_id is not None else None,
            'workload_task_id': source.get('workload_task_id'),
            'user_hash': source.get('user_hash'),
            'workspace': source.get('workspace'),
            'cloud': cloud,
            'region': source.get('region'),
            'use_spot': use_spot,
            'num_nodes': num_nodes,
            'machine_seconds': cluster_seconds * num_nodes,
            'catalog_hourly_rate': hourly_rate,
            'estimated_cost': estimated_cost,
            'exclusion_reason': exclusion_reason,
            'priced_at': as_of if hourly_rate is not None else None,
            'updated_at': as_of,
        })
    return rows


def _source_columns() -> List[Any]:
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


def _rows_as_mappings(rows: Iterable[Any]) -> List[Mapping[str, Any]]:
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
) -> Tuple[List[Tuple[Mapping[str, Any], int]], Dict[str, Any]]:
    """Fetch active, changed, and one bounded historical backfill batch."""
    history = global_user_state.cluster_history_table
    clusters = global_user_state.cluster_table
    columns = _source_columns()
    joined = history.outerjoin(
        clusters, history.c.cluster_hash == clusters.c.cluster_hash)
    sources: Dict[str, Tuple[Mapping[str, Any], int]] = {}

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
    backfill_rows: List[Mapping[str, Any]] = []
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
    replacements: List[Tuple[str, int, List[Dict[str, Any]]]],
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
                            table.c.day_start_utc >=
                            _utc_day_start(recompute_start),
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
        now: Optional[int] = None,
        lookback_days: int = ROLLUP_LOOKBACK_DAYS) -> Dict[str, Any]:
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

            rate_cache: Dict[str, float] = {}
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


def _aggregate_columns(daily: Any) -> List[Any]:
    priced_seconds = sqlalchemy.case((sqlalchemy.and_(
        daily.c.exclusion_reason.is_(None),
        daily.c.estimated_cost.is_not(None)), daily.c.machine_seconds),
                                     else_=0)
    excluded_seconds = sqlalchemy.case(
        (daily.c.exclusion_reason.is_not(None), daily.c.machine_seconds),
        else_=0)
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


def _row_to_breakdown(row: Any, key_names: Tuple[str, ...]) -> Dict[str, Any]:
    result = {name: getattr(row, name) for name in key_names}
    result.update({
        'estimated_cost': float(row.estimated_cost or 0),
        'spot_estimated_cost': float(row.spot_estimated_cost or 0),
        'on_demand_estimated_cost': float(row.on_demand_estimated_cost or 0),
        'priced_machine_seconds': int(row.priced_machine_seconds or 0),
        'excluded_machine_seconds': int(row.excluded_machine_seconds or 0),
    })
    return result


def _normalize_group_by(group_by: Union[str, GroupBy]) -> GroupBy:
    try:
        return GroupBy(group_by)
    except ValueError as e:
        options = ', '.join(option.value for option in GroupBy)
        raise ValueError(f'group_by must be one of: {options}') from e


def _group_key(group_by: GroupBy, row: Any) -> Tuple[Any, ...]:
    if group_by == GroupBy.JOB:
        return (row.workload_type, row.workload_id)
    if group_by == GroupBy.USER:
        return (row.user_hash,)
    return (row.purchase_option,)


def _group_key_names(group_by: GroupBy) -> Tuple[str, ...]:
    if group_by == GroupBy.JOB:
        return ('workload_type', 'workload_id')
    if group_by == GroupBy.USER:
        return ('user_hash', 'user_name')
    return ('purchase_option',)


def _get_group_rows(session: orm.Session, daily: Any, base_filter: Any,
                    workload_rows: List[Any], group_by: GroupBy) -> List[Any]:
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
                          top_group_rows: List[Any]) -> List[Any]:
    if not top_group_rows:
        return []
    match_conditions = [
        _group_match_condition(daily, group_by, row) for row in top_group_rows
    ]
    if group_by == GroupBy.JOB:
        key_columns = [daily.c.workload_type, daily.c.workload_id]
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


def _build_series(group_by: GroupBy, top_group_rows: List[Any],
                  daily_group_rows: List[Any],
                  days_response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    costs_by_group_and_day = {(_group_key(group_by,
                                          row), int(row.day_start_utc)):
                              float(row.estimated_cost or 0)
                              for row in daily_group_rows}
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


def get_estimated_spend(
    days: int = DEFAULT_LOOKBACK_DAYS,
    group_by: Union[str, GroupBy] = GroupBy.JOB,
) -> Dict[str, Any]:
    """Read the last materialized admin estimate using aggregate SQL only."""
    days = max(1, min(int(days), MAX_LOOKBACK_DAYS))
    normalized_group_by = _normalize_group_by(group_by)
    now = int(time.time())
    first_day = _utc_day_start(now - (days - 1) * SECONDS_PER_DAY)
    daily = global_user_state.estimated_spend_daily_table
    state_table = global_user_state.estimated_spend_state_table

    engine = global_user_state.initialize_and_get_db()
    with orm.Session(engine) as session:
        state = session.execute(
            sqlalchemy.select(state_table).where(
                state_table.c.singleton_id == _STATE_ID)).mappings().first()

        base_filter = daily.c.day_start_utc >= first_day
        day_rows = session.execute(
            sqlalchemy.select(
                daily.c.day_start_utc.label('day_start_utc'),
                *_aggregate_columns(daily),
            ).where(base_filter).group_by(daily.c.day_start_utc).order_by(
                daily.c.day_start_utc.asc())).fetchall()

        workload_rows = session.execute(
            sqlalchemy.select(
                daily.c.workload_type.label('workload_type'),
                daily.c.workload_id.label('workload_id'),
                *_aggregate_columns(daily),
            ).where(base_filter).group_by(
                daily.c.workload_type,
                daily.c.workload_id).order_by(sqlalchemy.desc(
                    'estimated_cost')).limit(GROUP_TABLE_LIMIT)).fetchall()

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
    days_response: List[Dict[str, Any]] = []
    for offset in range(days):
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
            'date': datetime.datetime.fromtimestamp(
                day_start, tz=datetime.timezone.utc).date().isoformat(),
            'day_start_utc': day_start,
            **values,
        })

    last_success = int(state['last_success_at'] or 0) if state else 0
    backfill_complete = bool(state['backfill_complete']) if state else False
    total_cost = sum(day['estimated_cost'] for day in days_response)
    total_priced_seconds = sum(
        day['priced_machine_seconds'] for day in days_response)
    total_excluded_seconds = sum(
        day['excluded_machine_seconds'] for day in days_response)
    return {
        'currency': 'USD',
        'basis': 'skypilot_catalog_payg_equivalent',
        'as_of': last_success or None,
        'last_successful_refresh_at': last_success or None,
        'stale': (not last_success or
                  now - last_success > REFRESH_INTERVAL_SECONDS * 2),
        'backfill_complete': backfill_complete,
        'coverage_start_utc':
            (int(state['coverage_start_utc'])
             if state and state['coverage_start_utc'] is not None else None),
        'kubernetes_included': False,
        'reservation_adjustments_applied': False,
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
        'excluded_by_reason': {
            str(row.exclusion_reason): int(row.machine_seconds or 0)
            for row in reason_rows
        },
        'last_error': state['last_error'] if state else None,
    }
