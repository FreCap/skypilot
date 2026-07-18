"""Stateless cost projection for estimated-spend history rows."""

from collections.abc import Mapping
import math
import pickle
from typing import Any

SECONDS_PER_DAY = 24 * 60 * 60
SECONDS_PER_HOUR = 60 * 60


def utc_day_start(timestamp: int) -> int:
    return int(timestamp) // SECONDS_PER_DAY * SECONDS_PER_DAY


def split_interval_by_utc_day(start: int, end: int) -> dict[int, int]:
    """Split a half-open epoch interval into UTC-day overlap seconds."""
    if end <= start:
        return {}
    overlaps: dict[int, int] = {}
    cursor = start
    while cursor < end:
        day_start = utc_day_start(cursor)
        next_day = day_start + SECONDS_PER_DAY
        overlap_end = min(end, next_day)
        overlaps[day_start] = overlaps.get(day_start, 0) + overlap_end - cursor
        cursor = overlap_end
    return overlaps


def safe_unpickle(value: Any) -> Any:
    if value is None:
        return None
    try:
        return pickle.loads(value)
    # Legacy resource classes may move or disappear between server versions,
    # and custom reducers can raise exceptions outside pickle.PickleError. A
    # single history row must not prevent the durable watermark from advancing.
    except Exception:  # pylint: disable=broad-except
        return None


def resource_cloud(resources: Any) -> str | None:
    if resources is None:
        return None
    cloud = getattr(resources, 'cloud', None)
    return str(cloud) if cloud is not None else None


def get_pricing(
        resources: Any, cloud: str | None, num_nodes: int,
        rate_cache: dict[str, float]) -> tuple[float | None, str | None]:
    """Return total cluster hourly rate and an exclusion reason."""
    if cloud is not None and cloud.casefold() == 'kubernetes':
        return None, 'kubernetes'
    if resources is None:
        return None, 'unknown_price'
    region = getattr(resources, 'region', None)
    zone = getattr(resources, 'zone', None)
    cache_key = (f'{resources!r}|region={region!r}|zone={zone!r}|'
                 f'nodes={num_nodes}')
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


def build_daily_rows(source: Mapping[str,
                                     Any], as_of: int, recompute_start: int,
                     rate_cache: dict[str, float]) -> list[dict[str, Any]]:
    """Materialize one cluster-history row over a bounded time window."""
    usage_intervals = safe_unpickle(source.get('usage_intervals'))
    if not isinstance(usage_intervals, list):
        return []

    resources = safe_unpickle(source.get('launched_resources'))
    try:
        num_nodes = int(source.get('num_nodes') or 1)
    except (TypeError, ValueError):
        num_nodes = 1
    if num_nodes <= 0:
        num_nodes = 1

    cloud = source.get('cloud') or resource_cloud(resources)
    hourly_rate, exclusion_reason = get_pricing(resources, cloud, num_nodes,
                                                rate_cache)
    use_spot = (bool(getattr(resources, 'use_spot', False))
                if resources is not None else None)

    overlap_by_day: dict[int, int] = {}
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
        for day_start, seconds in split_interval_by_utc_day(start, end).items():
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
