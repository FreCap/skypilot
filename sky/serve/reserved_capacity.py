"""Reserved-capacity fill poller.

[boltz fork] Opt-in (replica_policy.reserved_capacity_fill): a controller
background thread that measures FREE capacity on the service's zero-cost
locations (reserved/already-paid Kubernetes pools) and feeds the autoscaler
a snapshot via `collect_reserved_capacity`, so the fleet opportunistically
fills idle reserved GPUs. This module owns only the measurement side; the
target composition lives in `Autoscaler._apply_reserved_capacity_fill` and
the zero-cost-only launch pinning in `ReplicaManager._launch_replica`.
"""
import os
import re
import time
import typing
from typing import Any, Callable, Dict, List, Optional

from sky import sky_logging
from sky.catalog import kubernetes_catalog
from sky.serve import constants
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    from sky.serve import autoscalers
    from sky.serve import spot_placer as spot_placer_lib

logger = sky_logging.init_logger(__name__)


def poll_interval_seconds() -> float:
    override = os.environ.get(constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR)
    if override is not None:
        try:
            return max(1.0, float(override))
        except ValueError:
            logger.warning(
                f'Invalid {constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR} '
                f'value {override!r}, using default '
                f'{constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS}s.')
    return float(constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS)


def query_free_slots(
        zero_cost_locations: List['spot_placer_lib.Location']) -> int:
    """Free replica slots across the zero-cost locations, summed per shape.

    EXPENSIVE: the realtime availability query lists every pod in the
    cluster and is deliberately uncached -- call it ONLY from the poller
    thread, never from the autoscaler decision tick.

    Rules:
    - Unknown availability (-1, e.g. missing list-pods permission) counts
      as 0 free: fill must never launch on guessed capacity.
    - Shapes are assumed to map to disjoint node pools (v0; overlapping
      pools would double-count and are explicitly out of scope), but the
      same (context, gpu) shape enumerated twice is still counted once.
    - Only Kubernetes locations are queryable in v0; other zero-cost
      locations contribute no fill.
    """
    total = 0
    seen: set = set()
    for location in zero_cost_locations:
        if str(location.cloud).lower() != 'kubernetes':
            continue
        if not location.accelerators:
            continue
        gpu_name, per_replica = next(iter(location.accelerators.items()))
        try:
            per_replica = max(1, int(per_replica))
        except (TypeError, ValueError):
            per_replica = 1
        key = (location.region, gpu_name)
        if key in seen:
            continue
        seen.add(key)
        try:
            _, _, available = kubernetes_catalog.list_accelerators_realtime(
                gpus_only=True,
                name_filter=f'^{re.escape(gpu_name)}$',
                region_filter=location.region,
                quantity_filter=None,
                case_sensitive=False,
                require_price=False)
        except Exception as e:  # pylint: disable=broad-except
            # A failed context contributes 0 this cycle; the autoscaler's
            # staleness decay handles a persistently failing poller.
            logger.warning('Reserved-capacity poll failed for context '
                           f'{location.region!r} gpu {gpu_name!r}: '
                           f'{common_utils.format_exception(e)}')
            continue
        free_gpus = sum(count for count in available.values() if count > 0)
        total += free_gpus // per_replica
    return total


def poller_loop(
    get_autoscaler: Callable[[], 'autoscalers.Autoscaler'],
    get_spot_placer: Callable[[],
                              Optional['spot_placer_lib.SpotPlacer']]) -> None:
    """Poll free zero-cost capacity forever, feeding the autoscaler.

    Runs as a supervised thread started by the controller (only when the
    service opted in AND a spot placer exists -- the placer defines the
    zero-cost location set). Takes getters, not the live objects: an
    update_service can replace the controller's autoscaler, and the
    snapshot must reach the current one.
    """
    while True:
        try:
            placer = get_spot_placer()
            # An update can turn the flag off on the live autoscaler; the
            # thread stays alive (a later update can re-enable it) but
            # must not keep issuing the expensive cluster-wide pod-listing
            # query for a snapshot nobody consumes.
            fill_enabled = get_autoscaler().reserved_capacity_fill
            if placer is not None and fill_enabled:
                zero_cost = placer.zero_cost_locations()
                free_slots = query_free_slots(zero_cost)
                keys: List[Dict[str, Any]] = [
                    location.to_pickleable() for location in zero_cost
                ]
                get_autoscaler().collect_reserved_capacity(
                    free_slots, keys, time.time())
                logger.info(f'Reserved-capacity poll: {free_slots} free '
                            f'slot(s) across {len(keys)} zero-cost '
                            'location(s).')
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Error in reserved-capacity poller: '
                         f'{common_utils.format_exception(e)}')
        time.sleep(poll_interval_seconds())
