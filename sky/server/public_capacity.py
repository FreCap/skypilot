"""Unauthenticated, privacy-bounded infrastructure capacity read."""

import asyncio
import collections
import concurrent.futures
import datetime
import threading
import time
from typing import Any, Literal

import fastapi
import pydantic

from sky import clouds
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.jobs import state_queries as managed_job_state_queries
from sky.jobs import utils as managed_jobs_utils
from sky.jobs.server import core as managed_jobs_core
from sky.jobs.status_types import ManagedJobStatus
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.workspaces import core as workspaces_core

logger = sky_logging.init_logger(__name__)

router = fastapi.APIRouter()

_CACHE_TTL_SECONDS = 15
_MAX_CONTEXT_WORKERS = 8

PublicJobsStatus = Literal['ok', 'temporarily_unavailable']


class PublicGpuCapacity(pydantic.BaseModel):
    """Public, aggregate-only GPU accounting for one accelerator type."""

    model_config = pydantic.ConfigDict(populate_by_name=True, frozen=True)

    gpu_type: str = pydantic.Field(serialization_alias='type')
    total: int
    used: int | None
    preemptible: int | None
    available: int | None
    unavailable: int


class PublicClusterCapacity(pydantic.BaseModel):
    """Public capacity for one physical Kubernetes context."""

    model_config = pydantic.ConfigDict(frozen=True)

    name: str
    status: Literal['ok', 'temporarily_unavailable']
    gpus: tuple[PublicGpuCapacity, ...]


class PublicUserJobs(pydantic.BaseModel):
    """Active managed-job counts for one public display identity."""

    model_config = pydantic.ConfigDict(frozen=True)

    user: str | None
    active_jobs: int
    statuses: dict[str, int]


class PublicCapacityResponse(pydantic.BaseModel):
    """Versioned response for the unauthenticated public capacity API."""

    model_config = pydantic.ConfigDict(frozen=True)

    version: Literal[1] = 1
    generated_at: datetime.datetime
    partial: bool
    clusters: tuple[PublicClusterCapacity, ...]
    jobs_by_user: tuple[PublicUserJobs, ...]
    jobs_status: PublicJobsStatus


_cache_lock = threading.Lock()
_cache_entry: tuple[float, PublicCapacityResponse] | None = None


def _reset_cache_for_tests() -> None:
    """Clear process-local state between unit tests."""
    global _cache_entry
    with _cache_lock:
        _cache_entry = None


def _discover_contexts() -> tuple[list[tuple[str, str]], bool]:
    """Return deterministic (context, workspace) observations and partial."""
    server_common.refresh_workspace_state_for_sync_handler()
    workspace_names = sorted(workspaces_core.get_configured_workspace_names())
    context_to_workspace: dict[str, str] = {}
    partial = False
    for workspace in workspace_names:
        try:
            with skypilot_config.local_active_workspace_ctx(workspace):
                contexts = clouds.Kubernetes.existing_allowed_contexts(
                    silent=True)
        except Exception as e:  # pylint: disable=broad-except
            partial = True
            logger.warning('Could not discover public capacity contexts for '
                           f'workspace {workspace!r}: {type(e).__name__}.')
            continue
        for context in contexts:
            if context:
                context_to_workspace.setdefault(context, workspace)
    return sorted(context_to_workspace.items()), partial


def _node_is_unavailable(node: Any) -> bool:
    return (not node.is_ready or node.is_cordoned or
            kubernetes_utils.has_untolerated_taint(node.taints))


def _aggregate_gpu_capacity(
        nodes_info: Any) -> tuple[tuple[PublicGpuCapacity, ...], bool]:
    """Aggregate node info while preserving unknown accounting."""
    aggregates: dict[str, dict[str, Any]] = {}
    for node in nodes_info.node_info_dict.values():
        total = node.total.get('accelerator_count', 0)
        if not isinstance(total, int) or total <= 0:
            continue
        gpu_type = node.accelerator_type or 'unknown'
        aggregate = aggregates.setdefault(
            gpu_type, {
                'total': 0,
                'available': 0,
                'preemptible': 0,
                'unavailable': 0,
                'unknown': False,
            })
        aggregate['total'] += total

        if _node_is_unavailable(node):
            aggregate['unavailable'] += total
            continue

        available = node.free.get('accelerators_available')
        if (not isinstance(available, int) or available < 0 or
                available > total):
            aggregate['unknown'] = True
            continue

        allocated = total - available
        preemptible = node.accelerators_preemptible
        if preemptible is None and allocated == 0:
            preemptible = 0
        if (not isinstance(preemptible, int) or preemptible < 0 or
                preemptible > allocated):
            aggregate['unknown'] = True
            continue

        aggregate['available'] += available
        aggregate['preemptible'] += preemptible

    rows = []
    partial = False
    for gpu_type, aggregate in sorted(aggregates.items()):
        if aggregate['unknown']:
            partial = True
            used = None
            preemptible = None
            available = None
        else:
            available = aggregate['available']
            preemptible = aggregate['preemptible']
            used = (aggregate['total'] - aggregate['unavailable'] - available -
                    preemptible)
            if used < 0:
                partial = True
                used = None
                preemptible = None
                available = None
        rows.append(
            PublicGpuCapacity(gpu_type=gpu_type,
                              total=aggregate['total'],
                              used=used,
                              preemptible=preemptible,
                              available=available,
                              unavailable=aggregate['unavailable']))
    return tuple(rows), partial


def _observe_context(context: str,
                     workspace: str) -> tuple[PublicClusterCapacity, bool]:
    try:
        with skypilot_config.local_active_workspace_ctx(workspace):
            nodes_info = kubernetes_utils.get_kubernetes_node_info(context)
        gpus, partial = _aggregate_gpu_capacity(nodes_info)
        return (PublicClusterCapacity(name=context, status='ok',
                                      gpus=gpus), partial)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Could not observe public capacity for context '
                       f'{context!r}: {type(e).__name__}.')
        return (PublicClusterCapacity(name=context,
                                      status='temporarily_unavailable',
                                      gpus=()), True)


def _observe_contexts(
    context_workspaces: list[tuple[str, str]]
) -> tuple[tuple[PublicClusterCapacity, ...], bool]:
    if not context_workspaces:
        return (), False
    max_workers = min(_MAX_CONTEXT_WORKERS, len(context_workspaces))
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix='public-capacity') as executor:
        observations = list(
            executor.map(lambda item: _observe_context(*item),
                         context_workspaces))
    clusters = tuple(observation[0] for observation in observations)
    partial = any(observation[1] for observation in observations)
    return clusters, partial


_MANAGED_JOB_FIELDS = ['job_id', 'user_hash', 'status']


def _active_managed_job_rows() -> list[dict[str, Any]]:
    if managed_jobs_utils.is_consolidation_mode():
        rows, _ = managed_job_state_queries.get_managed_jobs_with_filters(
            fields=_MANAGED_JOB_FIELDS, skip_finished=True)
        return rows
    rows, _, _, _ = managed_jobs_core.queue_v2(refresh=False,
                                               skip_finished=True,
                                               all_users=True,
                                               fields=_MANAGED_JOB_FIELDS)
    return rows


def _status_value(status: Any) -> str:
    value = getattr(status, 'value', status)
    return str(value) if value is not None else 'UNKNOWN'


def _aggregate_jobs_by_user(
) -> tuple[tuple[PublicUserJobs, ...], PublicJobsStatus]:
    try:
        rows = _active_managed_job_rows()
        user_names = {
            user.id: user.name for user in global_user_state.get_all_users()
        }
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Could not observe public managed-job counts: '
                       f'{type(e).__name__}.')
        return (), 'temporarily_unavailable'

    terminal_statuses = {
        status.value for status in ManagedJobStatus.terminal_statuses()
    }
    jobs: dict[Any, dict[str, Any]] = {}
    for row in rows:
        job_id = row.get('job_id')
        if job_id is None:
            continue
        job = jobs.setdefault(job_id, {
            'user_hash': row.get('user_hash'),
            'statuses': set(),
        })
        if job['user_hash'] != row.get('user_hash'):
            job['user_hash'] = None
        job['statuses'].add(_status_value(row.get('status')))

    grouped: dict[str | None, collections.Counter[str]] = {}
    for job in jobs.values():
        active_statuses = job['statuses'] - terminal_statuses
        if not active_statuses:
            continue
        if len(active_statuses) == 1:
            status = next(iter(active_statuses))
        else:
            status = 'MIXED'
        user_name = user_names.get(job['user_hash'])
        grouped.setdefault(user_name, collections.Counter())[status] += 1

    def _user_sort_key(
            item: tuple[str | None,
                        collections.Counter[str]]) -> tuple[int, str]:
        user, _ = item
        return (user is None, user or '')

    result = []
    for user, statuses in sorted(grouped.items(), key=_user_sort_key):
        result.append(
            PublicUserJobs(user=user,
                           active_jobs=sum(statuses.values()),
                           statuses=dict(sorted(statuses.items()))))
    return tuple(result), 'ok'


def _build_public_capacity() -> PublicCapacityResponse:
    context_workspaces, discovery_partial = _discover_contexts()
    clusters, cluster_partial = _observe_contexts(context_workspaces)
    jobs_by_user, jobs_status = _aggregate_jobs_by_user()
    return PublicCapacityResponse(
        generated_at=datetime.datetime.now(datetime.timezone.utc),
        partial=(discovery_partial or cluster_partial or jobs_status != 'ok'),
        clusters=clusters,
        jobs_by_user=jobs_by_user,
        jobs_status=jobs_status,
    )


def get_public_capacity() -> PublicCapacityResponse:
    """Return a process-cached, single-flight public capacity snapshot."""
    global _cache_entry
    now = time.monotonic()
    with _cache_lock:
        if _cache_entry is not None and now < _cache_entry[0]:
            return _cache_entry[1].model_copy(deep=True)
        snapshot = _build_public_capacity()
        _cache_entry = (time.monotonic() + _CACHE_TTL_SECONDS, snapshot)
        return snapshot.model_copy(deep=True)


@router.get(server_constants.PUBLIC_CAPACITY_PATH,
            response_model=PublicCapacityResponse,
            response_model_by_alias=True)
async def public_capacity(response: fastapi.Response) -> PublicCapacityResponse:
    """Return aggregate cluster capacity and active jobs without auth."""
    try:
        snapshot = await asyncio.to_thread(get_public_capacity)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Could not construct public capacity snapshot: '
                     f'{type(e).__name__}.')
        raise fastapi.HTTPException(
            status_code=503,
            detail='Public capacity is temporarily unavailable.') from None
    response.headers['Cache-Control'] = (
        f'public, max-age={_CACHE_TTL_SECONDS}')
    return snapshot
