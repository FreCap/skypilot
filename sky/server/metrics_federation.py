"""External Kubernetes metrics federation for the API server."""

import asyncio
from collections import abc
import os
import threading

import fastapi

from sky import core
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.metrics import utils as metrics_utils
from sky.utils import annotations
from sky.utils import common_utils

# Keep the historical logger namespace so structural extraction does not
# change log routing configured by operators.
logger = sky_logging.init_logger('sky.server.metrics')

# Per-context timeout for metrics collection. Must be shorter than the
# Prometheus scrape_timeout configured on the upstream Prometheus that
# scrapes this endpoint so the response arrives before that scrape times
# out and marks the target down. Operators federating from a Prometheus
# with a non-default scrape_timeout should adjust both together; see
# docs/source/reference/api-server/examples/api-server-gpu-metrics-setup.rst.
#
# Without a per-context timeout, a single hanging port-forward (e.g. 30s
# httpx timeout) would block the entire /gpu-metrics response.
#
# 30s accommodates large compute clusters where federate latency plus
# port-forward setup can run 5-10s warm and longer cold.
_PER_CONTEXT_TIMEOUT_SECONDS = 30

_CREDENTIAL_MANAGER_KUBECONFIG_PATH = (
    '/var/skypilot/credentials/kubeconfig/kubeconfig')

_ResultHandler = abc.Callable[
    [str, str, object, metrics_utils.FederationStats, list[str]], None]
_MetricsFetcher = abc.Callable[..., abc.Awaitable[str]]


async def gpu_metrics_debug() -> dict:
    """Collect diagnostics for GPU metrics federation."""
    kubeconfig_env = os.environ.get('KUBECONFIG', 'NOT_SET')
    default_path = os.path.expanduser('~/.kube/config')

    # Check what contexts are visible before and after cache clear.
    pre_clear_contexts = core.get_all_contexts()
    annotations.clear_request_level_cache()
    post_clear_contexts = core.get_all_contexts()

    if kubeconfig_env != 'NOT_SET':
        kubeconfig_paths = kubeconfig_env.split(os.pathsep)
    else:
        kubeconfig_paths = [default_path]
    path_info = {}
    for path in kubeconfig_paths:
        expanded = os.path.expanduser(path)
        try:
            stat = os.stat(expanded)
            path_info[path] = {'exists': True, 'size': stat.st_size}
        except OSError:
            path_info[path] = {'exists': False, 'size': 0}

    cred_mgr_exists = os.path.exists(_CREDENTIAL_MANAGER_KUBECONFIG_PATH)
    cred_mgr_contexts = []
    if cred_mgr_exists:
        try:
            contexts, _ = (
                kubernetes_adaptor.kubernetes.config.list_kube_config_contexts(
                    config_file=_CREDENTIAL_MANAGER_KUBECONFIG_PATH))
            cred_mgr_contexts = [context['name'] for context in contexts]
        except Exception as e:  # pylint: disable=broad-except
            cred_mgr_contexts = [f'error: {e}']

    return {
        'pid': os.getpid(),
        'thread': threading.current_thread().name,
        'KUBECONFIG': kubeconfig_env,
        'kubeconfig_paths': path_info,
        'credential_manager_kubeconfig': {
            'path': _CREDENTIAL_MANAGER_KUBECONFIG_PATH,
            'exists': cred_mgr_exists,
            'contexts': cred_mgr_contexts,
        },
        'contexts_before_cache_clear': pre_clear_contexts,
        'contexts_after_cache_clear': post_clear_contexts,
    }


def handle_federation_result(context: str, route: str, result: object,
                             stats: metrics_utils.FederationStats,
                             all_metrics: list[str]) -> None:
    """Classify one federation task result and record its outcome."""
    if isinstance(result, asyncio.TimeoutError):
        metrics_utils.record_federation_outcome(context, route, 'timeout')
        logger.error(
            f'Failed to get metrics for context {context} (route {route}): '
            f'timed out after {_PER_CONTEXT_TIMEOUT_SECONDS}s '
            f'({stats.summary()}); kubectl port-forward + /federate exceeded '
            f'the per-context budget; series for this cluster are omitted from '
            f'this scrape')
        return
    if isinstance(result, Exception):
        metrics_utils.record_federation_outcome(context, route, 'error')
        logger.error(
            f'Failed to get metrics for context {context} (route {route}): '
            f'{common_utils.format_exception(result)} ({stats.summary()})')
        return
    if isinstance(result, BaseException):
        raise result
    metrics_utils.record_federation_outcome(context, route, 'success')
    logger.debug(f'Federated metrics for context {context} (route {route}): '
                 f'{stats.summary()}')
    assert isinstance(result, str)
    all_metrics.append(result)


async def _federate_metrics(
    route: str,
    fetch_metrics: _MetricsFetcher,
    handle_result: _ResultHandler,
) -> fastapi.Response:
    """Federate one metrics family from every external Kubernetes context."""
    skypilot_config.reload_config()
    annotations.clear_request_level_cache()
    contexts = core.get_all_contexts()
    remote_contexts = [
        context for context in contexts if context != 'in-cluster'
    ]
    stats_list = [metrics_utils.FederationStats() for _ in remote_contexts]
    tasks = [
        asyncio.create_task(
            asyncio.wait_for(fetch_metrics(context, stats=stats),
                             timeout=_PER_CONTEXT_TIMEOUT_SECONDS))
        for context, stats in zip(remote_contexts, stats_list)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_metrics: list[str] = []
    for context, result, stats in zip(remote_contexts, results, stats_list):
        handle_result(context, route, result, stats, all_metrics)

    return fastapi.Response(
        content='\n\n'.join(all_metrics),
        media_type='text/plain; version=0.0.4; charset=utf-8')


async def gpu_metrics(handle_result: _ResultHandler) -> fastapi.Response:
    """Federate GPU metrics from external Kubernetes contexts."""
    return await _federate_metrics('gpu-metrics',
                                   metrics_utils.get_metrics_for_context,
                                   handle_result)


async def endpoint_metrics(handle_result: _ResultHandler) -> fastapi.Response:
    """Federate serving metrics from external Kubernetes contexts."""
    return await _federate_metrics(
        'endpoints-metrics', metrics_utils.get_endpoint_metrics_for_context,
        handle_result)
