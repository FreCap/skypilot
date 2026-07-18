"""/gpu-metrics carries workload KSM series; /endpoints-metrics federates
serving-engine metrics.

The serving dashboards (vLLM Serving, Autoscaling) read these series from the
central Prometheus: vllm:* via /endpoints-metrics, and Deployment replica
counts + the HPA target via the /gpu-metrics federation.

Also covers the federation observability helpers: FederationStats.summary()
(the timing/size breakdown surfaced in logs) and _handle_federation_result()
(the per-context success/timeout/error classification).
"""
import asyncio
from unittest import mock

import pytest

from sky import core
from sky import skypilot_config
from sky.metrics import utils as metrics_utils
from sky.server import metrics as server_metrics
from sky.utils import annotations

_MIB = 2**20


def test_endpoint_metrics_carries_autoscaling_dashboard_series():
    joined = '\n'.join(metrics_utils.ENDPOINT_METRICS_MATCH_PATTERNS)
    # Serving-engine series + replica panels + the HPA threshold line.
    assert 'vllm:' in joined
    assert 'kube_deployment_' in joined
    assert 'kube_horizontalpodautoscaler_spec_target_metric' in joined


def test_gpu_metrics_keeps_gpu_semantics():
    # The workload KSM series exist solely for endpoint observability and
    # ride /endpoints-metrics, not the GPU federation.
    joined = '\n'.join(metrics_utils.GPU_METRICS_MATCH_PATTERNS)
    assert 'kube_deployment_' not in joined
    assert 'kube_horizontalpodautoscaler' not in joined


def test_no_per_pod_phase_series():
    # kube_pod_status_phase is the expensive per-pod family and has no
    # consumer; it must not ride either federation.
    for pats in (metrics_utils.GPU_METRICS_MATCH_PATTERNS,
                 metrics_utils.ENDPOINT_METRICS_MATCH_PATTERNS):
        assert 'kube_pod_status_phase' not in '\n'.join(pats)


def test_endpoint_metrics_route_registered():
    paths = {
        getattr(r, 'path', None) for r in server_metrics.metrics_app.routes
    }
    assert '/endpoints-metrics' in paths
    assert '/gpu-metrics' in paths
    assert hasattr(metrics_utils, 'get_endpoint_metrics_for_context')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('handler_name', 'fetch_name', 'route'),
    [
        ('gpu_metrics', 'get_metrics_for_context', 'gpu-metrics'),
        ('endpoint_metrics', 'get_endpoint_metrics_for_context',
         'endpoints-metrics'),
    ],
)
async def test_federation_handlers_preserve_order_and_facade(
        handler_name, fetch_name, route):
    handler = getattr(server_metrics, handler_name)
    fetch = mock.AsyncMock(
        side_effect=lambda context, **_: f'metric{{context="{context}"}} 1')

    with mock.patch.object(skypilot_config, 'reload_config') as reload_config, \
         mock.patch.object(annotations,
                           'clear_request_level_cache') as clear_cache, \
         mock.patch.object(core,
                           'get_all_contexts',
                           return_value=['in-cluster', 'ctx-b', 'ctx-a']), \
         mock.patch.object(metrics_utils, fetch_name, fetch), \
         mock.patch.object(metrics_utils,
                           'record_federation_outcome') as record_outcome:
        response = await handler()

    assert response.body.decode() == (
        'metric{context="ctx-b"} 1\n\nmetric{context="ctx-a"} 1')
    assert [call.args[0] for call in fetch.await_args_list
           ] == ['ctx-b', 'ctx-a']
    assert all('stats' in call.kwargs for call in fetch.await_args_list)
    assert record_outcome.call_args_list == [
        mock.call('ctx-b', route, 'success'),
        mock.call('ctx-a', route, 'success'),
    ]
    reload_config.assert_called_once_with()
    clear_cache.assert_called_once_with()

    registered = [
        candidate for candidate in server_metrics.metrics_app.routes
        if getattr(candidate, 'path', None) == f'/{route}'
    ]
    assert len(registered) == 1
    assert registered[0].endpoint is handler
    assert handler.__module__ == 'sky.server.metrics'


@pytest.mark.asyncio
async def test_gpu_metrics_debug_preserves_facade_and_cache_refresh(
        tmp_path, monkeypatch):
    kubeconfig = tmp_path / 'kubeconfig'
    kubeconfig.write_text('apiVersion: v1\n')
    monkeypatch.setenv('KUBECONFIG', str(kubeconfig))

    with mock.patch.object(
            core,
            'get_all_contexts',
            side_effect=[['before'], ['after']]) as get_contexts, \
         mock.patch.object(annotations,
                           'clear_request_level_cache') as clear_cache:
        result = await server_metrics.gpu_metrics_debug()

    assert result['KUBECONFIG'] == str(kubeconfig)
    assert result['kubeconfig_paths'][str(kubeconfig)] == {
        'exists': True,
        'size': len('apiVersion: v1\n'),
    }
    assert result['contexts_before_cache_clear'] == ['before']
    assert result['contexts_after_cache_clear'] == ['after']
    assert get_contexts.call_count == 2
    clear_cache.assert_called_once_with()

    registered = [
        candidate for candidate in server_metrics.metrics_app.routes
        if getattr(candidate, 'path', None) == '/debug-gpu-metrics'
    ]
    assert len(registered) == 1
    assert registered[0].endpoint is server_metrics.gpu_metrics_debug
    assert server_metrics.gpu_metrics_debug.__module__ == 'sky.server.metrics'


@pytest.mark.asyncio
async def test_gpu_metrics_preserves_result_handler_patch_point():
    fetch = mock.AsyncMock(return_value='metric 1')

    def append_result(_context, _route, result, _stats, all_metrics):
        all_metrics.append(result)

    with mock.patch.object(skypilot_config, 'reload_config'), \
         mock.patch.object(annotations, 'clear_request_level_cache'), \
         mock.patch.object(core,
                           'get_all_contexts',
                           return_value=['ctx']), \
         mock.patch.object(metrics_utils, 'get_metrics_for_context', fetch), \
         mock.patch.object(server_metrics,
                           '_handle_federation_result',
                           side_effect=append_result) as handle_result:
        response = await server_metrics.gpu_metrics()

    assert response.body == b'metric 1'
    handle_result.assert_called_once()


# --- FederationStats.summary() ---


def test_federation_stats_summary_empty():
    # No phase finished (e.g. timed out establishing the port-forward).
    stats = metrics_utils.FederationStats()
    assert stats.summary() == 'port_forward=incomplete, federate=incomplete'


def test_federation_stats_summary_port_forward_only():
    # Port-forward done, federate cancelled mid-transfer (the timeout case).
    stats = metrics_utils.FederationStats()
    stats.port_forward_seconds = 0.31
    assert stats.summary() == 'port_forward=0.31s, federate=incomplete'


def test_federation_stats_summary_full():
    stats = metrics_utils.FederationStats()
    stats.port_forward_seconds = 0.29
    stats.federate_seconds = 4.82
    stats.body_bytes = int(64.4 * _MIB)
    stats.wire_bytes = int(3.45 * _MIB)
    stats.content_encoding = 'gzip'
    out = stats.summary()
    assert 'port_forward=0.29s' in out
    assert 'federate=4.82s' in out
    assert 'body=64.4MiB' in out
    assert 'wire=3.45MiB' in out
    assert 'enc=gzip' in out


def test_federation_stats_summary_never_raises_on_missing_bytes():
    # Defensive: summary() runs inside log calls and must never raise, even in
    # the should-not-happen state where federate_seconds is set but the byte
    # fields are not.
    stats = metrics_utils.FederationStats()
    stats.federate_seconds = 1.0
    out = stats.summary()  # must not raise
    assert 'body=unknown' in out
    assert 'wire=unknown' in out


# --- _handle_federation_result() classification ---


def test_handle_result_success_appends():
    out = []
    server_metrics._handle_federation_result('ctx', 'gpu-metrics',
                                             'metric_text',
                                             metrics_utils.FederationStats(),
                                             out)
    assert out == ['metric_text']


def test_handle_result_empty_body_appends():
    # An empty federated body is valid and must still be appended.
    out = []
    server_metrics._handle_federation_result('ctx', 'gpu-metrics', '',
                                             metrics_utils.FederationStats(),
                                             out)
    assert out == ['']


def test_handle_result_timeout_not_appended():
    out = []
    server_metrics._handle_federation_result('ctx', 'gpu-metrics',
                                             asyncio.TimeoutError(),
                                             metrics_utils.FederationStats(),
                                             out)
    assert out == []


def test_handle_result_error_not_appended():
    out = []
    server_metrics._handle_federation_result('ctx', 'gpu-metrics',
                                             ValueError('boom'),
                                             metrics_utils.FederationStats(),
                                             out)
    assert out == []


def test_handle_result_base_exception_reraised():
    # KeyboardInterrupt/SystemExit must propagate, not be swallowed.
    out = []
    with pytest.raises(KeyboardInterrupt):
        server_metrics._handle_federation_result(
            'ctx', 'gpu-metrics', KeyboardInterrupt(),
            metrics_utils.FederationStats(), out)
    assert out == []
