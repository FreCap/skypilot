"""Bounded, dynamically sized SkyServe load-balancer request queue."""
# pylint: disable=protected-access
import asyncio
from collections.abc import AsyncIterator
from typing import Any
from unittest import mock

import fastapi
import pytest

from sky.serve import load_balancer
from sky.serve import service_spec as service_spec_lib


def _queue_config(**overrides: Any) -> dict[str, Any]:
    config = {
        'min_size': 10,
        'size_per_replica': 3,
        'max_size': 100,
        'max_concurrency_per_replica': 1,
        'max_concurrency': 32,
        'timeout_seconds': 1,
        'max_request_body_bytes': 16,
    }
    config.update(overrides)
    return config


def _make_lb(**queue_overrides: Any) -> load_balancer.SkyServeLoadBalancer:
    lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 8890)
    lb._apply_routing_spec({'request_queue': _queue_config(**queue_overrides)})
    return lb


def _make_spec(**kwargs: Any) -> service_spec_lib.SkyServiceSpec:
    base = {
        'readiness_path': '/health',
        'initial_delay_seconds': 1,
        'readiness_timeout_seconds': 1,
        'endpoint_probe_interval_seconds': 1,
        'lb_stream_timeout_seconds': 60,
        'min_replicas': 1,
    }
    base.update(kwargs)
    return service_spec_lib.SkyServiceSpec(**base)


def test_queue_config_round_trip_and_defaults():
    spec = _make_spec(lb_request_queue={})
    queue = spec.lb_request_queue
    assert queue is not None
    assert queue['min_size'] == 10
    assert queue['size_per_replica'] == 3
    assert queue['max_size'] == 1000
    assert queue['max_concurrency'] == 32
    assert queue['max_request_body_bytes'] == 1024 * 1024
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(
        spec.to_yaml_config())
    assert restored.lb_request_queue == queue
    assert spec.copy().lb_request_queue == queue


@pytest.mark.parametrize('queue', [
    {
        'min_size': 11,
        'max_size': 10
    },
    {
        'max_size': 0
    },
    {
        'max_concurrency_per_replica': 0
    },
    {
        'max_concurrency': 0
    },
    {
        'timeout_seconds': 0
    },
    {
        'max_request_body_bytes': 0
    },
    {
        'max_size': 2001
    },
    {
        'max_concurrency': 128,
        'max_request_body_bytes': 2 * 1024 * 1024
    },
])
def test_invalid_queue_config_rejected(queue):
    with pytest.raises(ValueError):
        _make_spec(lb_request_queue=queue)


def test_dynamic_queue_size_is_capped():
    lb = _make_lb(min_size=10, size_per_replica=3, max_size=20)
    assert lb._request_queue_limits() == (0, 10)
    lb._load_balancing_policy.set_ready_replicas(
        [f'http://worker-{i}:8000' for i in range(4)])
    assert lb._request_queue_limits() == (4, 12)
    lb._load_balancing_policy.set_ready_replicas(
        [f'http://worker-{i}:8000' for i in range(10)])
    assert lb._request_queue_limits() == (10, 20)


def test_dispatch_concurrency_has_absolute_cap():
    lb = _make_lb(max_concurrency_per_replica=4, max_concurrency=7)
    lb._load_balancing_policy.set_ready_replicas(
        [f'http://worker-{i}:8000' for i in range(3)])
    assert lb._request_queue_limits()[0] == 7


def test_full_queue_rejects_without_growing_waiter_count():

    async def _run():
        lb = _make_lb(min_size=2, size_per_replica=0, max_size=2)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        lb._waiting_request_count = 2
        request = mock.MagicMock()
        request.headers = {}
        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._acquire_request_slot(request)
        assert exc.value.status_code == 503
        assert lb._waiting_request_count == 2

    asyncio.run(_run())


def test_waiter_wakes_when_dispatch_slot_released():

    async def _run():
        lb = _make_lb()
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        request = mock.MagicMock()
        request.headers = {}
        waiter = asyncio.create_task(lb._acquire_request_slot(request))
        while lb._waiting_request_count != 1:
            await asyncio.sleep(0)
        await lb._release_request_slot()
        assert await waiter is True
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 1

    asyncio.run(_run())


def test_proxy_handler_queues_until_first_request_completes():

    async def _run():
        lb = _make_lb()
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        call_count = 0

        async def _proxy(request):
            nonlocal call_count
            del request
            call_count += 1
            if call_count == 1:
                first_started.set()
                await release_first.wait()
            return fastapi.responses.Response(status_code=200)

        lb._proxy_with_retries_inner = _proxy
        first = asyncio.create_task(lb._proxy_with_retries(mock.MagicMock()))
        await first_started.wait()
        second = asyncio.create_task(lb._proxy_with_retries(mock.MagicMock()))
        while lb._waiting_request_count != 1:
            await asyncio.sleep(0)
        assert call_count == 1
        release_first.set()
        assert (await first).status_code == 200
        assert (await second).status_code == 200
        assert call_count == 2
        assert lb._active_request_count == 0
        assert lb._waiting_request_count == 0

    asyncio.run(_run())


def test_disabling_queue_releases_existing_waiters():

    async def _run():
        lb = _make_lb()
        request = mock.MagicMock()
        request.headers = {}
        waiter = asyncio.create_task(lb._acquire_request_slot(request))
        while lb._waiting_request_count != 1:
            await asyncio.sleep(0)
        lb._apply_routing_spec({'request_queue': None})
        await lb._notify_request_queue()
        assert await waiter is False
        assert lb._waiting_request_count == 0

    asyncio.run(_run())


def test_streaming_response_holds_admission_until_asgi_release():

    async def _run():
        lb = _make_lb()
        lb._active_request_count = 1

        async def _upstream_release():
            return None

        response = load_balancer._ReleasingStreamingResponse(
            content=iter(()), release=_upstream_release)
        response.hold_admission_slot_until_complete(lb._release_request_slot)
        assert lb._active_request_count == 1
        await response._release()
        assert lb._active_request_count == 0
        await response._release()
        assert lb._active_request_count == 0

    asyncio.run(_run())


class _StreamingRequest:

    def __init__(self, chunks):
        self.headers = {}
        self._chunks = chunks

    async def stream(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


def test_request_body_bound_handles_chunked_uploads():

    async def _run():
        lb = _make_lb(max_request_body_bytes=4)
        request = _StreamingRequest([b'12', b'345'])
        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._request_body(request)  # type: ignore[arg-type]
        assert exc.value.status_code == 413

    asyncio.run(_run())
