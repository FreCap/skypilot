"""Bounded, dynamically sized SkyServe load-balancer request queue."""
# pylint: disable=protected-access
import asyncio
from collections.abc import AsyncIterator
from collections.abc import Callable
import json
from typing import Any
from unittest import mock

import fastapi
from load_balancer_test_utils import publish_current_occupancy_snapshot
import pytest
from starlette import datastructures

from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import load_balancer
from sky.serve import load_balancing_policies
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
        'use_async_occupancy': False,
    }
    config.update(overrides)
    return config


def _make_lb(**queue_overrides: Any) -> load_balancer.SkyServeLoadBalancer:
    lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 8890)
    lb._apply_routing_spec({'request_queue': _queue_config(**queue_overrides)})
    return lb


def _request() -> mock.MagicMock:
    request = mock.MagicMock()
    request.method = 'POST'
    request.headers = {}
    request.is_disconnected = mock.AsyncMock(return_value=False)
    return request


def _request_with_headers(
        raw_headers: list[tuple[bytes, bytes]]) -> mock.MagicMock:
    request = _request()
    request.headers = datastructures.Headers(raw=raw_headers)
    return request


async def _wait_until(predicate: Callable[[], bool],
                      timeout_seconds: float = 1) -> None:

    async def _poll() -> None:
        while not predicate():  # noqa: ASYNC110
            await asyncio.sleep(0)

    await asyncio.wait_for(_poll(), timeout=timeout_seconds)


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
    assert queue['use_async_occupancy'] is False
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(
        spec.to_yaml_config())
    assert restored.lb_request_queue == queue
    assert spec.copy().lb_request_queue == queue


def test_per_accelerator_floor_round_trip_and_validation():
    spec = _make_spec(min_replicas=1,
                      max_replicas=4,
                      min_replicas_by_accelerator={
                          'A100': 1,
                          'A100-80GB': 1,
                      },
                      target_qps_per_replica={
                          'A100': 1,
                          'A100-80GB': 2,
                      },
                      load_balancing_policy='instance_aware_least_load')
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(
        spec.to_yaml_config())
    assert restored.min_replicas_by_accelerator == {
        'A100': 1,
        'A100-80GB': 1,
    }
    assert spec.copy().min_replicas_by_accelerator == (
        spec.min_replicas_by_accelerator)
    concurrency_spec = _make_spec(
        min_replicas=0,
        max_replicas=4,
        min_replicas_by_accelerator={'A100': 2},
        target_concurrency_per_replica=1,
        load_balancing_policy='instance_aware_least_load')
    assert concurrency_spec.min_replicas_by_accelerator == {'A100': 2}
    with pytest.raises(ValueError, match='must not exceed max_replicas'):
        _make_spec(min_replicas=0,
                   max_replicas=1,
                   min_replicas_by_accelerator={
                       'A100': 1,
                       'A100-80GB': 1,
                   })
    with pytest.raises(ValueError, match='requires either dict type'):
        _make_spec(min_replicas=0,
                   max_replicas=2,
                   min_replicas_by_accelerator={'A100': 1},
                   load_balancing_policy='instance_aware_least_load')


def test_priority_timeout_thresholds_round_trip_and_select_highest_match():
    thresholds = [{
        'min_priority': 0,
        'timeout_seconds': 600,
    }, {
        'min_priority': 50,
        'timeout_seconds': 60,
    }]
    spec = _make_spec(lb_request_queue={
        'timeout_seconds': 20,
        'timeout_seconds_by_priority': thresholds,
    })
    queue = spec.lb_request_queue
    assert queue is not None
    assert queue['timeout_seconds_by_priority'] == thresholds
    assert service_spec_lib.SkyServiceSpec.from_yaml_config(
        spec.to_yaml_config()).lb_request_queue == queue
    assert load_balancer.SkyServeLoadBalancer._request_queue_timeout(queue,
                                                                     0) == 600
    assert load_balancer.SkyServeLoadBalancer._request_queue_timeout(queue,
                                                                     49) == 600
    assert load_balancer.SkyServeLoadBalancer._request_queue_timeout(queue,
                                                                     50) == 60
    assert load_balancer.SkyServeLoadBalancer._request_queue_timeout(
        queue, 100) == 60


@pytest.mark.parametrize('thresholds', [
    [{
        'min_priority': 50,
        'timeout_seconds': 60,
    }, {
        'min_priority': 0,
        'timeout_seconds': 600,
    }],
    [{
        'min_priority': 50,
        'timeout_seconds': 60,
    }, {
        'min_priority': 50,
        'timeout_seconds': 600,
    }],
    [{
        'min_priority': 101,
        'timeout_seconds': 60,
    }],
    [{
        'min_priority': 0,
        'timeout_seconds': float('inf'),
    }],
])
def test_invalid_priority_timeout_thresholds_rejected(thresholds):
    with pytest.raises(ValueError):
        _make_spec(lb_request_queue={
            'timeout_seconds_by_priority': thresholds,
        })


def test_async_occupancy_defaults_per_replica_cap_to_global_cap():
    spec = _make_spec(lb_request_queue={
        'use_async_occupancy': True,
        'max_concurrency': 17,
    })
    queue = spec.lb_request_queue
    assert queue is not None
    assert queue['max_concurrency_per_replica'] == 17

    capped = _make_spec(
        lb_request_queue={
            'use_async_occupancy': True,
            'max_concurrency': 17,
            'max_concurrency_per_replica': 4,
        })
    assert capped.lb_request_queue is not None
    assert capped.lb_request_queue['max_concurrency_per_replica'] == 4


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
        'max_size': 10001
    },
    {
        'use_async_occupancy': 1
    },
    {
        'max_concurrency': 128,
        'max_request_body_bytes': 2 * 1024 * 1024
    },
])
def test_invalid_queue_config_rejected(queue):
    with pytest.raises(ValueError):
        _make_spec(lb_request_queue=queue)


def test_queue_size_limit_accepts_ten_thousand():
    spec = _make_spec(lb_request_queue={'max_size': 10000})
    assert spec.lb_request_queue is not None
    assert spec.lb_request_queue['max_size'] == 10000


def test_dynamic_queue_size_is_capped():
    lb = _make_lb(min_size=0, size_per_replica=3, max_size=3000)
    assert lb._request_queue_limits() == (0, 0)
    lb._load_balancing_policy.set_ready_replicas(['http://worker-0:8000'])
    assert lb._request_queue_limits() == (1, 3)
    lb._load_balancing_policy.set_ready_replicas(
        [f'http://worker-{i}:8000' for i in range(1000)])
    assert lb._request_queue_limits() == (32, 3000)


def test_async_occupancy_clamps_dispatch_limit():
    lb = _make_lb(min_size=0,
                  size_per_replica=3,
                  max_size=3000,
                  use_async_occupancy=True)
    urls = ['http://worker-0:8000', 'http://worker-1:8000']
    lb._load_balancing_policy.set_ready_replicas(urls)
    publish_current_occupancy_snapshot(lb,
                                       occupancy={
                                           urls[0]: 1,
                                           urls[1]: 0,
                                       },
                                       total_slots={
                                           urls[0]: 1,
                                           urls[1]: 1,
                                       },
                                       free_slots={
                                           urls[0]: 0,
                                           urls[1]: 1,
                                       })
    assert lb._request_queue_limits() == (1, 6)
    lb._replica_free_slots = {}
    assert lb._request_queue_limits() == (0, 6)


def test_async_occupancy_requires_complete_local_sample_contract():
    lb = _make_lb(min_size=0, size_per_replica=3, use_async_occupancy=True)
    url = 'http://worker:8000'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._replica_occupancy = {url: 0}
    lb._replica_total_slots = {url: 1}
    lb._replica_free_slots = {url: 1}

    assert lb._request_queue_limits()[0] == 0
    lb._occupancy_pending_reservations = {url: 1}
    assert lb._request_queue_limits()[0] == 0
    lb._occupancy_pending_reservations = {}
    lb._occupancy_sample_generation = {url: 0}
    assert lb._request_queue_limits()[0] == 0
    lb._occupancy_sample_time = {url: load_balancer.time.monotonic()}
    assert lb._request_queue_limits()[0] == 0
    lb._occupancy_sample_role_epoch = {url: lb._occupancy_role_epoch}
    assert lb._request_queue_limits()[0] == 1

    lb._replica_occupancy = {}
    assert lb._request_queue_limits()[0] == 0
    lb._replica_occupancy = {url: 0}
    lb._replica_total_slots = {}
    assert lb._request_queue_limits()[0] == 0


def test_async_occupancy_sizes_queue_by_probed_slots():
    lb = _make_lb(min_size=0,
                  size_per_replica=3,
                  max_size=3000,
                  max_concurrency_per_replica=8,
                  use_async_occupancy=True)
    one_gpu = 'http://one-gpu:8000'
    four_gpu = 'http://four-gpu:8000'
    unknown = 'http://unknown:8000'
    lb._load_balancing_policy.set_ready_replicas([one_gpu, four_gpu, unknown])
    publish_current_occupancy_snapshot(lb,
                                       occupancy={
                                           one_gpu: 0,
                                           four_gpu: 0,
                                       },
                                       total_slots={
                                           one_gpu: 1,
                                           four_gpu: 4,
                                       },
                                       free_slots={
                                           one_gpu: 1,
                                           four_gpu: 4,
                                       })

    assert lb._request_queue_limits() == (5, 15)
    lb._request_queue_config = {
        **(lb._request_queue_config or {}),
        'max_concurrency_per_replica': 2,
    }
    assert lb._request_queue_limits() == (3, 9)


def test_logical_queue_size_uses_plan_but_dispatch_requires_observation():
    lb = _make_lb(min_size=0,
                  size_per_replica=3,
                  max_size=3000,
                  max_concurrency_per_replica=8,
                  use_async_occupancy=True)
    url = 'http://four-gpu:8000'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._capacity_hint = {
        'replica_unit': 'logical_slot',
        'planned_capacity_by_url': {
            url: 4,
        },
    }

    assert lb._request_queue_limits() == (0, 12)

    publish_current_occupancy_snapshot(lb,
                                       occupancy={url: 0},
                                       total_slots={url: 4},
                                       free_slots={url: 4})
    assert lb._request_queue_limits() == (4, 12)

    lb._occupancy_sample_time[url] -= (
        constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS + 1)
    assert lb._request_queue_limits() == (0, 12)


def test_fast_ack_multi_slot_reservations_fill_and_resume_exactly():
    """Collected local E2E for one URL backed by four async workers."""

    async def _run():
        capacity = 4
        running = 0
        url = 'http://aggregate-four-gpu:8000'
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      max_concurrency_per_replica=capacity,
                      max_concurrency=capacity,
                      timeout_seconds=1,
                      use_async_occupancy=True)
        lb._load_balancing_policy.set_ready_replicas([url])
        lb._occupancy_declared_urls = {url}

        async def _capacity(session, replica_url):
            del session
            assert replica_url == url
            return running, capacity - running, capacity

        async def _fast_ack(replica_url, request):
            nonlocal running
            del request
            assert replica_url == url
            assert running < capacity, 'dispatched past aggregate capacity'
            running += 1
            return fastapi.responses.Response(status_code=202)

        lb._fetch_replica_occupancy = _capacity
        lb._proxy_request_to = _fast_ack
        await lb._probe_replica_occupancy_once()

        for expected_running in range(1, capacity + 1):
            response = await lb._proxy_with_retries(_request())
            assert response.status_code == 202
            assert running == expected_running
            with lb._client_pool_lock:
                assert lb._effective_replica_free_slots_locked() == {
                    url: capacity - expected_running
                }

        assert lb._occupancy_pending_reservations == {url: capacity}
        assert lb._load_balancing_policy.occupancy_map == {url: capacity}
        assert lb._request_queue_limits()[0] == 0

        fifth = asyncio.create_task(lb._proxy_with_retries(_request()))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        assert running == capacity

        # Completing one child does not help until the aggregate endpoint
        # reports it. That valid probe reconciles all old reservations and
        # wakes exactly one waiter, which consumes the newly free slot.
        running -= 1
        await lb._probe_replica_occupancy_once()
        assert (await fifth).status_code == 202
        assert running == capacity
        assert lb._occupancy_unassigned_reservations == 0
        assert lb._occupancy_pending_reservations == {url: 1}
        with lb._client_pool_lock:
            assert lb._effective_replica_free_slots_locked() == {url: 0}

    asyncio.run(_run())


def test_instance_aware_routing_fills_one_and_four_gpu_replicas():

    async def _run():
        one_gpu = 'http://one-gpu:8000'
        four_gpu = 'http://four-gpu:8000'
        capacities = {one_gpu: 1, four_gpu: 4}
        counts = {one_gpu: 0, four_gpu: 0}
        lb = _make_lb(min_size=0,
                      size_per_replica=0,
                      max_concurrency_per_replica=4,
                      max_concurrency=5,
                      use_async_occupancy=True)
        with lb._client_pool_lock:
            lb._apply_routing_spec({
                'load_balancing_policy_name': 'instance_aware_least_load',
                'target_concurrency_per_replica': 1,
                'request_queue': _queue_config(min_size=0,
                                               size_per_replica=0,
                                               max_concurrency_per_replica=4,
                                               max_concurrency=5,
                                               use_async_occupancy=True),
            })
            policy = lb._load_balancing_policy
            assert isinstance(
                policy, load_balancing_policies.InstanceAwareLeastLoadPolicy)
            policy.set_ready_replicas([one_gpu, four_gpu])
            policy.set_replica_info({
                one_gpu: {
                    'gpu_type': 'L4',
                    'gpu_count': '1'
                },
                four_gpu: {
                    'gpu_type': 'L4',
                    'gpu_count': '4'
                },
            })
            publish_current_occupancy_snapshot(lb,
                                               occupancy={
                                                   one_gpu: 0,
                                                   four_gpu: 0
                                               },
                                               total_slots=capacities,
                                               free_slots=capacities)
            lb._occupancy_declared_urls = {one_gpu, four_gpu}
            policy.set_occupancy({one_gpu: 0, four_gpu: 0})

        async def _fast_ack(url, request):
            del request
            assert counts[url] < capacities[url]
            counts[url] += 1
            return fastapi.responses.Response(status_code=202)

        lb._proxy_request_to = _fast_ack
        # Deterministic initial tie: the one-GPU replica fills first; GPU-count
        # normalization plus strict eligibility then sends four to the 4-GPU
        # replica, reaching equal normalized load at full capacity.
        with mock.patch.object(load_balancing_policies.random,
                               'choice',
                               side_effect=lambda values: values[0]):
            for _ in range(5):
                assert (await
                        lb._proxy_with_retries(_request())).status_code == 202

        assert counts == {one_gpu: 1, four_gpu: 4}
        assert lb._occupancy_pending_reservations == {
            one_gpu: 1,
            four_gpu: 4,
        }
        with lb._client_pool_lock:
            assert lb._effective_replica_free_slots_locked() == {
                one_gpu: 0,
                four_gpu: 0,
            }

    asyncio.run(_run())


def test_unassigned_admissions_close_selection_scheduling_gap():

    async def _run():
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      max_concurrency_per_replica=8,
                      max_concurrency=8,
                      timeout_seconds=1,
                      use_async_occupancy=True)
        url = 'http://two-slots:8000'
        lb._load_balancing_policy.set_ready_replicas([url])
        publish_current_occupancy_snapshot(lb,
                                           occupancy={url: 0},
                                           total_slots={url: 2},
                                           free_slots={url: 2})
        requests = [_request() for _ in range(3)]

        assert await lb._acquire_request_slot(requests[0]) is True
        assert await lb._acquire_request_slot(requests[1]) is True
        assert lb._occupancy_unassigned_reservations == 2
        assert lb._request_queue_limits()[0] == 2

        third = asyncio.create_task(lb._acquire_request_slot(requests[2]))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        assert not third.done()

        await lb._release_request_slot(requests[0])
        assert await third is True
        assert lb._active_request_count == 2
        assert lb._occupancy_unassigned_reservations == 2
        await lb._release_request_slot(requests[1])
        await lb._release_request_slot(requests[2])
        assert lb._active_request_count == 0
        assert lb._occupancy_unassigned_reservations == 0

    asyncio.run(_run())


def test_slow_proxy_does_not_hold_reservation_locks():

    async def _run():
        lb = _make_lb(min_size=0,
                      size_per_replica=0,
                      max_concurrency_per_replica=4,
                      max_concurrency=4,
                      use_async_occupancy=True)
        url = 'http://four-slots:8000'
        lb._load_balancing_policy.set_ready_replicas([url])
        lb._occupancy_declared_urls = {url}
        publish_current_occupancy_snapshot(lb,
                                           occupancy={url: 0},
                                           total_slots={url: 4},
                                           free_slots={url: 4})
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_started = asyncio.Event()
        calls = 0

        async def _proxy(replica_url, request):
            nonlocal calls
            del replica_url, request
            calls += 1
            if calls == 1:
                first_started.set()
                await release_first.wait()
            else:
                second_started.set()
            return fastapi.responses.Response(status_code=202)

        lb._proxy_request_to = _proxy
        first = asyncio.create_task(lb._proxy_with_retries(_request()))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        second = asyncio.create_task(lb._proxy_with_retries(_request()))
        await asyncio.wait_for(second_started.wait(), timeout=1)
        assert (await second).status_code == 202
        release_first.set()
        assert (await first).status_code == 202
        assert lb._occupancy_pending_reservations == {url: 2}

    asyncio.run(_run())


def test_policy_swap_during_attempt_preserves_pending_capacity():

    async def _run():
        url = 'http://two-slots:8000'
        lb = _make_lb(min_size=0,
                      size_per_replica=0,
                      max_concurrency_per_replica=2,
                      max_concurrency=2,
                      use_async_occupancy=True)
        lb._load_balancing_policy.set_ready_replicas([url])
        lb._occupancy_declared_urls = {url}
        publish_current_occupancy_snapshot(lb,
                                           occupancy={url: 0},
                                           total_slots={url: 2},
                                           free_slots={url: 2})
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        calls = 0

        async def _proxy(replica_url, request):
            nonlocal calls
            del replica_url, request
            calls += 1
            if calls == 1:
                first_started.set()
                await release_first.wait()
            return fastapi.responses.Response(status_code=202)

        lb._proxy_request_to = _proxy
        first = asyncio.create_task(lb._proxy_with_retries(_request()))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        with lb._client_pool_lock:
            lb._apply_routing_spec({
                'load_balancing_policy_name': 'instance_aware_least_load',
                'target_concurrency_per_replica': 1,
                'request_queue': _queue_config(min_size=0,
                                               size_per_replica=0,
                                               max_concurrency_per_replica=2,
                                               max_concurrency=2,
                                               use_async_occupancy=True),
            })
            policy = lb._load_balancing_policy
            assert isinstance(
                policy, load_balancing_policies.InstanceAwareLeastLoadPolicy)
            policy.set_ready_replicas([url])
            policy.set_replica_info({url: {'gpu_type': 'L4', 'gpu_count': '2'}})
            policy.set_occupancy(lb._effective_occupancy_locked())
        assert policy.occupancy_map == {url: 1}

        second = await lb._proxy_with_retries(_request())
        assert second.status_code == 202
        assert policy.occupancy_map == {url: 2}
        release_first.set()
        assert (await first).status_code == 202
        assert lb._occupancy_pending_reservations == {url: 2}
        assert lb._occupancy_active_attempts == {}
        assert lb._active_request_count == 0
        assert lb._queue_depth == 0

    asyncio.run(_run())


def test_retriable_rejection_keeps_admitted_slot_until_next_selection():

    async def _run():
        first_url = 'http://first:8000'
        second_url = 'http://second:8000'
        lb = _make_lb(min_size=0,
                      size_per_replica=0,
                      max_concurrency_per_replica=1,
                      max_concurrency=2,
                      use_async_occupancy=True)
        lb._load_balancing_policy.set_ready_replicas([first_url, second_url])
        lb._occupancy_declared_urls = {first_url, second_url}
        slots = {first_url: 1, second_url: 1}
        publish_current_occupancy_snapshot(lb,
                                           occupancy={
                                               first_url: 0,
                                               second_url: 0
                                           },
                                           total_slots=slots,
                                           free_slots=slots)
        lb._load_balancing_policy.set_occupancy({first_url: 0, second_url: 0})
        request = _request()
        request.is_disconnected = mock.AsyncMock(return_value=False)
        attempts = []

        async def _proxy(url, forwarded_request):
            del forwarded_request
            attempts.append(url)
            if url == first_url:
                return load_balancer._RetriableStatusError(429, url)
            return fastapi.responses.Response(status_code=202)

        async def _between_attempts(delay):
            del delay
            # The first URL's stale baseline is invalidated, but this admitted
            # request still owns one fleet slot while it retries. Therefore no
            # third request can be admitted against the second URL's sole slot.
            assert lb._occupancy_pending_reservations == {}
            assert lb._occupancy_unassigned_reservations == 1
            assert lb._has_unassigned_occupancy_admission(request)
            assert lb._request_queue_limits()[0] == 1

        lb._proxy_request_to = _proxy
        with mock.patch.object(
                load_balancing_policies.random,
                'choice',
                side_effect=lambda values: values[0]), mock.patch(
                    'sky.serve.load_balancer.asyncio.sleep',
                    side_effect=_between_attempts):
            response = await lb._proxy_with_retries(request)

        assert response.status_code == 202
        assert attempts == [first_url, second_url]
        assert lb._occupancy_unassigned_reservations == 0
        assert lb._occupancy_pending_reservations == {second_url: 1}

    asyncio.run(_run())


def test_enabling_occupancy_queue_mid_request_does_not_leak_admission():

    async def _run():
        first_url = 'http://first:8000'
        second_url = 'http://second:8000'
        lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 8890)
        lb._load_balancing_policy.set_ready_replicas([first_url, second_url])
        lb._occupancy_declared_urls = {first_url, second_url}
        slots = {first_url: 1, second_url: 1}
        publish_current_occupancy_snapshot(lb,
                                           occupancy={
                                               first_url: 0,
                                               second_url: 0
                                           },
                                           total_slots=slots,
                                           free_slots=slots)
        lb._load_balancing_policy.set_occupancy({first_url: 0, second_url: 0})
        request = _request()
        request.is_disconnected = mock.AsyncMock(return_value=False)
        attempts = []

        async def _proxy(url, forwarded_request):
            del forwarded_request
            attempts.append(url)
            if url == first_url:
                return load_balancer._RetriableStatusError(429, url)
            return fastapi.responses.Response(status_code=400)

        async def _enable_queue(delay):
            del delay
            # This request entered while queueing was disabled, but still owns
            # the process-local admission used for HA drain accounting. Enable
            # occupancy-aware queueing in its retry gap to reproduce a live
            # service update.
            assert lb._occupancy_unassigned_reservations == 0
            lb._apply_routing_spec({
                'request_queue': _queue_config(min_size=0,
                                               size_per_replica=0,
                                               max_concurrency_per_replica=1,
                                               max_concurrency=2,
                                               use_async_occupancy=True),
            })

        lb._proxy_request_to = _proxy
        lb._notify_request_queue = mock.AsyncMock()
        with mock.patch.object(
                load_balancing_policies.random,
                'choice',
                side_effect=lambda values: values[0]), mock.patch(
                    'sky.serve.load_balancer.asyncio.sleep',
                    side_effect=_enable_queue):
            response = await lb._proxy_with_retries(request)

        assert response.status_code == 400
        assert attempts == [first_url, second_url]
        assert lb._occupancy_unassigned_reservations == 0
        assert not lb._has_unassigned_occupancy_admission(request)
        assert lb._active_request_count == 0

    asyncio.run(_run())


@pytest.mark.parametrize(('initial_occupancy', 'updated_occupancy'),
                         [(False, True), (True, False)])
def test_live_queue_mode_toggle_preserves_retry_ownership(
        initial_occupancy, updated_occupancy):

    async def _run():
        first_url = 'http://first:8000'
        second_url = 'http://second:8000'
        lb = _make_lb(min_size=0,
                      size_per_replica=0,
                      max_concurrency_per_replica=1,
                      max_concurrency=2,
                      use_async_occupancy=initial_occupancy)
        lb._load_balancing_policy.set_ready_replicas([first_url, second_url])
        lb._occupancy_declared_urls = {first_url, second_url}
        slots = {first_url: 1, second_url: 1}
        publish_current_occupancy_snapshot(lb,
                                           occupancy={
                                               first_url: 0,
                                               second_url: 0
                                           },
                                           total_slots=slots,
                                           free_slots=slots)
        request = _request()
        request.is_disconnected = mock.AsyncMock(return_value=False)
        attempts = []

        async def _proxy(url, forwarded_request):
            del forwarded_request
            attempts.append(url)
            if url == first_url:
                return load_balancer._RetriableStatusError(429, url)
            return fastapi.responses.Response(status_code=400)

        async def _toggle_queue_mode(delay):
            del delay
            with lb._client_pool_lock:
                lb._apply_routing_spec({
                    'request_queue': _queue_config(
                        min_size=0,
                        size_per_replica=0,
                        max_concurrency_per_replica=1,
                        max_concurrency=2,
                        use_async_occupancy=updated_occupancy),
                })

        lb._proxy_request_to = _proxy
        with mock.patch.object(
                load_balancing_policies.random,
                'choice',
                side_effect=lambda values: values[0]), mock.patch(
                    'sky.serve.load_balancer.asyncio.sleep',
                    side_effect=_toggle_queue_mode):
            response = await lb._proxy_with_retries(request)

        assert response.status_code == 400
        assert attempts == [first_url, second_url]
        assert lb._active_request_count == 0
        assert lb._queue_depth == 0
        assert lb._occupancy_unassigned_reservations == 0
        assert not lb._has_unassigned_occupancy_admission(request)
        assert lb._occupancy_active_attempts == {}
        assert lb._occupancy_pending_reservations == {}

    asyncio.run(_run())


def test_legacy_queue_config_defaults_to_envelope_admission():
    lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 8890)
    config = _queue_config()
    config.pop('use_async_occupancy')
    lb._apply_routing_spec({'request_queue': config})
    lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
    assert lb._request_queue_limits() == (1, 10)


def test_dispatch_concurrency_has_absolute_cap():
    lb = _make_lb(max_concurrency_per_replica=4, max_concurrency=7)
    lb._load_balancing_policy.set_ready_replicas(
        [f'http://worker-{i}:8000' for i in range(3)])
    assert lb._request_queue_limits()[0] == 7


@pytest.mark.parametrize(('raw_headers', 'expected'), [
    ([], 0),
    ([(b'x-skyserve-priority', b'0')], 0),
    ([(b'X-SkyServe-Priority', b'37')], 37),
    ([(b'x-skyserve-priority', b'100')], 100),
    ([(b'x-skyserve-priority', b'007')], 7),
    ([(b'x-skyserve-priority', b'0100')], 100),
    ([(b'x-skyserve-priority', b'0' * 5000)], 0),
])
def test_request_priority_header_parsing(raw_headers, expected):
    request = _request_with_headers(raw_headers)
    assert (load_balancer.SkyServeLoadBalancer._parse_request_priority(request)
            == expected)


@pytest.mark.parametrize('raw_headers', [
    [(b'x-skyserve-priority', b'')],
    [(b'x-skyserve-priority', b'-1')],
    [(b'x-skyserve-priority', b' 1')],
    [(b'x-skyserve-priority', b'1.0')],
    [(b'x-skyserve-priority', b'101')],
    [(b'x-skyserve-priority', b'1' * 5000)],
    [(b'x-skyserve-priority', b'\xff')],
    [(b'x-skyserve-priority', b'1'), (b'X-SkyServe-Priority', b'2')],
])
def test_invalid_request_priority_header_returns_400(raw_headers):
    request = _request_with_headers(raw_headers)
    with pytest.raises(fastapi.HTTPException) as exc:
        load_balancer.SkyServeLoadBalancer._parse_request_priority(request)
    assert exc.value.status_code == 400


def test_request_priority_header_is_consumed_before_proxying():
    request = _request_with_headers([
        (b'x-duplicate', b'first'),
        (b'X-SkyServe-Priority', b'99'),
        (b'x-duplicate', b'second'),
        (b'x-skyserve-priority', b'1'),
    ])
    assert (load_balancer.SkyServeLoadBalancer.
            _headers_without_request_priority(request) == [
                (b'x-duplicate', b'first'),
                (b'x-duplicate', b'second'),
            ])


def test_request_accelerator_header_parsing_and_default():
    lb = _make_lb()
    lb._apply_routing_spec({
        'request_queue': _queue_config(),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100', 'A100-80GB'],
    })
    omitted = _request_with_headers([])
    assert lb._parse_request_accelerators(omitted) == ('L4', 'A100',
                                                       'A100-80GB')
    explicit = _request_with_headers([
        (b'x-skyserve-compatible-accelerators', b'l4, A100-80GB'),
    ])
    assert lb._parse_request_accelerators(explicit) == ('L4', 'A100-80GB')


@pytest.mark.parametrize('value', [b'', b'L4,', b'L4,l4', b'A100-40GB'])
def test_invalid_request_accelerator_header_returns_400(value):
    lb = _make_lb()
    lb._apply_routing_spec({
        'request_queue': _queue_config(),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100', 'A100-80GB'],
    })
    request = _request_with_headers([
        (b'x-skyserve-compatible-accelerators', value),
    ])
    with pytest.raises(fastapi.HTTPException) as exc:
        lb._parse_request_accelerators(request)
    assert exc.value.status_code == 400


def test_explicit_accelerators_fail_closed_before_capability_sync():
    lb = _make_lb()
    request = _request_with_headers([
        (b'x-skyserve-compatible-accelerators', b'L4,A100'),
    ])
    with pytest.raises(fastapi.HTTPException) as exc:
        lb._parse_request_accelerators(request)
    assert exc.value.status_code == 503


def test_accelerator_header_is_consumed_before_proxying():
    request = _request_with_headers([
        (b'x-keep', b'value'),
        (b'x-skyserve-compatible-accelerators', b'L4,A100'),
        (b'x-skyserve-priority', b'50'),
    ])
    assert (load_balancer.SkyServeLoadBalancer.
            _headers_without_request_priority(request) == [(b'x-keep', b'value')
                                                          ])


def test_equal_priority_queue_preserves_scarce_card_matching():
    lb = _make_lb(max_concurrency=2)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=2),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100', 'H100'],
    })
    lb._replica_info_by_url = {
        'a100': {
            'gpu_type': 'A100',
            'is_zero_cost': 'false',
        },
        'h100': {
            'gpu_type': 'H100',
            'is_zero_cost': 'false',
        },
    }
    lb._load_balancing_policy.set_ready_replicas(['a100', 'h100'])
    loop = asyncio.new_event_loop()
    try:
        flexible = _request()
        setattr(flexible, '_skyserve_compatible_accelerators', ('L4', 'A100'))
        larger = _request()
        setattr(larger, '_skyserve_compatible_accelerators', ('A100', 'H100'))
        first = load_balancer._RequestQueueWaiter(flexible, 50, 0,
                                                  loop.create_future())
        second = load_balancer._RequestQueueWaiter(larger, 50, 1,
                                                   loop.create_future())
        lb._request_queue_waiters = {50: {0: first, 1: second}}
        lb._waiting_request_count = 2
        lb._dispatch_request_queue_locked()
        assert getattr(flexible, '_skyserve_granted_accelerator') == 'A100'
        assert getattr(larger, '_skyserve_granted_accelerator') == 'H100'
    finally:
        loop.close()


def test_late_a100_only_request_gets_next_a100_after_1000_flexible_waiters():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1, max_size=10000),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100'],
    })
    lb._replica_info_by_url = {
        'a100': {
            'gpu_type': 'A100',
            'is_zero_cost': 'false',
        },
    }
    lb._load_balancing_policy.set_ready_replicas(['a100'])
    loop = asyncio.new_event_loop()
    try:
        bucket = {}
        flexible_waiters = []
        for sequence in range(1000):
            request = _request()
            setattr(request, '_skyserve_compatible_accelerators',
                    ('L4', 'A100'))
            waiter = load_balancer._RequestQueueWaiter(request, 50, sequence,
                                                       loop.create_future())
            bucket[sequence] = waiter
            flexible_waiters.append(waiter)
        a100_request = _request()
        setattr(a100_request, '_skyserve_compatible_accelerators', ('A100',))
        constrained = load_balancer._RequestQueueWaiter(a100_request, 50, 1000,
                                                        loop.create_future())
        bucket[constrained.sequence] = constrained
        lb._request_queue_waiters = {50: bucket}
        lb._waiting_request_count = len(bucket)

        lb._dispatch_request_queue_locked()

        assert constrained.granted
        assert getattr(a100_request, '_skyserve_granted_accelerator') == 'A100'
        assert not any(waiter.granted for waiter in flexible_waiters)
        assert lb._waiting_request_count == 1000
    finally:
        loop.close()


def test_queue_prefers_ready_reserved_compatible_card():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100'],
    })
    lb._replica_info_by_url = {
        'l4': {
            'gpu_type': 'L4',
            'is_zero_cost': 'false',
        },
        'a100': {
            'gpu_type': 'A100',
            'is_zero_cost': 'true',
        },
    }
    lb._load_balancing_policy.set_ready_replicas(['l4', 'a100'])
    loop = asyncio.new_event_loop()
    try:
        request = _request()
        setattr(request, '_skyserve_compatible_accelerators', ('L4', 'A100'))
        waiter = load_balancer._RequestQueueWaiter(request, 50, 0,
                                                   loop.create_future())
        lb._request_queue_waiters = {50: {0: waiter}}
        lb._waiting_request_count = 1
        lb._dispatch_request_queue_locked()
        assert getattr(request, '_skyserve_granted_accelerator') == 'A100'
    finally:
        loop.close()


def test_queue_consumes_reserved_card_preference_once_per_ready_slot():
    lb = _make_lb(max_concurrency=2)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=2),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100'],
    })
    lb._replica_info_by_url = {
        'l4-paid': {
            'gpu_type': 'L4',
            'is_zero_cost': 'false',
        },
        'a100-reserved': {
            'gpu_type': 'A100',
            'is_zero_cost': 'true',
        },
        'a100-paid': {
            'gpu_type': 'A100',
            'is_zero_cost': 'false',
        },
    }
    lb._load_balancing_policy.set_ready_replicas(
        ['l4-paid', 'a100-reserved', 'a100-paid'])
    loop = asyncio.new_event_loop()
    try:
        requests = [_request(), _request()]
        waiters = {}
        for sequence, request in enumerate(requests):
            setattr(request, '_skyserve_compatible_accelerators',
                    ('L4', 'A100'))
            waiters[sequence] = load_balancer._RequestQueueWaiter(
                request, 50, sequence, loop.create_future())
        lb._request_queue_waiters = {50: waiters}
        lb._waiting_request_count = 2

        lb._dispatch_request_queue_locked()

        assert getattr(requests[0], '_skyserve_granted_accelerator') == 'A100'
        assert getattr(requests[1], '_skyserve_granted_accelerator') == 'L4'
    finally:
        loop.close()


def test_zero_dispatch_capacity_skips_compatibility_matching():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100'],
    })
    loop = asyncio.new_event_loop()
    try:
        request = _request()
        setattr(request, '_skyserve_compatible_accelerators', ('A100',))
        waiter = load_balancer._RequestQueueWaiter(request, 50, 0,
                                                   loop.create_future())
        lb._request_queue_waiters = {50: {0: waiter}}
        lb._waiting_request_count = 1
        lb._active_request_count = 1

        with mock.patch.object(
                lb,
                '_build_request_queue_grant_plan_locked',
                side_effect=AssertionError('matching should be skipped')):
            lb._dispatch_request_queue_locked()

        assert not waiter.granted
        assert lb._waiting_request_count == 1
    finally:
        loop.close()


def test_dense_compatibility_matching_is_bounded_by_profiles_and_slots():
    cards = tuple(f'GPU-{index}' for index in range(8))
    lb = _make_lb(max_concurrency=128, max_size=1000)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=128, max_size=1000),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': list(cards),
    })
    loop = asyncio.new_event_loop()
    try:
        waiters = {}
        for sequence in range(500):
            request = _request()
            mask = 1 + sequence % 255
            compatible = tuple(
                card for index, card in enumerate(cards) if mask & (1 << index))
            setattr(request, '_skyserve_compatible_accelerators', compatible)
            waiters[sequence] = load_balancer._RequestQueueWaiter(
                request, 50, sequence, loop.create_future())
        lb._request_queue_waiters = {50: waiters}
        lb._waiting_request_count = len(waiters)

        profile_count = len({
            getattr(waiter.request, '_skyserve_compatible_accelerators')
            for waiter in waiters.values()
        })
        with mock.patch.object(
                load_balancer.heapq,
                'heappush',
                wraps=load_balancer.heapq.heappush) as heappush, \
             mock.patch.object(
                 load_balancer.heapq,
                 'heappop',
                 wraps=load_balancer.heapq.heappop) as heappop:
            plan = lb._build_request_queue_grant_plan_locked(
                {card: 16 for card in cards}, {card: 0 for card in cards}, 128)

        assert len(plan) == 128
        assert profile_count == 255
        # Pin the algorithmic bound without depending on wall-clock timing on
        # a shared CI runner: one initial push per profile, then at most one
        # pop and one requeue per granted slot.
        assert heappop.call_count <= 128
        assert heappush.call_count <= profile_count + 128
    finally:
        loop.close()


def test_queue_uses_fallback_quality_not_raw_compatibility_count():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100', 'H100'],
    })
    lb._replica_info_by_url = {
        'a100': {
            'gpu_type': 'A100',
            'is_zero_cost': 'false',
        },
    }
    lb._load_balancing_policy.set_ready_replicas(['a100'])
    loop = asyncio.new_event_loop()
    try:
        cheaper_fallback = _request()
        setattr(cheaper_fallback, '_skyserve_compatible_accelerators',
                ('L4', 'A100'))
        worse_fallback = _request()
        setattr(worse_fallback, '_skyserve_compatible_accelerators',
                ('A100', 'H100'))
        older = load_balancer._RequestQueueWaiter(cheaper_fallback, 50, 0,
                                                  loop.create_future())
        newer = load_balancer._RequestQueueWaiter(worse_fallback, 50, 1,
                                                  loop.create_future())
        lb._request_queue_waiters = {50: {0: older, 1: newer}}
        lb._waiting_request_count = 2

        lb._dispatch_request_queue_locked()

        assert not older.granted
        assert newer.granted
        assert getattr(worse_fallback,
                       '_skyserve_granted_accelerator') == 'A100'
    finally:
        loop.close()


def test_numeric_priority_dominates_accelerator_scarcity():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100'],
    })
    lb._replica_info_by_url = {
        'a100': {
            'gpu_type': 'A100',
            'is_zero_cost': 'false',
        },
    }
    lb._load_balancing_policy.set_ready_replicas(['a100'])
    loop = asyncio.new_event_loop()
    try:
        high_flexible = _request()
        setattr(high_flexible, '_skyserve_compatible_accelerators',
                ('L4', 'A100'))
        low_constrained = _request()
        setattr(low_constrained, '_skyserve_compatible_accelerators', ('A100',))
        high = load_balancer._RequestQueueWaiter(high_flexible, 50, 1,
                                                 loop.create_future())
        low = load_balancer._RequestQueueWaiter(low_constrained, 20, 0,
                                                loop.create_future())
        lb._request_queue_waiters = {20: {0: low}, 50: {1: high}}
        lb._waiting_request_count = 2

        lb._dispatch_request_queue_locked()

        assert high.granted
        assert not low.granted
    finally:
        loop.close()


def test_aggregate_free_slot_does_not_admit_incompatible_request():

    async def _run():
        lb = _make_lb(max_concurrency=1, timeout_seconds=5)
        lb._apply_routing_spec({
            'request_queue': _queue_config(max_concurrency=1,
                                           timeout_seconds=5),
            'request_accelerator_compatibility_version': 1,
            'configured_accelerators': ['L4', 'A100'],
        })
        lb._replica_info_by_url = {
            'l4': {
                'gpu_type': 'L4',
                'is_zero_cost': 'false',
            },
        }
        lb._load_balancing_policy.set_ready_replicas(['l4'])
        request = _request()
        setattr(request, '_skyserve_compatible_accelerators', ('A100',))
        acquire = asyncio.create_task(lb._acquire_request_slot(request, 50))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        assert not acquire.done()
        assert lb._active_request_count == 0
        acquire.cancel()
        with pytest.raises(asyncio.CancelledError):
            await acquire
        await _wait_until(lambda: lb._waiting_request_count == 0)

    asyncio.run(_run())


def test_capable_controller_with_missing_replica_identity_fails_closed():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100'],
    })
    lb._load_balancing_policy.set_ready_replicas(['identity-not-synced'])
    request = _request()
    setattr(request, '_skyserve_compatible_accelerators', ('L4',))
    assert lb._request_queue_accelerator_slots_locked() == ({
        'L4': 0,
        'A100': 0,
    }, {
        'L4': 0,
        'A100': 0,
    })
    assert not lb._reserve_immediate_accelerator_locked(request)


def test_empty_fleet_records_compatibility_demand_before_admission():

    async def _run():
        lb = _make_lb(max_concurrency=1, timeout_seconds=5)
        lb._queued_compatibility_demand_supported = True
        lb._apply_routing_spec({
            'request_queue': _queue_config(max_concurrency=1,
                                           timeout_seconds=5),
            'request_accelerator_compatibility_version': 1,
            'configured_accelerators': ['L4', 'A100'],
        })
        request = _request_with_headers([
            (b'x-skyserve-compatible-accelerators', b'A100'),
            (b'x-skyserve-priority', b'50'),
        ])
        request.body = mock.AsyncMock(return_value=b'')
        proxy = asyncio.create_task(lb._proxy_with_retries(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        assert lb._request_aggregator.request_history_snapshot() is None
        assert lb._request_queue_profiles() == [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 1,
        }]
        proxy.cancel()
        with pytest.raises(asyncio.CancelledError):
            await proxy
        assert lb._request_queue_profiles() == []

    asyncio.run(_run())


def test_service_update_intersects_or_rejects_queued_compatibility():
    lb = _make_lb(max_concurrency=1)
    lb._apply_routing_spec({
        'request_queue': _queue_config(max_concurrency=1),
        'request_accelerator_compatibility_version': 1,
        'configured_accelerators': ['L4', 'A100', 'H100'],
    })
    loop = asyncio.new_event_loop()
    try:
        surviving_request = _request()
        setattr(surviving_request, '_skyserve_compatible_accelerators',
                ('L4', 'A100'))
        rejected_request = _request()
        setattr(rejected_request, '_skyserve_compatible_accelerators',
                ('H100',))
        surviving = load_balancer._RequestQueueWaiter(surviving_request, 50, 0,
                                                      loop.create_future())
        rejected = load_balancer._RequestQueueWaiter(rejected_request, 50, 1,
                                                     loop.create_future())
        lb._request_queue_waiters = {50: {0: surviving, 1: rejected}}
        lb._waiting_request_count = 2

        lb._apply_routing_spec({
            'request_queue': _queue_config(max_concurrency=1),
            'request_accelerator_compatibility_version': 1,
            'configured_accelerators': ['A100'],
        })

        assert getattr(surviving_request,
                       '_skyserve_compatible_accelerators') == ('A100',)
        assert surviving.sequence in lb._request_queue_waiters[50]
        assert rejected.terminal_error is not None
        assert rejected.terminal_error.status_code == 503
        assert rejected.future.done()
        assert lb._waiting_request_count == 1
    finally:
        loop.close()


def test_instance_aware_policy_prefers_reserved_only_on_load_tie():
    policy = load_balancing_policies.InstanceAwareLeastLoadPolicy()
    policy.set_ready_replicas(['paid-l4', 'reserved-a100'])
    policy.set_target_qps_per_accelerator({'L4': 1, 'A100': 1})
    policy.set_replica_info({
        'paid-l4': {
            'gpu_type': 'L4',
            'gpu_count': '1',
            'is_zero_cost': 'false',
        },
        'reserved-a100': {
            'gpu_type': 'A100',
            'gpu_count': '1',
            'is_zero_cost': 'true',
        },
    })
    assert policy.select_replica(_request()) == 'reserved-a100'
    policy.load_map['reserved-a100'] = 1
    assert policy.select_replica(_request()) == 'paid-l4'


def test_queue_disabled_still_validates_request_priority():

    async def _run():
        lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 8890)
        lb._proxy_with_retries_inner = mock.AsyncMock(
            return_value=fastapi.responses.Response(status_code=200))
        invalid = _request_with_headers([(b'x-skyserve-priority', b'101')])
        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._proxy_with_retries(invalid)
        assert exc.value.status_code == 400
        lb._proxy_with_retries_inner.assert_not_awaited()
        assert lb._active_request_count == 0

        valid = _request_with_headers([(b'x-skyserve-priority', b'91')])
        response = await lb._proxy_with_retries(valid)
        assert response.status_code == 200
        assert vars(valid)['_skyserve_request_priority'] == 91
        assert lb._active_request_count == 0

    asyncio.run(_run())


def test_strict_priority_with_fifo_ties():

    async def _run():
        lb = _make_lb(min_size=4,
                      size_per_replica=0,
                      max_size=4,
                      timeout_seconds=5)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        queued: list[tuple[str, asyncio.Task]] = []

        async def _enqueue(name: str, priority: int) -> None:
            task = asyncio.create_task(
                lb._acquire_request_slot(_request(), priority))
            queued.append((name, task))
            await _wait_until(lambda: lb._waiting_request_count == len(queued))

        await _enqueue('low', 1)
        await _enqueue('high-first', 100)
        await _enqueue('high-second', 100)
        await _enqueue('medium', 50)

        order = []
        await lb._release_request_slot()
        for _ in queued:
            await _wait_until(lambda: any(
                task.done() and name not in order for name, task in queued))
            completed = [(name, task)
                         for name, task in queued
                         if task.done() and name not in order]
            assert len(completed) == 1
            name, task = completed[0]
            assert await task is True
            order.append(name)
            await lb._release_request_slot()

        assert order == ['high-first', 'high-second', 'medium', 'low']
        assert lb._active_request_count == 0
        assert lb._waiting_request_count == 0

    asyncio.run(_run())


def test_late_higher_priority_registration_uses_existing_capacity():

    async def _run():
        lb = _make_lb(min_size=2,
                      size_per_replica=0,
                      max_size=2,
                      timeout_seconds=5)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        low = asyncio.create_task(lb._acquire_request_slot(_request(), 1))
        await _wait_until(lambda: lb._waiting_request_count == 1)

        # Model capacity becoming available immediately before registration,
        # while its normal notification is still waiting to take the scheduler
        # lock. Registration itself must run the central dispatcher and select
        # the new highest-priority head.
        lb._active_request_count = 0
        high = asyncio.create_task(lb._acquire_request_slot(_request(), 100))
        assert await high is True
        assert not low.done()
        assert lb._active_request_count == 1
        assert lb._waiting_request_count == 1

        await lb._release_request_slot()
        assert await low is True
        await lb._release_request_slot()

    asyncio.run(_run())


def test_priority_is_non_preemptive():

    async def _run():
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      timeout_seconds=5)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        low_request = _request()
        assert await lb._acquire_request_slot(low_request, 1) is True
        high = asyncio.create_task(lb._acquire_request_slot(_request(), 100))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        assert not high.done()
        await lb._release_request_slot(low_request)
        assert await high is True
        await lb._release_request_slot()

    asyncio.run(_run())


def test_full_queue_does_not_evict_lower_priority_waiter():

    async def _run():
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      timeout_seconds=5)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        low = asyncio.create_task(lb._acquire_request_slot(_request(), 1))
        await _wait_until(lambda: lb._waiting_request_count == 1)

        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._acquire_request_slot(_request(), 100)
        assert exc.value.status_code == 503
        assert not low.done()
        assert lb._waiting_request_count == 1

        await lb._release_request_slot()
        assert await low is True
        await lb._release_request_slot()

    asyncio.run(_run())


def test_dispatch_resolves_only_newly_granted_waiters_at_scale():

    async def _run():
        lb = _make_lb(min_size=10000,
                      size_per_replica=0,
                      max_size=10000,
                      max_concurrency_per_replica=1,
                      max_concurrency=128)
        lb._load_balancing_policy.set_ready_replicas(
            [f'http://worker-{index}:8000' for index in range(128)])
        loop = asyncio.get_running_loop()
        waiters = []
        for sequence in range(10000):
            waiter = load_balancer._RequestQueueWaiter(
                request=_request(),
                priority=sequence % 101,
                sequence=sequence,
                future=loop.create_future())
            lb._request_queue_waiters.setdefault(waiter.priority,
                                                 {})[sequence] = waiter
            waiters.append(waiter)
        lb._request_queue_sequence = len(waiters)
        lb._waiting_request_count = len(waiters)

        async with lb._request_queue_condition:
            lb._dispatch_request_queue_locked()

        granted = [waiter for waiter in waiters if waiter.future.done()]
        assert len(granted) == 128
        assert all(waiter.granted for waiter in granted)
        assert min(waiter.priority for waiter in granted) == 99
        assert all(waiter.priority >= 99 for waiter in granted)
        assert lb._active_request_count == 128
        assert lb._waiting_request_count == 9872

    asyncio.run(_run())


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
        request = _request()
        waiter = asyncio.create_task(lb._acquire_request_slot(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        await lb._release_request_slot()
        assert await waiter is True
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 1

    asyncio.run(_run())


def test_controller_capability_rollback_backfills_waiting_demand_once():

    async def _run():
        lb = _make_lb(max_concurrency=1, timeout_seconds=5)
        lb._queued_compatibility_demand_supported = True
        lb._apply_routing_spec({
            'request_queue': _queue_config(max_concurrency=1,
                                           timeout_seconds=5),
            'request_accelerator_compatibility_version': 1,
            'configured_accelerators': ['L4', 'A100'],
        })
        request = _request_with_headers([
            (b'x-skyserve-compatible-accelerators', b'A100'),
            (b'x-skyserve-priority', b'50'),
        ])
        request.body = mock.AsyncMock(return_value=b'')
        proxy = asyncio.create_task(lb._proxy_with_retries(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        assert lb._request_aggregator.request_history_snapshot() is None

        lb._set_queued_compatibility_demand_support(False)
        first = lb._request_aggregator.request_history_snapshot()
        assert first is not None
        assert first['buckets'][0]['request_count'] == 1
        lb._set_queued_compatibility_demand_support(False)
        assert lb._request_aggregator.request_history_snapshot() == first

        proxy.cancel()
        with pytest.raises(asyncio.CancelledError):
            await proxy

    asyncio.run(_run())


def test_occupancy_probe_wakes_waiter():

    async def _run():
        lb = _make_lb(min_size=1, use_async_occupancy=True, timeout_seconds=1)
        url = 'http://worker:8000'
        lb._load_balancing_policy.set_ready_replicas([url])
        request = _request()
        waiter = asyncio.create_task(lb._acquire_request_slot(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)

        async def _free_slot(session, replica_url):
            del session
            assert replica_url == url
            return 0, 1, 1

        with mock.patch.object(lb,
                               '_fetch_replica_occupancy',
                               side_effect=_free_slot):
            await lb._probe_replica_occupancy_once()
        assert await waiter is True
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 1

    asyncio.run(_run())


def test_timeout_and_cancellation_remove_waiters():

    async def _run():
        lb = _make_lb(min_size=1,
                      use_async_occupancy=True,
                      timeout_seconds=0.01)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        request = mock.MagicMock()
        request.headers = {}
        request.is_disconnected = mock.AsyncMock(return_value=False)
        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._acquire_request_slot(request)
        assert exc.value.status_code == 503
        assert lb._waiting_request_count == 0
        history = lb._request_aggregator.request_history_snapshot()
        assert history is not None
        assert history['buckets'][0]['request_count'] == 1
        assert lb._request_aggregator.request_classification_history_snapshot(
        )['buckets'][0] == {
            'bucket_start': history['buckets'][0]['bucket_start'],
            'classified_request_count': 1,
            'counted_rejected_count': 1,
        }

        lb._request_queue_config = {
            **(lb._request_queue_config or {}),
            'timeout_seconds': 1,
        }
        waiter = asyncio.create_task(lb._acquire_request_slot(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert lb._waiting_request_count == 0
        assert lb._request_aggregator.request_history_snapshot() == history

    asyncio.run(_run())


def test_full_queue_classifies_the_terminal_rejection():

    async def _run():
        lb = _make_lb(min_size=1, max_size=1)
        lb._waiting_request_count = 1

        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._acquire_request_slot(_request())

        assert exc.value.status_code == 503
        assert lb._request_aggregator.request_classification_history_snapshot(
        )['buckets'][0] == {
            'bucket_start': lb._request_aggregator.request_history_snapshot()
                            ['buckets'][0]['bucket_start'],
            'classified_request_count': 1,
            'counted_rejected_count': 1,
        }

    asyncio.run(_run())


def test_waiter_keeps_priority_timeout_selected_at_admission():

    async def _run():
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      max_concurrency=1,
                      timeout_seconds=1,
                      timeout_seconds_by_priority=[{
                          'min_priority': 50,
                          'timeout_seconds': 0.01,
                      }])
        lb._active_request_count = 1
        request = _request_with_headers([
            (constants.LB_REQUEST_PRIORITY_HEADER_BYTES, b'50')
        ])
        setattr(request, '_skyserve_request_priority', 50)
        with mock.patch.object(
                lb, '_request_queue_timeout',
                wraps=lb._request_queue_timeout) as timeout_resolver:
            waiter = asyncio.create_task(lb._acquire_request_slot(request))
            await _wait_until(lambda: lb._waiting_request_count == 1)
            lb._request_queue_config = {
                **(lb._request_queue_config or {}),
                'timeout_seconds_by_priority': [{
                    'min_priority': 50,
                    'timeout_seconds': 10,
                }],
            }
            with pytest.raises(fastapi.HTTPException) as exc:
                await waiter

        assert exc.value.status_code == 503
        timeout_resolver.assert_called_once()
        assert lb._waiting_request_count == 0

    asyncio.run(_run())


def test_offered_arrivals_deduplicate_stable_jobs_and_bound_headerless():
    lb = _make_lb()
    stable = _request()
    stable.headers = {constants.LB_JOB_ID_HEADER: 'job-secret'}
    headerless = _request()

    with mock.patch.object(load_balancer.time,
                           'monotonic',
                           side_effect=[100.0, 101.0, 102.0, 102.0]):
        lb._record_offered_arrival(stable)
        lb._record_offered_arrival(stable)
        lb._record_offered_arrival(headerless)
        counts = lb._offered_arrival_counts()

    assert counts == {
        'unique_job_arrivals_60s': 1,
        'unique_job_arrivals_300s': 1,
        'headerless_arrivals_60s': 1,
        'headerless_arrivals_300s': 1,
        'offered_arrival_tracking_saturated': False,
    }
    assert 'job-secret' not in (lb._offered_arrivals_by_job or {})


def test_offered_arrival_tracking_saturates_instead_of_evicting():
    lb = _make_lb()
    requests = []
    for index in range(3):
        request = _request()
        request.headers = {constants.LB_JOB_ID_HEADER: f'job-{index}'}
        requests.append(request)

    with mock.patch.object(constants, 'LB_OFFERED_ARRIVAL_CAP', 2), \
            mock.patch.object(load_balancer.time,
                              'monotonic',
                              return_value=100.0):
        for request in requests:
            lb._record_offered_arrival(request)
        counts = lb._offered_arrival_counts()

    assert counts['offered_arrival_tracking_saturated'] is True
    assert counts['unique_job_arrivals_300s'] == 2


def test_rejection_priority_bucket_tracks_latest_stable_job_priority():
    lb = _make_lb()
    request = _request()
    request.headers = {constants.LB_JOB_ID_HEADER: 'job-1'}
    setattr(request, '_skyserve_request_priority', 10)
    lb._record_rejection(request)
    setattr(request, '_skyserve_request_priority', 80)
    lb._record_rejection(request)

    assert lb._rejected_in_window() == 1
    assert lb._rejected_by_priority() == {'80': 1}


def test_disconnected_waiter_is_removed_without_dispatch():

    async def _run():
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      use_async_occupancy=True,
                      timeout_seconds=10)
        request = mock.MagicMock()
        request.headers = {}
        request.is_disconnected = mock.AsyncMock(return_value=True)
        lb._request_body = mock.AsyncMock(return_value=b'payload')
        lb._proxy_with_retries_inner = mock.AsyncMock()

        with mock.patch.object(load_balancer,
                               '_REQUEST_QUEUE_DISCONNECT_POLL_SECONDS', 0.001):
            with pytest.raises(fastapi.HTTPException) as exc:
                await lb._proxy_with_retries(request)

        assert exc.value.status_code == 499
        lb._request_body.assert_awaited_once_with(request)
        lb._proxy_with_retries_inner.assert_not_awaited()
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 0
        assert lb._queue_depth == 0
        assert lb._rejected_in_window() == 0

    asyncio.run(_run())


def test_disconnect_racing_slot_notification_does_not_dispatch():

    async def _run():
        lb = _make_lb(timeout_seconds=10)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        request = mock.MagicMock()
        request.headers = {}
        request.is_disconnected = mock.AsyncMock(return_value=False)
        lb._request_body = mock.AsyncMock(return_value=b'payload')
        lb._proxy_with_retries_inner = mock.AsyncMock()

        waiter = asyncio.create_task(lb._proxy_with_retries(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        request.is_disconnected.return_value = True
        await lb._release_request_slot()

        with pytest.raises(fastapi.HTTPException) as exc:
            await waiter
        assert exc.value.status_code == 499
        lb._proxy_with_retries_inner.assert_not_awaited()
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 0
        assert lb._queue_depth == 0

    asyncio.run(_run())


def test_cancellation_after_grant_reclaims_slot():

    async def _run():
        lb = _make_lb(timeout_seconds=10)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        request = _request()
        disconnect_check_started = asyncio.Event()
        never = asyncio.Event()

        async def _is_disconnected():
            disconnect_check_started.set()
            await never.wait()
            raise AssertionError('unreachable')

        request.is_disconnected.side_effect = _is_disconnected
        waiter = asyncio.create_task(lb._acquire_request_slot(request, 100))
        await _wait_until(lambda: lb._waiting_request_count == 1)

        await lb._release_request_slot()
        await asyncio.wait_for(disconnect_check_started.wait(), timeout=1)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert lb._active_request_count == 0
        assert lb._waiting_request_count == 0
        assert lb._occupancy_unassigned_reservations == 0
        assert not lb._background_tasks

    asyncio.run(_run())


def test_draining_waiter_is_rejected_without_dispatch():

    async def _run():
        lb = _make_lb(timeout_seconds=10)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        request = _request()
        lb._request_body = mock.AsyncMock(return_value=b'payload')
        lb._proxy_with_retries_inner = mock.AsyncMock()

        with mock.patch.object(load_balancer,
                               '_REQUEST_QUEUE_DISCONNECT_POLL_SECONDS', 0.001):
            waiter = asyncio.create_task(lb._proxy_with_retries(request))
            await _wait_until(lambda: lb._waiting_request_count == 1)
            lb._begin_draining()
            with pytest.raises(fastapi.HTTPException) as exc:
                await waiter

        assert exc.value.status_code == 503
        assert exc.value.headers['Retry-After']
        lb._proxy_with_retries_inner.assert_not_awaited()
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 1
        assert lb._queue_depth == 0
        await lb._release_request_slot()

    asyncio.run(_run())


def test_drain_racing_slot_notification_does_not_dispatch():

    async def _run():
        lb = _make_lb(timeout_seconds=10)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        lb._active_request_count = 1
        request = _request()
        disconnect_check_started = asyncio.Event()
        finish_disconnect_check = asyncio.Event()

        async def _is_disconnected():
            disconnect_check_started.set()
            await finish_disconnect_check.wait()
            return False

        request.is_disconnected.side_effect = _is_disconnected
        lb._request_body = mock.AsyncMock(return_value=b'payload')
        lb._proxy_with_retries_inner = mock.AsyncMock()

        waiter = asyncio.create_task(lb._proxy_with_retries(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        await lb._release_request_slot()
        await asyncio.wait_for(disconnect_check_started.wait(), timeout=1)
        lb._begin_draining()
        finish_disconnect_check.set()

        with pytest.raises(fastapi.HTTPException) as exc:
            await waiter
        assert exc.value.status_code == 503
        lb._proxy_with_retries_inner.assert_not_awaited()
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 0
        assert lb._queue_depth == 0

    asyncio.run(_run())


def test_repeated_cancellation_cannot_leak_admission_count():

    async def _run():
        lb = _make_lb(min_size=0,
                      size_per_replica=0,
                      max_concurrency_per_replica=1,
                      max_concurrency=1,
                      use_async_occupancy=True)
        url = 'http://worker:8000'
        lb._load_balancing_policy.set_ready_replicas([url])
        lb._occupancy_declared_urls = {url}
        publish_current_occupancy_snapshot(lb,
                                           occupancy={url: 0},
                                           total_slots={url: 1},
                                           free_slots={url: 1})
        started = asyncio.Event()
        never = asyncio.Event()

        async def _proxy(replica_url, request):
            del replica_url, request
            started.set()
            await never.wait()
            raise AssertionError('unreachable')

        lb._proxy_request_to = _proxy
        task = asyncio.create_task(lb._proxy_with_retries(_request()))
        await asyncio.wait_for(started.wait(), timeout=1)
        condition = lb._request_queue_condition
        assert condition is not None
        async with condition:
            # The first cancellation leaves the accepted async reservation
            # conservative, then enters the outer admission cleanup. Hold its
            # condition lock and cancel again to model disconnect plus server
            # shutdown arriving together.
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert lb._active_request_count == 0
        assert lb._queue_depth == 0
        assert lb._occupancy_unassigned_reservations == 0
        assert lb._occupancy_active_attempts == {}
        assert lb._occupancy_pending_reservations == {url: 1}

    asyncio.run(_run())


def test_repeated_cancellation_still_wakes_envelope_waiter():

    async def _run():
        lb = _make_lb(min_size=1,
                      size_per_replica=0,
                      max_size=1,
                      max_concurrency_per_replica=1,
                      max_concurrency=1,
                      timeout_seconds=5)
        url = 'http://worker:8000'
        lb._load_balancing_policy.set_ready_replicas([url])
        started = asyncio.Event()
        never = asyncio.Event()
        calls = 0

        async def _proxy(replica_url, request):
            nonlocal calls
            del replica_url, request
            calls += 1
            if calls == 1:
                started.set()
                await never.wait()
            return fastapi.responses.Response(status_code=200)

        lb._proxy_request_to = _proxy
        active = asyncio.create_task(lb._proxy_with_retries(_request()))
        await asyncio.wait_for(started.wait(), timeout=1)
        waiter = asyncio.create_task(lb._proxy_with_retries(_request()))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        condition = lb._request_queue_condition
        assert condition is not None
        async with condition:
            # Release state on the first cancellation, then interrupt the
            # condition notification. The already-registered envelope waiter
            # must still be woken after this lock is released.
            active.cancel()
            await asyncio.sleep(0)
            assert lb._active_request_count == 0
            active.cancel()
            await asyncio.sleep(0)
        with pytest.raises(asyncio.CancelledError):
            await active

        response = await asyncio.wait_for(waiter, timeout=1)
        assert response.status_code == 200
        await asyncio.sleep(0)
        assert calls == 2
        assert lb._active_request_count == 0
        assert lb._waiting_request_count == 0
        assert lb._queue_depth == 0
        assert not lb._background_tasks

    asyncio.run(_run())


def test_capacity_reports_request_queue_state():

    async def _run():
        lb = _make_lb(min_size=0,
                      size_per_replica=3,
                      max_size=3000,
                      use_async_occupancy=True)
        url = 'http://worker:8000'
        lb._load_balancing_policy.set_ready_replicas([url])
        publish_current_occupancy_snapshot(lb,
                                           occupancy={url: 0},
                                           total_slots={url: 1},
                                           free_slots={url: 1})
        lb._waiting_request_count = 2
        lb._queue_depth = 3
        lb._queue_depth_by_priority = {0: 2, 50: 1}
        response = await lb._capacity(mock.MagicMock())
        payload = json.loads(response.body)
        assert payload['request_queue_depth'] == 2
        assert payload['request_queue_capacity'] == 3
        assert payload['request_queue_dispatch_limit'] == 1
        assert payload['request_queue_submission_limit'] == 3032
        assert payload['request_queue_min_size'] == 0
        assert payload['request_queue_size_per_replica'] == 3
        assert payload['request_queue_max_size'] == 3000
        assert payload['request_queue_max_concurrency'] == 32
        assert payload['request_queue_max_request_body_bytes'] == 16
        assert payload['request_queue_timeout_seconds'] == 1
        assert payload['request_queue_uses_async_occupancy'] is True
        assert payload['queue_depth'] == 3
        assert payload['queue_depth_by_priority'] == {'0': 2, '50': 1}
        assert payload['unique_job_arrivals_60s'] == 0
        assert payload['unique_job_arrivals_300s'] == 0

    asyncio.run(_run())


def test_capacity_reports_positive_submission_limit_while_cold():

    async def _run():
        lb = _make_lb(min_size=200,
                      size_per_replica=10,
                      max_size=2000,
                      max_concurrency=128,
                      max_request_body_bytes=1048576,
                      timeout_seconds=3600,
                      use_async_occupancy=True)
        response = await lb._capacity(mock.MagicMock())
        payload = json.loads(response.body)

        assert payload['request_queue_capacity'] == 200
        assert payload['request_queue_depth'] == 0
        assert payload['request_queue_dispatch_limit'] == 0
        assert payload['request_queue_submission_limit'] == 2128
        assert payload['request_queue_min_size'] == 200
        assert payload['request_queue_size_per_replica'] == 10
        assert payload['request_queue_max_size'] == 2000
        assert payload['request_queue_max_concurrency'] == 128
        assert payload['request_queue_max_request_body_bytes'] == 1048576
        assert payload['request_queue_timeout_seconds'] == 3600
        assert payload['current_capacity'] == 0
        assert payload['free_slots'] == 0

    asyncio.run(_run())


def test_capacity_withholds_submission_limit_from_standby():

    async def _run():
        lb = _make_lb(min_size=200,
                      size_per_replica=10,
                      max_size=2000,
                      max_concurrency=128,
                      max_request_body_bytes=1048576,
                      timeout_seconds=3600,
                      use_async_occupancy=True)
        lb._lb_role = lb_ha.LbRole.STANDBY
        response = await lb._capacity(mock.MagicMock())
        payload = json.loads(response.body)

        assert payload['request_queue_capacity'] == 0
        assert payload['request_queue_dispatch_limit'] == 0
        assert payload['request_queue_submission_limit'] == 0
        # The inactive slot still reports the configured contract so clients
        # can diagnose a role mismatch without treating it as admission.
        assert payload['request_queue_min_size'] == 200
        assert payload['request_queue_size_per_replica'] == 10
        assert payload['request_queue_max_size'] == 2000
        assert payload['request_queue_max_concurrency'] == 128
        assert payload['request_queue_max_request_body_bytes'] == 1048576
        assert payload['request_queue_timeout_seconds'] == 3600

    asyncio.run(_run())


def test_capacity_reports_null_queue_contract_only_when_disabled():

    async def _run():
        lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 8890)
        response = await lb._capacity(mock.MagicMock())
        payload = json.loads(response.body)

        for field in ('request_queue_capacity', 'request_queue_dispatch_limit',
                      'request_queue_submission_limit',
                      'request_queue_min_size',
                      'request_queue_size_per_replica',
                      'request_queue_max_size', 'request_queue_max_concurrency',
                      'request_queue_max_request_body_bytes',
                      'request_queue_timeout_seconds',
                      'request_queue_uses_async_occupancy'):
            assert payload[field] is None

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
        first = asyncio.create_task(lb._proxy_with_retries(_request()))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        second = asyncio.create_task(lb._proxy_with_retries(_request()))
        await _wait_until(lambda: lb._waiting_request_count == 1)
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
        request = _request()
        waiter = asyncio.create_task(lb._acquire_request_slot(request))
        await _wait_until(lambda: lb._waiting_request_count == 1)
        lb._apply_routing_spec({'request_queue': None})
        await lb._notify_request_queue()
        assert await waiter is True
        assert lb._waiting_request_count == 0
        assert lb._active_request_count == 1
        await lb._release_request_slot(request)
        assert lb._active_request_count == 0

    asyncio.run(_run())


def test_streaming_response_holds_admission_until_asgi_release():

    async def _run():
        lb = _make_lb()
        lb._active_request_count = 1

        async def _upstream_release():
            return None

        response = load_balancer._ReleasingStreamingResponse(
            content=iter(()), release=_upstream_release)
        response.hold_cleanup_until_complete(lb._release_request_slot)
        assert lb._active_request_count == 1
        await response._release()
        assert lb._active_request_count == 0
        await response._release()
        assert lb._active_request_count == 0

    asyncio.run(_run())


def test_queue_less_stream_holds_process_local_admission_until_release(
        monkeypatch):

    async def _run():
        monkeypatch.setenv(constants.LB_POD_UID_ENV_VAR, 'queue-less-pod')
        lb = load_balancer.SkyServeLoadBalancer('http://controller:8001',
                                                8890,
                                                lb_slot='a')
        lb._lb_role = load_balancer.lb_ha.LbRole.ACTIVE
        lb._lb_role_generation = 1
        request = _request()

        async def _upstream_release():
            return None

        async def _proxy(_request):
            return load_balancer._ReleasingStreamingResponse(
                content=iter(()), release=_upstream_release)

        lb._proxy_with_retries_inner = _proxy
        response = await lb._proxy_with_retries(request)
        assert lb._active_request_count == 1
        assert lb._ha_role_payload()['local_in_flight'] == 1
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
        assert lb._waiting_request_body_bytes == 0

    asyncio.run(_run())


def test_waiting_body_budget_rejects_and_releases_partial_body():

    async def _run():
        lb = _make_lb(max_request_body_bytes=10)
        held = _StreamingRequest([b'123'])
        overflow = _StreamingRequest([b'4', b'5'])

        with mock.patch.object(
                constants, 'LB_REQUEST_QUEUE_WAITING_BODY_MEMORY_BUDGET_BYTES',
                4):
            body = await lb._request_body(held)  # type: ignore[arg-type]
            assert body == b'123'
            assert lb._waiting_request_body_bytes == 3
            with pytest.raises(fastapi.HTTPException) as exc:
                await lb._request_body(  # type: ignore[arg-type]
                    overflow)

        assert exc.value.status_code == 503
        assert exc.value.headers['Retry-After']
        # The overflow request temporarily reserved one byte before its second
        # chunk crossed the budget; that partial ownership must be returned.
        assert lb._waiting_request_body_bytes == 3
        lb._release_waiting_body_budget(
            held, drop_body=True)  # type: ignore[arg-type]
        assert lb._waiting_request_body_bytes == 0
        assert '_skyserve_bounded_body' not in vars(held)

    asyncio.run(_run())


def test_body_buffer_cancellation_releases_partial_reservation():

    async def _run():
        lb = _make_lb(max_request_body_bytes=10)
        started = asyncio.Event()
        never = asyncio.Event()

        class _BlockingRequest(_StreamingRequest):

            async def stream(self) -> AsyncIterator[bytes]:
                yield b'12'
                started.set()
                await never.wait()

        request = _BlockingRequest([])
        task = asyncio.create_task(
            lb._request_body(request))  # type: ignore[arg-type]
        await asyncio.wait_for(started.wait(), timeout=1)
        assert lb._waiting_request_body_bytes == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert lb._waiting_request_body_bytes == 0

    asyncio.run(_run())


def test_waiting_body_budget_transfers_on_admission():

    async def _run():
        lb = _make_lb(max_request_body_bytes=10)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        request = _StreamingRequest([b'payload'])

        async def _proxy(req):
            assert vars(req)['_skyserve_bounded_body'] == b'payload'
            assert lb._waiting_request_body_bytes == 0
            return fastapi.responses.Response(status_code=200)

        lb._proxy_with_retries_inner = _proxy
        response = await lb._proxy_with_retries(  # type: ignore[arg-type]
            request)
        assert response.status_code == 200
        assert lb._waiting_request_body_bytes == 0
        assert '_skyserve_bounded_body' not in vars(request)
        assert lb._active_request_count == 0

    asyncio.run(_run())


def test_queue_rejection_releases_buffered_body():

    async def _run():
        lb = _make_lb(min_size=0, size_per_replica=0, max_request_body_bytes=10)
        request = _StreamingRequest([b'payload'])
        lb._proxy_with_retries_inner = mock.AsyncMock()

        with pytest.raises(fastapi.HTTPException) as exc:
            await lb._proxy_with_retries(  # type: ignore[arg-type]
                request)
        assert exc.value.status_code == 503
        lb._proxy_with_retries_inner.assert_not_awaited()
        assert lb._waiting_request_body_bytes == 0
        assert '_skyserve_bounded_body' not in vars(request)

    asyncio.run(_run())


def test_queue_disable_releases_body_budget_after_unqueued_admission():

    async def _run():
        lb = _make_lb(max_request_body_bytes=10)
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])
        request = _StreamingRequest([b'payload'])

        class _DisablingCondition(asyncio.Condition):

            async def __aenter__(self):
                lb._request_queue_config = None
                return await super().__aenter__()

        async def _noop_release():
            return None

        async def _proxy(_):
            return load_balancer._ReleasingStreamingResponse(
                content=iter(()), release=_noop_release)

        lb._request_queue_condition = _DisablingCondition()
        lb._proxy_with_retries_inner = _proxy
        response = await lb._proxy_with_retries(  # type: ignore[arg-type]
            request)
        assert lb._waiting_request_body_bytes == 0
        assert '_skyserve_bounded_body' not in vars(request)

        await response._release()
        assert lb._waiting_request_body_bytes == 0
        assert '_skyserve_bounded_body' not in vars(request)

    asyncio.run(_run())


def test_acquire_survives_config_disable_during_lock_wait():
    """A controller sync may disable the queue while an acquire is waiting
    to take the condition lock; the request must fall back to unqueued
    dispatch instead of crashing."""

    async def _run():
        lb = _make_lb()
        lb._load_balancing_policy.set_ready_replicas(['http://worker:8000'])

        class _DisablingCondition(asyncio.Condition):

            async def __aenter__(self):
                lb._request_queue_config = None
                return await super().__aenter__()

        lb._request_queue_condition = _DisablingCondition()
        request = mock.MagicMock()
        request.headers = {}
        assert await lb._acquire_request_slot(request) is True
        assert lb._active_request_count == 1
        await lb._release_request_slot(request)
        assert lb._active_request_count == 0

    asyncio.run(_run())


def test_admission_slot_released_when_upstream_release_raises():
    """The chained admission release must run even if the upstream stream
    release raises, or dispatch capacity leaks permanently."""

    async def _run():
        lb = _make_lb()
        lb._active_request_count = 1

        async def _upstream_release():
            raise RuntimeError('aclose failed')

        response = load_balancer._ReleasingStreamingResponse(
            content=iter(()), release=_upstream_release)
        response.hold_cleanup_until_complete(lb._release_request_slot)
        with pytest.raises(RuntimeError):
            await response._release()
        assert lb._active_request_count == 0

    asyncio.run(_run())
