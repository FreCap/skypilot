"""LB demand feed for concurrency-native autoscaling.

The LB reports outstanding work to the controller as GAUGES
(per-replica in-flight snapshot, queue depth, deduped reject window)
alongside the existing timestamp aggregator, and caches the
controller's capacity_hint for /_lb/capacity readers.
"""
# pylint: disable=protected-access
import asyncio
import time
from unittest import mock

import fastapi
import httpx
import pytest

from sky.serve import constants
from sky.serve import load_balancer as lb_module
from sky.serve import load_balancing_policies as lb_policies


def _make_lb(policy_name='least_load'):
    return lb_module.SkyServeLoadBalancer(
        controller_url='http://controller:8001',
        load_balancer_port=30001,
        load_balancing_policy_name=policy_name)


def _request(job_id=None):
    request = mock.MagicMock()
    headers = {}
    if job_id is not None:
        headers[constants.LB_JOB_ID_HEADER] = job_id
    request.headers = headers
    return request


# --- snapshot_in_flight ---


def test_snapshot_in_flight_none_without_load_accounting():
    policy = lb_policies.RoundRobinPolicy()
    policy.set_ready_replicas(['http://a:8080'])
    assert policy.snapshot_in_flight() is None


def test_snapshot_in_flight_copies_load_map():
    policy = lb_policies.LeastLoadPolicy()
    policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
    policy.load_map['http://a:8080'] = 2
    snapshot = policy.snapshot_in_flight()
    assert snapshot == {'http://a:8080': 2, 'http://b:8080': 0}
    # A copy: mutating the snapshot must not touch live accounting.
    snapshot['http://a:8080'] = 99
    assert policy.load_map['http://a:8080'] == 2


def test_snapshot_in_flight_scoped_to_ready_replicas():
    policy = lb_policies.InstanceAwareLeastLoadPolicy()  # inherits tracking
    policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
    policy.load_map['http://a:8080'] = 1
    policy.load_map['http://b:8080'] = 1
    policy.set_ready_replicas(['http://a:8080'])
    assert policy.snapshot_in_flight() == {'http://a:8080': 1}


# --- queue-depth gauge ---


def test_queue_depth_counts_in_handler_and_resets_on_raise():
    lb = _make_lb()
    depths_during_select = []

    def _spy_select(request, exclude=None):
        del request, exclude
        depths_during_select.append(lb._queue_depth)
        return None  # no ready replicas -> terminal 503

    lb._load_balancing_policy.select_replica = _spy_select
    with pytest.raises(fastapi.HTTPException) as exc_info:
        asyncio.run(lb._proxy_with_retries(_request()))
    assert exc_info.value.status_code == 503
    # In-handler the request was counted; the raise released it.
    assert depths_during_select == [1]
    assert lb._queue_depth == 0


def test_queue_depth_excludes_dispatch_and_resets_on_return():
    # While dispatched, the unit belongs to the policy's load_map, not
    # queue_depth: counting both would double-count a running job for
    # its whole (hour-long, for sync servers) proxy await.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
    response = fastapi.responses.Response(status_code=200)

    async def _fake_proxy(url, request):
        del url, request
        assert lb._queue_depth == 0
        return response

    lb._proxy_request_to = _fake_proxy
    result = asyncio.run(lb._proxy_with_retries(_request()))
    assert result is response
    assert lb._queue_depth == 0


def test_queue_depth_recounts_between_failed_dispatches():
    # A failed attempt re-enters the queue while it backs off for the
    # next retry: the gauge must cover the between-dispatch phase.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(
        ['http://a:8080', 'http://b:8080'])
    response = fastapi.responses.Response(status_code=200)
    depths_during_dispatch = []
    request = _request()

    async def _is_disconnected():
        return False

    request.is_disconnected = _is_disconnected

    async def _fake_proxy(url, request):
        del url, request
        depths_during_dispatch.append(lb._queue_depth)
        if len(depths_during_dispatch) == 1:
            return httpx.ConnectError('boom')
        return response

    lb._proxy_request_to = _fake_proxy
    with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                    new=mock.AsyncMock()):
        result = asyncio.run(lb._proxy_with_retries(request))
    assert result is response
    # Both dispatches saw the unit handed off; nothing leaked after.
    assert depths_during_dispatch == [0, 0]
    assert lb._queue_depth == 0


# --- reject window with dedup ---


def test_reject_window_dedups_by_job_id():
    lb = _make_lb()
    lb._record_rejection(_request(job_id='job-1'))
    lb._record_rejection(_request(job_id='job-1'))
    lb._record_rejection(_request(job_id='job-2'))
    assert lb._rejected_in_window() == 2


def test_reject_window_headerless_counts_each_request():
    lb = _make_lb()
    lb._record_rejection(_request())
    lb._record_rejection(_request())
    assert lb._rejected_in_window() == 2


def test_reject_window_ttl_expiry_and_refresh():
    lb = _make_lb()
    lb._record_rejection(_request(job_id='job-1'))
    # Age the entry past the window: it must be pruned on access.
    lb._reject_last_seen['job-1'] = (time.monotonic() -
                                     constants.LB_REJECT_WINDOW_SECONDS - 1)
    assert lb._rejected_in_window() == 0
    # A re-fired job refreshes its TTL: aged then re-recorded -> live.
    lb._record_rejection(_request(job_id='job-2'))
    lb._reject_last_seen['job-2'] = (time.monotonic() -
                                     constants.LB_REJECT_WINDOW_SECONDS + 5)
    lb._record_rejection(_request(job_id='job-2'))
    assert lb._rejected_in_window() == 1


def test_terminal_503_records_rejection():
    lb = _make_lb()  # empty ready set -> "no ready replicas" exit
    with pytest.raises(fastapi.HTTPException):
        asyncio.run(lb._proxy_with_retries(_request(job_id='job-1')))
    with pytest.raises(fastapi.HTTPException):
        asyncio.run(lb._proxy_with_retries(_request(job_id='job-1')))
    # Two retries of the same held job are one unit of pressure.
    assert lb._rejected_in_window() == 1


# --- controller sync: payload gauges + capacity_hint caching ---


class _FakeResponse:

    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    async def json(self):
        return self._payload


class _FakeSession:

    def __init__(self, payload, captured):
        self._payload = payload
        self._captured = captured

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, json=None, timeout=None, headers=None):  # pylint: disable=redefined-builtin
        del timeout
        self._captured['url'] = url
        self._captured['json'] = json
        self._captured['headers'] = headers
        return _FakeResponse(self._payload)


def _run_sync(lb, response_payload):
    captured = {}
    with mock.patch.object(lb_module.aiohttp, 'ClientSession',
                           lambda: _FakeSession(response_payload, captured)):
        asyncio.run(lb._sync_with_controller_once())
    return captured


def test_sync_payload_carries_demand_gauges():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
    lb._load_balancing_policy.load_map['http://a:8080'] = 2
    lb._queue_depth = 3
    lb._record_rejection(_request(job_id='job-1'))
    captured = _run_sync(lb, {'replica_info': {}})
    body = captured['json']
    assert body['in_flight'] == {'http://a:8080': 2}
    assert body['queue_depth'] == 3
    assert body['rejected_in_window'] == 1
    assert 'timestamps' in body['request_aggregator']
    # Gauges are NOT cleared by a successful sync (only the timestamp
    # aggregator keeps clear-on-report semantics).
    assert lb._queue_depth == 3
    assert lb._rejected_in_window() == 1


def test_sync_payload_in_flight_none_without_tracking():
    lb = _make_lb(policy_name='round_robin')
    captured = _run_sync(lb, {'replica_info': {}})
    assert captured['json']['in_flight'] is None


def test_sync_caches_capacity_hint():
    lb = _make_lb()
    hint = {'provisioning_replicas': 4, 'target_num_replicas': 12}
    _run_sync(lb, {'replica_info': {}, 'capacity_hint': hint})
    assert lb._capacity_hint == hint
    assert lb._ready is True


def test_sync_without_hint_resets_cache_to_unknown():
    lb = _make_lb()
    lb._capacity_hint = {'provisioning_replicas': 1, 'target_num_replicas': 2}
    _run_sync(lb, {'replica_info': {}})
    assert lb._capacity_hint is None
