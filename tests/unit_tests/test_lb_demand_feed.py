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


def _request(job_id=None, method='POST'):
    request = mock.MagicMock()
    request.method = method
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
    request = _request(method='GET')

    async def _is_disconnected():
        return False

    request.is_disconnected = _is_disconnected

    async def _fake_proxy(url, request):
        del url, request
        depths_during_dispatch.append(lb._queue_depth)
        if len(depths_during_dispatch) == 1:
            # GET is idempotent, so an ambiguous read failure remains safe to
            # retry while we validate the queue-depth handoff.
            return httpx.ReadError('reset after send')
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


def test_accepted_retry_clears_same_job_rejection():
    lb = _make_lb()
    lb._record_rejection(_request(job_id='job-1'))
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
    response = fastapi.responses.Response(status_code=200)

    async def _fake_proxy(url, request):
        del url, request
        return response

    lb._proxy_request_to = _fake_proxy
    request = _request(job_id='job-1')
    assert asyncio.run(lb._proxy_with_retries(request)) is response
    assert lb._rejected_in_window() == 0


def test_non_success_response_keeps_same_job_rejection():
    lb = _make_lb()
    lb._record_rejection(_request(job_id='job-1'))
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
    response = fastapi.responses.Response(status_code=503)

    async def _fake_proxy(url, request):
        del url, request
        return response

    lb._proxy_request_to = _fake_proxy
    request = _request(job_id='job-1')
    assert asyncio.run(lb._proxy_with_retries(request)) is response
    assert lb._rejected_in_window() == 1


def test_async_request_contract_marks_replica_capable_before_dispatch():
    lb = _make_lb()
    url = 'http://async:8080'
    lb._load_balancing_policy.set_ready_replicas([url])
    response = fastapi.responses.Response(status_code=202)
    request = _request()
    request.headers = {'content-type': 'application/json'}
    request.json = mock.AsyncMock(return_value={'action': 'async_predict'})

    async def _fake_proxy(selected_url, forwarded_request):
        assert selected_url == url
        assert forwarded_request is request
        assert url in lb._occupancy_capable
        return response

    lb._proxy_request_to = _fake_proxy
    assert asyncio.run(lb._proxy_with_retries(request)) is response


def test_async_dispatch_invalidates_prior_sampled_zero():
    lb = _make_lb()
    url = 'http://async:8080'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._occupancy_capable = {url}
    lb._replica_occupancy = {url: 0}
    lb._occupancy_dispatch_generation = {url: 0}
    lb._occupancy_sample_generation = {url: 0}
    request = _request(job_id='job-1')

    async def _fake_proxy(selected_url, forwarded_request):
        del selected_url, forwarded_request
        return fastapi.responses.Response(status_code=202)

    lb._proxy_request_to = _fake_proxy
    asyncio.run(lb._proxy_with_retries(request))
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert url not in (in_flight or {})
    assert unknown_urls == [url]
    assert sampled_urls == []


def test_declared_url_custom_request_invalidates_occupancy_sample():
    lb = _make_lb()
    url = 'http://mixed:8080'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._occupancy_capable = {url}
    lb._occupancy_declared_urls = {url}
    lb._replica_occupancy = {url: 0}
    lb._occupancy_dispatch_generation = {url: 0}
    lb._occupancy_sample_generation = {url: 0}
    request = _request()

    async def _fake_proxy(selected_url, forwarded_request):
        del selected_url, forwarded_request
        return fastapi.responses.Response(status_code=200)

    lb._proxy_request_to = _fake_proxy
    asyncio.run(lb._proxy_with_retries(request))
    assert lb._occupancy_dispatch_generation[url] == 2
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert url not in (in_flight or {})
    assert unknown_urls == [url]
    assert sampled_urls == []


def test_ambiguous_async_failure_is_not_replayed_and_stays_unknown():
    lb = _make_lb()
    urls = ['http://a:8080', 'http://b:8080']
    lb._load_balancing_policy.set_ready_replicas(urls)
    lb._replica_occupancy = {url: 0 for url in urls}
    lb._replica_free_slots = {url: 1 for url in urls}
    lb._occupancy_dispatch_generation = {url: 0 for url in urls}
    lb._occupancy_sample_generation = {url: 0 for url in urls}
    attempts = []

    async def _fake_proxy(url, request):
        del request
        attempts.append(url)
        if len(attempts) == 1:
            return httpx.ReadError('reset after possible acceptance')
        return fastapi.responses.Response(status_code=202)

    request = _request(job_id='job-retried')
    request.is_disconnected = mock.AsyncMock(return_value=False)
    lb._proxy_request_to = _fake_proxy
    with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                    new=mock.AsyncMock()):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            asyncio.run(lb._proxy_with_retries(request))

    assert exc_info.value.status_code == 502
    assert len(attempts) == 1
    attempted_url = attempts[0]
    untouched_url = next(url for url in urls if url != attempted_url)
    assert lb._occupancy_dispatch_generation == {
        attempted_url: 2,
        untouched_url: 0,
    }
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert in_flight == {untouched_url: 0}
    assert unknown_urls == [attempted_url]
    assert sampled_urls == [untouched_url]
    assert lb._replica_free_slots == {untouched_url: 1}


def test_probe_started_before_async_dispatch_cannot_revalidate_zero():
    lb = _make_lb()
    url = 'http://async:8080'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._occupancy_capable = {url}
    lb._replica_occupancy = {url: 0}
    lb._replica_free_slots = {url: 1}
    lb._occupancy_dispatch_generation = {url: 0}
    lb._occupancy_sample_generation = {url: 0}

    async def _race():
        # Construct synchronization primitives inside asyncio.run() so they
        # bind to the running loop on Python 3.9 as well as newer versions.
        probe_started = asyncio.Event()
        finish_probe = asyncio.Event()

        async def _fetch(session, selected_url):
            del session, selected_url
            probe_started.set()
            await finish_probe.wait()
            return (0, 1)

        async def _fake_proxy(selected_url, forwarded_request):
            del selected_url, forwarded_request
            return fastapi.responses.Response(status_code=202)

        lb._fetch_replica_occupancy = _fetch
        lb._proxy_request_to = _fake_proxy
        probe_task = asyncio.create_task(lb._probe_replica_occupancy_once())
        await probe_started.wait()
        await lb._proxy_with_retries(_request(job_id='job-1'))
        finish_probe.set()
        await probe_task

    asyncio.run(_race())
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert url not in (in_flight or {})
    assert unknown_urls == [url]
    assert sampled_urls == []
    assert url not in lb._replica_free_slots


def test_probe_during_async_dispatch_cannot_publish_idle_after_accept():
    lb = _make_lb()
    url = 'http://async:8080'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._occupancy_capable = {url}
    lb._occupancy_declared_urls = {url}
    lb._replica_occupancy = {url: 0}
    lb._occupancy_dispatch_generation = {url: 0}
    lb._occupancy_sample_generation = {url: 0}

    async def _race():
        # Python 3.9 eagerly binds Event objects to the current loop.
        proxy_started = asyncio.Event()
        finish_proxy = asyncio.Event()

        async def _fetch_zero(session, selected_url):
            del session, selected_url
            return (0, 1)

        async def _held_proxy(selected_url, forwarded_request):
            del selected_url, forwarded_request
            proxy_started.set()
            await finish_proxy.wait()
            return fastapi.responses.Response(status_code=202)

        lb._fetch_replica_occupancy = _fetch_zero
        lb._proxy_request_to = _held_proxy
        proxy_task = asyncio.create_task(lb._proxy_with_retries(_request()))
        await proxy_started.wait()
        # This probe captures the post-start generation, then overtakes the
        # held POST and publishes zero before the replica accepts it.
        await lb._probe_replica_occupancy_once()
        assert lb._replica_free_slots == {url: 1}
        finish_proxy.set()
        await proxy_task

    asyncio.run(_race())
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert lb._occupancy_dispatch_generation[url] == 2
    assert url not in (in_flight or {})
    assert unknown_urls == [url]
    assert sampled_urls == []
    assert url not in lb._replica_free_slots


def test_probe_started_after_async_dispatch_revalidates_occupancy():
    lb = _make_lb()
    url = 'http://async:8080'
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._occupancy_capable = {url}
    lb._replica_occupancy = {url: 0}
    lb._occupancy_dispatch_generation = {url: 1}
    lb._occupancy_sample_generation = {url: 0}

    async def _fetch(session, selected_url):
        del session, selected_url
        return (1, 0)

    lb._fetch_replica_occupancy = _fetch
    asyncio.run(lb._probe_replica_occupancy_once())
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert in_flight == {url: 1}
    assert unknown_urls == []
    assert sampled_urls == [url]
    assert lb._occupancy_sample_generation[url] == 1
    assert lb._replica_free_slots == {url: 0}


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


def test_sync_loop_sleeps_after_unexpected_failure_and_recovers():
    lb = _make_lb()
    sync_rounds = 0

    async def _sync_once():
        nonlocal sync_rounds
        sync_rounds += 1
        if sync_rounds == 1:
            raise RuntimeError('bad response')
        # End the loop after proving a later successful round still runs.
        lb._draining = True

    lb._sync_with_controller_once = _sync_once
    first_backoff = mock.Mock()
    first_backoff.current_backoff.return_value = 3
    reset_backoff = mock.Mock()
    with mock.patch.object(lb_module.common_utils,
                           'Backoff',
                           side_effect=[first_backoff, reset_backoff]), \
         mock.patch.object(lb_module.asyncio,
                           'sleep',
                           new=mock.AsyncMock()) as sleep:
        asyncio.run(lb._sync_with_controller())

    assert sync_rounds == 2
    first_backoff.current_backoff.assert_called_once_with()
    assert sleep.await_args_list == [
        mock.call(5),
        mock.call(3),
        mock.call(constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS),
    ]


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


def test_declared_async_replica_is_unknown_before_first_probe():
    url = 'http://async:8080'
    lb = _make_lb()
    first = _run_sync(
        lb, {
            'replica_info': {
                url: {
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                    'async_occupancy': 'true',
                }
            }
        })
    # The outgoing snapshot precedes application of this sync response, so it
    # cannot claim an occupancy sample yet.
    assert first['json']['occupancy_sampled_urls'] == []
    assert lb._occupancy_capable == {url}
    assert lb._occupancy_declared_urls == {url}

    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert url not in (in_flight or {})
    assert unknown_urls == [url]
    assert sampled_urls == []


def test_sync_payload_proves_valid_occupancy_sample():
    url = 'http://async:8080'
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([url])
    lb._occupancy_capable = {url}
    lb._replica_occupancy = {url: 0}

    captured = _run_sync(lb, {'replica_info': {}})
    assert captured['json']['in_flight'] == {url: 0}
    assert captured['json']['occupancy_sampled_urls'] == [url]


def test_old_controller_omission_preserves_async_declaration():
    url = 'http://async:8080'
    lb = _make_lb()
    lb._occupancy_declared_urls = {url}
    _run_sync(lb,
              {'replica_info': {
                  url: {
                      'gpu_type': 'L4',
                      'gpu_count': '1',
                  }
              }})
    assert lb._occupancy_declared_urls == {url}


def test_old_controller_omission_preserves_explicit_disabled_state():
    url = 'http://sync:8080'
    lb = _make_lb()
    lb._occupancy_explicitly_disabled_urls = {url}
    _run_sync(lb,
              {'replica_info': {
                  url: {
                      'gpu_type': 'L4',
                      'gpu_count': '1',
                  }
              }})
    assert lb._occupancy_explicitly_disabled_urls == {url}


def test_explicit_false_waits_for_idle_before_clearing_capability():
    url = 'http://async:8080'
    lb = _make_lb()
    lb._occupancy_declared_urls = {url}
    lb._occupancy_capable = {url}
    lb._replica_occupancy = {url: 1}
    lb._replica_free_slots = {url: 0}
    lb._occupancy_dispatch_generation = {url: 2}
    lb._occupancy_sample_generation = {url: 2}
    _run_sync(
        lb, {
            'replica_info': {
                url: {
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                    'async_occupancy': 'false',
                }
            }
        })
    assert lb._occupancy_declared_urls == set()
    assert lb._occupancy_disable_pending == {url}
    assert lb._occupancy_capable == {url}
    assert lb._replica_occupancy == {url: 1}
    in_flight, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert in_flight == {url: 1}
    assert unknown_urls == []

    async def _fetch_idle(session, selected_url):
        del session, selected_url
        return (0, 1)

    lb._fetch_replica_occupancy = _fetch_idle
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_disable_pending == set()
    assert lb._occupancy_capable == set()
    assert lb._occupancy_explicitly_disabled_urls == {url}
    assert lb._replica_occupancy == {url: 0}
    assert lb._replica_free_slots == {url: 1}
    in_flight, _, unknown_urls, sampled_urls = (lb._in_flight_with_draining())
    assert in_flight == {url: 0}
    assert unknown_urls == []
    assert sampled_urls == [url]

    # A second passive zero remains disabled; it does not oscillate.
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == set()
    assert lb._occupancy_disable_pending == set()


def test_cold_explicit_false_preserves_positive_runtime_occupancy():
    url = 'http://async:8080'
    lb = _make_lb()
    _run_sync(
        lb, {
            'replica_info': {
                url: {
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                    'async_occupancy': 'false',
                }
            }
        })
    assert lb._occupancy_capable == {url}
    assert lb._occupancy_disable_pending == {url}
    assert lb._occupancy_explicitly_disabled_urls == {url}
    _, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert unknown_urls == [url]

    # The controller can retire the reused URL before this cold LB's first
    # probe. Transition evidence must survive off-ready and remain unknown.
    lb._load_balancing_policy.set_ready_replicas([])
    _, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert unknown_urls == [url]

    results = [(1, 0), None, (0, 1), (0, 1)]

    async def _fetch(session, selected_url):
        del session, selected_url
        return results.pop(0)

    lb._fetch_replica_occupancy = _fetch
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == {url}
    assert lb._occupancy_disable_pending == {url}
    assert lb._in_flight_with_draining()[0] == {url: 1}

    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == {url}
    assert lb._occupancy_disable_pending == {url}
    _, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert unknown_urls == [url]

    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == set()
    assert lb._occupancy_disable_pending == set()
    assert lb._occupancy_explicitly_disabled_urls == {url}

    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == set()
    assert lb._occupancy_disable_pending == set()


def test_recognized_async_request_temporarily_overrides_explicit_false():
    url = 'http://async:8080'
    lb = _make_lb()
    _run_sync(
        lb, {
            'replica_info': {
                url: {
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                    'async_occupancy': 'false',
                }
            }
        })

    async def _accept_async(selected_url, forwarded_request):
        del selected_url, forwarded_request
        return fastapi.responses.Response(status_code=202)

    lb._proxy_request_to = _accept_async
    asyncio.run(lb._proxy_with_retries(_request(job_id='job-after-false')))
    assert lb._occupancy_capable == {url}
    assert lb._occupancy_disable_pending == {url}
    assert lb._occupancy_explicitly_disabled_urls == {url}
    _, _, unknown_urls, sampled_urls = lb._in_flight_with_draining()
    assert unknown_urls == [url]
    assert sampled_urls == []

    async def _fetch_idle(session, selected_url):
        del session, selected_url
        return (0, 1)

    lb._fetch_replica_occupancy = _fetch_idle
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == set()
    assert lb._occupancy_disable_pending == set()
    assert lb._occupancy_explicitly_disabled_urls == {url}


def test_sync_payload_in_flight_none_without_tracking():
    lb = _make_lb(policy_name='round_robin')
    captured = _run_sync(lb, {'replica_info': {}})
    assert captured['json']['in_flight'] is None


class _FakeDrainingClient:

    def __init__(self, inflight):
        setattr(self, lb_module._INFLIGHT_ATTR, inflight)


def test_in_flight_includes_pruned_but_draining_work():
    # A probe-blipped replica leaves the routable set (load_map entry
    # pruned) while its hour-long requests keep running on the draining
    # client; the demand feed must keep attributing that work to the
    # url or the autoscaler reads the replica as an idle victim.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
    lb._load_balancing_policy.load_map['http://a:8080'] = 1
    lb._draining_clients = {
        'http://b:8080': [_FakeDrainingClient(2)],
    }
    assert lb._in_flight_with_draining()[0] == {
        'http://a:8080': 1,
        'http://b:8080': 2,
    }


def test_draining_overlay_sums_with_readded_url():
    # Probe recovered: the re-added url's NEW client tracks fresh work
    # in the load_map while the OLD draining client still carries the
    # pre-blip streams -- distinct requests, so counts add.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
    lb._load_balancing_policy.load_map['http://a:8080'] = 1
    lb._draining_clients = {'http://a:8080': [_FakeDrainingClient(1)]}
    assert lb._in_flight_with_draining()[0] == {'http://a:8080': 2}


def test_occupancy_and_envelope_counts_sum_conservatively():
    # Counts cannot prove overlap. Summing may briefly double a fast-ack
    # submit before its acknowledgement closes, but it must not collapse
    # distinct synchronous and async work into one unit.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(
        ['http://a:8080', 'http://b:8080'])
    lb._load_balancing_policy.load_map['http://b:8080'] = 2
    lb._replica_occupancy = {'http://a:8080': 3, 'http://b:8080': 1}
    lb._occupancy_capable = {'http://a:8080', 'http://b:8080'}
    assert lb._in_flight_with_draining()[0] == {
        'http://a:8080': 3,
        'http://b:8080': 3,
    }


def test_occupancy_capable_probe_miss_reads_unknown_not_zero():
    # A url that EVER reported occupancy but is absent from this probe
    # round is UNKNOWN: reporting the envelope's explicit 0 would bypass
    # the autoscaler's missing-entry-means-busy protection and let a
    # drain kill it mid-async-job. Non-capable urls keep their envelope
    # count (sync workloads have no occupancy endpoint at all).
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(
        ['http://cap:8080', 'http://sync:8080'])
    lb._replica_occupancy = {}
    lb._occupancy_capable = {'http://cap:8080'}
    assert lb._in_flight_with_draining()[0] == {'http://sync:8080': 0}


def test_pruned_url_occupancy_and_draining_sum_fail_closed():
    # A just-pruned url can appear in both the last probe round and draining
    # refcounts. Counts alone cannot identify overlap, so retain the safe upper
    # bound until a post-retirement sample resolves it.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([])
    lb._replica_occupancy = {'http://gone:8080': 1}
    lb._occupancy_capable = {'http://gone:8080'}
    lb._occupancy_sampled_off_ready = {'http://gone:8080'}
    lb._draining_clients = {'http://gone:8080': [_FakeDrainingClient(1)]}
    assert lb._in_flight_with_draining()[0] == {'http://gone:8080': 2}


def test_probe_round_marks_and_prunes_capability():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])

    async def _fake_fetch(session, url):
        del session, url
        return (1, 0)

    lb._fetch_replica_occupancy = _fake_fetch
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == {'http://a:8080'}
    # The replica disappears (not ready, not draining, probe miss): the
    # capability entry is RETAINED within the off-ready retention TTL (a
    # single miss is ambiguous -- see retention comment in the prober),
    # and pruned once the TTL expires without a successful answer, so the
    # set stays fleet-bounded.
    lb._load_balancing_policy.set_ready_replicas(['http://b:8080'])

    async def _fake_fetch_none(session, url):
        del session, url
        return None

    lb._fetch_replica_occupancy = _fake_fetch_none
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == {'http://a:8080'}
    lb._occupancy_off_ready_since = {
        'http://a:8080':
            (time.monotonic() -
             constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS - 1)
    }
    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_capable == set()


def test_scale_to_zero_prunes_all_occupancy_metadata():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([])
    lb._occupancy_capable = set()
    lb._occupancy_declared_urls = {'http://gone:8080'}
    lb._occupancy_disable_pending = {'http://gone:8080'}
    lb._occupancy_explicitly_disabled_urls = {'http://gone:8080'}
    lb._occupancy_dispatch_generation = {'http://gone:8080': 2}
    lb._occupancy_sample_generation = {'http://gone:8080': 1}
    lb._occupancy_off_ready_since = {'http://gone:8080': time.monotonic()}
    lb._occupancy_sampled_off_ready = {'http://gone:8080'}

    asyncio.run(lb._probe_replica_occupancy_once())
    assert lb._occupancy_declared_urls == set()
    assert lb._occupancy_disable_pending == set()
    assert lb._occupancy_explicitly_disabled_urls == set()
    assert lb._occupancy_dispatch_generation == {}
    assert lb._occupancy_sample_generation == {}
    assert lb._occupancy_off_ready_since == {}
    assert lb._occupancy_sampled_off_ready == set()


def test_drained_client_deregisters_from_demand_feed():
    lb = _make_lb()
    client = mock.Mock()
    setattr(client, lb_module._INFLIGHT_ATTR, 0)

    async def _aclose():
        return None

    client.aclose = _aclose
    lb._draining_clients = {'http://a:8080': [client]}
    lb._stream_timeout_seconds = 1
    asyncio.run(lb._drain_and_close_client('http://a:8080', client))
    assert lb._draining_clients == {}


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


def test_occupancy_miss_with_live_draining_stream_is_kept():
    # Fix for the retirement drain: an occupancy-capable url absent from
    # the probe round still carries EXACT draining refcounts; dropping it
    # would let the drain read the url as gone and kill the very request
    # it is waiting for.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([])
    lb._replica_occupancy = {}
    lb._occupancy_capable = {'http://gone:8080'}
    lb._draining_clients = {'http://gone:8080': [_FakeDrainingClient(2)]}
    assert lb._in_flight_with_draining()[0] == {'http://gone:8080': 2}


def test_routing_urls_sampled_with_gauge():
    # The routing view ships with the gauge so the controller can prove a
    # retiring replica was already un-routed when the gauge was sampled.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(
        ['http://a:8080', 'http://b:8080'])
    _, routing_urls, _, _ = lb._in_flight_with_draining()
    assert sorted(routing_urls) == ['http://a:8080', 'http://b:8080']


def test_quarantined_url_keeps_draining_refcounts_attributed():
    # Quarantine drops a url from routing outside the controller-prune
    # path; its live client must still enter the draining overlay, or a
    # retirement drain would read the replica as unrouted AND idle while
    # a long request still runs on it.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://q:8080'])
    lb._client_pool = {'http://q:8080': _FakeDrainingClient(1)}
    lb._replica_quarantine_until = {}
    lb._replica_dead_failures = {}
    lb._client_close_tasks = set()

    async def _quarantine():
        lb._quarantine_replica('http://q:8080')

    asyncio.run(_quarantine())
    in_flight, routing_urls, _, _ = lb._in_flight_with_draining()
    assert routing_urls == []
    assert in_flight == {'http://q:8080': 1}


def test_occupancy_capable_probe_miss_is_reported_unknown():
    # The drain must be able to distinguish 'no in-flight work' from
    # 'occupancy unknown this round': capable urls with no probe answer
    # and no draining streams ride in the unknown set.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['http://cap:8080'])
    lb._replica_occupancy = {}
    lb._occupancy_capable = {'http://cap:8080'}
    in_flight, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert 'http://cap:8080' not in (in_flight or {})
    assert unknown_urls == ['http://cap:8080']


def test_off_ready_capable_url_survives_probe_miss():
    # One transient probe miss on a retired occupancy-capable url must not
    # prune it from the capable set: that would convert 'occupancy
    # unknown' into 'absent = drained' and let a retirement kill live
    # async work. It stays capable (and unknown) within the retention TTL,
    # and is pruned after the TTL expires without a successful answer.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([])
    lb._occupancy_capable = {'http://gone:8080'}
    lb._replica_occupancy = {}

    async def _fetch_none(session, url):
        del session, url
        return None

    lb._fetch_replica_occupancy = _fetch_none
    asyncio.run(lb._probe_replica_occupancy_once())
    assert 'http://gone:8080' in lb._occupancy_capable
    _, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert unknown_urls == ['http://gone:8080']

    # After the retention TTL with no successful answer, it is pruned.
    lb._occupancy_off_ready_since = {
        'http://gone:8080':
            (time.monotonic() -
             constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS - 1)
    }
    asyncio.run(lb._probe_replica_occupancy_once())
    assert 'http://gone:8080' not in lb._occupancy_capable


def test_off_ready_retention_starts_at_retirement_not_last_confirmation():
    # The retention clock must start at the FIRST off-ready miss, not at
    # the last pre-retirement confirmation: otherwise a replica confirmed
    # long ago could lose its unknown protection before a maximum-length
    # graceful drain deadline expires.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([])
    lb._occupancy_capable = {'http://gone:8080'}
    lb._replica_occupancy = {}
    # Simulates state carried from when the url was last CONFIRMED long
    # ago; the new off-ready timer must ignore it and start fresh.
    lb._occupancy_off_ready_since = None

    async def _fetch_none(session, url):
        del session, url
        return None

    lb._fetch_replica_occupancy = _fetch_none
    asyncio.run(lb._probe_replica_occupancy_once())
    assert 'http://gone:8080' in lb._occupancy_capable
    since = lb._occupancy_off_ready_since['http://gone:8080']
    assert time.monotonic() - since < 5


def test_stale_pre_retirement_occupancy_zero_reads_unknown():
    # An occupancy sample taken while the url was still ROUTED cannot
    # prove post-retirement idleness: async work may have arrived after
    # the sample but before the url left routing. Until a post-retirement
    # probe answers, the url must read as unknown, not as the stale zero.
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas([])
    lb._occupancy_capable = {'http://gone:8080'}
    # Stale sample from when the url was ready (not marked off-ready).
    lb._replica_occupancy = {'http://gone:8080': 0}
    lb._occupancy_sampled_off_ready = set()
    in_flight, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert 'http://gone:8080' not in (in_flight or {})
    assert unknown_urls == ['http://gone:8080']
    # Once the prober samples it off-ready, the explicit zero is usable.
    lb._occupancy_sampled_off_ready = {'http://gone:8080'}
    in_flight, _, unknown_urls, _ = lb._in_flight_with_draining()
    assert in_flight == {'http://gone:8080': 0}
    assert unknown_urls == []
