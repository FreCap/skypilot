"""External-LB recovery route-lease correctness fences.

These tests deliberately drive the LB's monotonic lease clock and exact client
objects.  A recovery-capable process can restart behind the same URL, so URL
equality alone is not a dispatch fence.
"""
# pylint: disable=protected-access
import asyncio
import threading
from unittest import mock

import fastapi
import httpx
import pytest
from starlette.datastructures import Headers
from starlette.datastructures import URL

from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import load_balancer as lb_module
from sky.serve import system_recovery_route_lease

_MARKED_URL = 'http://marked:8080'
_ORDINARY_URL = 'http://ordinary:8080'
_MOVED_URL = 'http://moved:8080'
_TOKEN = 'a' * 32
_ROTATED_TOKEN = 'b' * 32


def _make_lb() -> lb_module.SkyServeLoadBalancer:
    lb = lb_module.SkyServeLoadBalancer(controller_url='http://controller:8001',
                                        load_balancer_port=8890,
                                        service_hash='service-incarnation')
    lb._ready = True
    return lb


def _marker_info(replica_id: str = '1', token: str = _TOKEN):
    return {
        constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
        constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY: replica_id,
        constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY: token,
    }


def _heartbeat_payload(replica_id: str = '1',
                       token: str = _TOKEN,
                       remaining_seconds: float = 30.0):
    return {
        'version': constants.SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION,
        'entries': [{
            'replica_id': replica_id,
            'route_token': token,
            'remaining_seconds': remaining_seconds,
        }],
    }


def _install_routes(lb: lb_module.SkyServeLoadBalancer,
                    replica_info: dict[str, dict],
                    ready_urls: list[str] | None = None) -> None:
    if ready_urls is None:
        ready_urls = list(replica_info)
    with lb._client_pool_lock:
        lb._replace_system_recovery_route_markers_locked(replica_info)
        lb._load_balancing_policy.set_ready_replicas(ready_urls)


def _begin_heartbeat(lb: lb_module.SkyServeLoadBalancer, started_at: float):
    with mock.patch.object(lb_module.time, 'monotonic',
                           return_value=started_at):
        with lb._client_pool_lock:
            return lb._begin_system_recovery_route_lease_heartbeat_locked()


def _apply_heartbeat(lb: lb_module.SkyServeLoadBalancer, heartbeat, payload,
                     applied_at: float) -> bool:
    sequence, request_started_at, marker_snapshot = heartbeat
    with mock.patch.object(lb_module.time, 'monotonic',
                           return_value=applied_at):
        return lb._apply_system_recovery_route_lease_heartbeat(
            payload,
            sequence=sequence,
            request_started_at=request_started_at,
            marker_snapshot=marker_snapshot)


def _request(*, method: str = 'GET', body=None, stable_job_id: bool = True):
    request = mock.MagicMock()
    request.method = method
    request.url = URL('http://load-balancer/predict')
    headers = ({
        constants.LB_JOB_ID_HEADER: 'stable-job'
    } if stable_job_id else {})
    request.headers = Headers(headers)
    # MagicMock manufactures arbitrary attributes.  Production's outer proxy
    # handler installs these request-local values explicitly before entering
    # the retry loop, so mirror that boundary for direct inner-loop tests.
    setattr(request, lb_module._REQUEST_PRIORITY_ATTR,
            constants.LB_REQUEST_PRIORITY_MIN)
    setattr(request, lb_module._REQUEST_ACCELERATORS_ATTR, None)
    setattr(request, lb_module._REQUEST_GRANTED_ACCELERATOR_ATTR, None)

    if body is None:

        async def body():
            return b''

    async def is_disconnected():
        return False

    request.body = body
    request.is_disconnected = is_disconnected
    return request


class _RecordingClient:
    """Small httpx client double that records the final timeout object."""

    def __init__(self, *, build_error: Exception | None = None):
        self.build_error = build_error
        self.build_calls = 0
        self.timeout = None
        setattr(self, lb_module._INFLIGHT_ATTR, 0)

    def build_request(self, *args, **kwargs):
        del args
        self.build_calls += 1
        self.timeout = kwargs['timeout']
        if self.build_error is not None:
            raise self.build_error
        return mock.MagicMock()

    async def send(self, request, *, stream):
        del request, stream
        raise AssertionError('test client should fail during build_request')

    async def aclose(self):
        return None


def _install_client(lb: lb_module.SkyServeLoadBalancer, url: str,
                    client: _RecordingClient) -> None:
    with lb._client_pool_lock:
        lb._client_pool[url] = client
        lb._client_generation_locked(url, client)


def test_marked_route_starts_unavailable_until_first_heartbeat():
    lb = _make_lb()
    assert lb._system_recovery_route_markers == {}
    assert lb._system_recovery_route_lease_deadlines == {}

    _install_routes(lb, {_MARKED_URL: _marker_info()})
    with lb._client_pool_lock:
        assert not lb._system_recovery_route_is_available_locked(_MARKED_URL)
        assert lb._routable_ready_urls_locked() == set()


def test_heartbeat_deadline_is_request_start_anchored_and_expires_locally():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    heartbeat = _begin_heartbeat(lb, started_at=100.0)

    assert _apply_heartbeat(lb,
                            heartbeat,
                            _heartbeat_payload(remaining_seconds=10.0),
                            applied_at=105.0)
    assert lb._system_recovery_route_lease_deadlines[_MARKED_URL] == 110.0
    with lb._client_pool_lock:
        assert lb._system_recovery_route_is_available_locked(_MARKED_URL,
                                                             now=109.999)
        assert not lb._system_recovery_route_is_available_locked(_MARKED_URL,
                                                                 now=110.0)
    assert _MARKED_URL not in lb._system_recovery_route_lease_deadlines


def test_newer_omission_revokes_and_reordered_positive_cannot_reinstall():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    older = _begin_heartbeat(lb, started_at=10.0)
    newer = _begin_heartbeat(lb, started_at=11.0)

    _apply_heartbeat(lb,
                     newer,
                     _heartbeat_payload(remaining_seconds=30.0),
                     applied_at=12.0)
    expected_deadline = 41.0
    assert lb._system_recovery_route_lease_deadlines[_MARKED_URL] == (
        expected_deadline)
    # The older well-formed omission is ignored by the per-marker sequence.
    _apply_heartbeat(lb, older, {'version': 1, 'entries': []}, applied_at=13.0)
    assert lb._system_recovery_route_lease_deadlines[_MARKED_URL] == (
        expected_deadline)

    omission = _begin_heartbeat(lb, started_at=14.0)
    _apply_heartbeat(lb,
                     omission, {
                         'version': 1,
                         'entries': []
                     },
                     applied_at=15.0)
    assert _MARKED_URL not in lb._system_recovery_route_lease_deadlines
    _apply_heartbeat(lb,
                     newer,
                     _heartbeat_payload(remaining_seconds=30.0),
                     applied_at=16.0)
    assert _MARKED_URL not in lb._system_recovery_route_lease_deadlines


def test_malformed_heartbeat_is_atomic_and_cannot_refresh():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    valid = _begin_heartbeat(lb, started_at=20.0)
    _apply_heartbeat(lb,
                     valid,
                     _heartbeat_payload(remaining_seconds=30.0),
                     applied_at=21.0)
    deadlines_before = dict(lb._system_recovery_route_lease_deadlines)
    sequences_before = dict(
        lb._system_recovery_route_lease_last_applied_sequences)
    malformed = _begin_heartbeat(lb, started_at=22.0)

    payload = _heartbeat_payload(remaining_seconds=10.0)
    payload['entries'][0]['remaining_seconds'] = True
    with pytest.raises(system_recovery_route_lease.RouteLeaseError):
        _apply_heartbeat(lb, malformed, payload, applied_at=23.0)
    assert lb._system_recovery_route_lease_deadlines == deadlines_before
    assert (lb._system_recovery_route_lease_last_applied_sequences ==
            sequences_before)


def test_timed_out_heartbeat_cannot_refresh_existing_deadline():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    initial = _begin_heartbeat(lb, started_at=20.0)
    _apply_heartbeat(lb,
                     initial,
                     _heartbeat_payload(remaining_seconds=30.0),
                     applied_at=21.0)
    deadline_before = lb._system_recovery_route_lease_deadlines[_MARKED_URL]

    with mock.patch.object(lb_module.serve_utils,
                           'get_lb_sync_auth_tokens',
                           return_value=('token',)), mock.patch.object(
                               lb_module.aiohttp,
                               'ClientSession',
                               side_effect=asyncio.TimeoutError):
        with pytest.raises(asyncio.TimeoutError):
            asyncio.run(lb._sync_system_recovery_route_lease_once())
    assert lb._system_recovery_route_lease_deadlines[_MARKED_URL] == (
        deadline_before)


@pytest.mark.parametrize('mutation', ['replica_id', 'route_token', 'url'])
def test_old_response_cannot_cross_exact_marker_or_url_change(mutation):
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    old_heartbeat = _begin_heartbeat(lb, started_at=30.0)

    new_url = _MARKED_URL
    new_info = _marker_info()
    if mutation == 'replica_id':
        new_info = _marker_info(replica_id='2')
    elif mutation == 'route_token':
        new_info = _marker_info(token=_ROTATED_TOKEN)
    else:
        new_url = _MOVED_URL
    _install_routes(lb, {new_url: new_info}, ready_urls=[new_url])

    _apply_heartbeat(lb,
                     old_heartbeat,
                     _heartbeat_payload(remaining_seconds=30.0),
                     applied_at=31.0)
    with lb._client_pool_lock:
        assert not lb._system_recovery_route_is_available_locked(new_url,
                                                                 now=31.0)
        assert lb._routable_ready_urls_locked() == set()


@pytest.mark.parametrize('invalid_info', [
    {
        constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY: 'v1'
    },
    {
        **_marker_info(), constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY: 'v2'
    },
    _marker_info(replica_id='01'),
    _marker_info(token='A' * 32),
])
def test_partial_or_invalid_marker_is_fail_closed(invalid_info):
    lb = _make_lb()
    _install_routes(lb, {
        _MARKED_URL: invalid_info,
        _ORDINARY_URL: {},
    })
    now = lb_module.time.monotonic()
    with lb._client_pool_lock:
        assert _MARKED_URL in lb._system_recovery_invalid_route_marker_urls
        assert lb._routable_ready_urls_locked() == {_ORDINARY_URL}
        lb._replica_free_slots = {
            _MARKED_URL: 4,
            _ORDINARY_URL: 3,
        }
        lb._occupancy_dispatch_generation = {
            _MARKED_URL: 0,
            _ORDINARY_URL: 0,
        }
        lb._occupancy_sample_generation = {
            _MARKED_URL: 0,
            _ORDINARY_URL: 0,
        }
        lb._occupancy_sample_time = {
            _MARKED_URL: now,
            _ORDINARY_URL: now,
        }
        assert lb._effective_replica_free_slots_locked() == {_ORDINARY_URL: 3}


def test_heavy_sync_explicit_collision_fence_revokes_route_client_and_lease():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    client = _RecordingClient()
    _install_client(lb, _MARKED_URL, client)
    lb._system_recovery_route_lease_deadlines[_MARKED_URL] = (
        lb_module.time.monotonic() + 30.0)

    class _Response:

        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def raise_for_status(self):
            return None

        async def json(self):
            return {
                'replica_info': {
                    _MARKED_URL: {
                        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                            constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
                    }
                },
                'num_ready_replicas': 2,
                'routing_spec': {},
                'service_version': 7,
            }

    class _Session:

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, *args, **kwargs):
            del args, kwargs
            return _Response()

    async def scenario():
        await lb._sync_with_controller_once()
        # Let the zero-refcount retired-client close task finish.
        await asyncio.sleep(0)

    with mock.patch.object(lb_module.serve_utils,
                           'get_lb_sync_auth_tokens',
                           return_value=('token',)), mock.patch.object(
                               lb,
                               '_get_lb_session_id',
                               return_value='lb-session'), mock.patch.object(
                                   lb_module.aiohttp,
                                   'ClientSession',
                                   return_value=_Session()):
        asyncio.run(scenario())

    assert lb._load_balancing_policy.ready_replicas == []
    assert _MARKED_URL not in lb._client_pool
    assert _MARKED_URL not in lb._system_recovery_route_lease_deadlines
    assert _MARKED_URL in lb._system_recovery_invalid_route_marker_urls
    assert client.build_calls == 0


def test_occupancy_probe_and_request_routing_exclude_unleased_marker():
    lb = _make_lb()
    _install_routes(lb, {
        _MARKED_URL: _marker_info(),
        _ORDINARY_URL: {},
    })
    lb._occupancy_capable = {_MARKED_URL, _ORDINARY_URL}
    fetched_urls = []

    async def fetch(_session, url):
        fetched_urls.append(url)
        return 1, 2, 3

    class _Session:

        def __init__(self, *args, **kwargs):
            del args, kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    with mock.patch.object(lb, '_fetch_replica_occupancy',
                           side_effect=fetch), mock.patch.object(
                               lb_module.aiohttp,
                               'TCPConnector',
                               return_value=object()), mock.patch.object(
                                   lb_module.aiohttp, 'ClientSession',
                                   _Session):
        asyncio.run(lb._probe_replica_occupancy_once_unlocked())
    assert fetched_urls == [_ORDINARY_URL]
    assert set(lb._replica_free_slots) == {_ORDINARY_URL}

    attempts = []

    async def proxy(url, request):
        del request
        attempts.append(url)
        return fastapi.responses.Response(status_code=200)

    lb._proxy_request_to = proxy
    lb._queue_depth = 1
    response = asyncio.run(lb._proxy_with_retries_inner(_request()))
    assert response.status_code == 200
    assert attempts == [_ORDINARY_URL]


def test_body_await_remove_readd_never_substitutes_new_client():
    lb = _make_lb()
    _install_routes(lb, {_ORDINARY_URL: {}})
    old_client = _RecordingClient()
    new_client = _RecordingClient()
    _install_client(lb, _ORDINARY_URL, old_client)
    body_started = asyncio.Event()
    release_body = asyncio.Event()

    async def body():
        body_started.set()
        await release_body.wait()
        return b'payload'

    async def scenario():
        task = asyncio.create_task(
            lb._proxy_request_to(_ORDINARY_URL,
                                 _request(method='POST', body=body)))
        await body_started.wait()
        with lb._client_pool_lock:
            lb._client_pool.pop(_ORDINARY_URL)
            lb._client_pool_generations.pop(_ORDINARY_URL, None)
            lb._client_pool[_ORDINARY_URL] = new_client
            lb._client_generation_locked(_ORDINARY_URL, new_client)
        release_body.set()
        return await task

    result = asyncio.run(scenario())
    assert isinstance(result, lb_module._PreDispatchError)
    assert old_client.build_calls == 0
    assert new_client.build_calls == 0
    assert getattr(old_client, lb_module._INFLIGHT_ATTR) == 0
    assert getattr(new_client, lb_module._INFLIGHT_ATTR) == 0


def test_body_await_marker_expiry_fails_final_atomic_checkout():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    client = _RecordingClient()
    _install_client(lb, _MARKED_URL, client)
    clock = [50.0]
    lb._system_recovery_route_lease_deadlines[_MARKED_URL] = 55.0
    body_started = asyncio.Event()
    release_body = asyncio.Event()

    async def body():
        body_started.set()
        await release_body.wait()
        return b'payload'

    async def scenario():
        with mock.patch.object(lb_module.time,
                               'monotonic',
                               side_effect=lambda: clock[0]):
            task = asyncio.create_task(
                lb._proxy_request_to(_MARKED_URL,
                                     _request(method='POST', body=body)))
            await body_started.wait()
            clock[0] = 55.0
            release_body.set()
            return await task

    result = asyncio.run(scenario())
    assert isinstance(result, lb_module._PreDispatchError)
    assert client.build_calls == 0
    assert getattr(client, lb_module._INFLIGHT_ATTR) == 0


def test_armed_is_admission_eligible_but_draining_final_checkout_is_fenced():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    client = _RecordingClient()
    _install_client(lb, _MARKED_URL, client)
    lb._system_recovery_route_lease_deadlines[_MARKED_URL] = (
        lb_module.time.monotonic() + 30.0)
    lb._lb_role = lb_ha.LbRole.ARMED
    with lb._client_pool_lock:
        selected = lb._capture_selected_replica_locked(
            _MARKED_URL, require_current_route=True)
        assert lb._checkout_selected_replica_locked(selected) is client
    lb._release_client_refcount(client)

    lb._lb_role = lb_ha.LbRole.DRAINING
    with lb._client_pool_lock:
        assert lb._checkout_selected_replica_locked(selected) is None


def test_active_to_draining_transition_fences_retry_selection():
    lb = _make_lb()
    _install_routes(lb, {
        _ORDINARY_URL: {},
        _MOVED_URL: {},
    })
    lb._lb_role = lb_ha.LbRole.ACTIVE
    lb._max_retries = 2
    lb._queue_depth = 1
    attempts = []

    async def proxy(url, request):
        del request
        attempts.append(url)
        lb._lb_role = lb_ha.LbRole.DRAINING
        return lb_module._PreDispatchError('not dispatched')

    lb._proxy_request_to = proxy
    with mock.patch.object(lb_module.asyncio, 'sleep', new=mock.AsyncMock()):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            asyncio.run(lb._proxy_with_retries_inner(_request()))
    assert exc_info.value.status_code == 503
    assert 'draining' in exc_info.value.detail.lower()
    assert attempts and len(attempts) == 1


def test_marked_route_bounds_pool_and_connect_timeout_to_ten_seconds():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    error = httpx.ConnectError('stop after timeout capture')
    client = _RecordingClient(build_error=error)
    _install_client(lb, _MARKED_URL, client)
    lb._system_recovery_route_lease_deadlines[_MARKED_URL] = (
        lb_module.time.monotonic() + 30.0)

    result = asyncio.run(
        lb._proxy_request_to(_MARKED_URL, _request(method='POST')))
    assert result is error
    assert client.timeout.pool == (
        constants.LB_SYSTEM_RECOVERY_POOL_TIMEOUT_SECONDS)
    assert client.timeout.connect == constants.LB_CONNECT_TIMEOUT_SECONDS
    assert client.timeout.read == lb._stream_timeout_seconds
    assert getattr(client, lb_module._INFLIGHT_ATTR) == 0


def test_heartbeat_loop_is_immediate_fixed_start_and_nonoverlapping():
    lb = _make_lb()
    lb._draining = False
    clock = [0.0]
    starts = []
    sleep_delays = []
    active = 0
    max_active = 0
    real_sleep = asyncio.sleep

    async def once():
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        starts.append(clock[0])
        await real_sleep(0)
        clock[0] += 0.75
        active -= 1
        if len(starts) == 3:
            lb._draining = True

    async def sleep(delay):
        sleep_delays.append(delay)
        clock[0] += delay
        await real_sleep(0)

    with mock.patch.object(lb,
                           '_sync_system_recovery_route_lease_once',
                           side_effect=once), mock.patch.object(
                               lb_module.time,
                               'monotonic',
                               side_effect=lambda: clock[0]), mock.patch.object(
                                   lb_module.asyncio,
                                   'sleep',
                                   side_effect=sleep):
        asyncio.run(lb._sync_system_recovery_route_lease())
    assert starts == [0.0, 2.0, 4.0]
    assert sleep_delays == [1.25, 1.25]
    assert max_active == 1


def test_concurrent_heartbeat_calls_share_one_inflight_lock():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    entered = [asyncio.Event(), asyncio.Event()]
    release = [asyncio.Event(), asyncio.Event()]
    post_index = 0
    active = 0
    max_active = 0

    class _Response:

        status = 200

        def __init__(self, index):
            self.index = index

        async def __aenter__(self):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            entered[self.index].set()
            await release[self.index].wait()
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            nonlocal active
            active -= 1
            return False

        def raise_for_status(self):
            return None

        async def json(self):
            return _heartbeat_payload(remaining_seconds=30.0)

    class _Session:

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, *args, **kwargs):
            del args, kwargs
            nonlocal post_index
            response = _Response(post_index)
            post_index += 1
            return response

    async def scenario():
        first = asyncio.create_task(lb._sync_system_recovery_route_lease_once())
        second = asyncio.create_task(
            lb._sync_system_recovery_route_lease_once())
        await entered[0].wait()
        await asyncio.sleep(0)
        assert not entered[1].is_set()
        release[0].set()
        await entered[1].wait()
        release[1].set()
        await asyncio.gather(first, second)

    with mock.patch.object(lb_module.serve_utils,
                           'get_lb_sync_auth_tokens',
                           return_value=('token',)), mock.patch.object(
                               lb_module.aiohttp,
                               'ClientSession',
                               return_value=_Session()):
        asyncio.run(scenario())
    assert max_active == 1
    assert post_index == 2


def test_auth_retry_shares_ten_second_total_budget_and_request_start():
    lb = _make_lb()
    _install_routes(lb, {_MARKED_URL: _marker_info()})
    clock = [100.0]
    posts = []

    class _Response:

        def __init__(self, status):
            self.status = status

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            if self.status == 401:
                clock[0] = 106.0
            return False

        def raise_for_status(self):
            assert self.status == 200

        async def json(self):
            return _heartbeat_payload(remaining_seconds=20.0)

    class _Session:

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, url, *, headers, timeout):
            posts.append((url, headers, timeout.total))
            return _Response(401 if len(posts) == 1 else 200)

    with mock.patch.object(lb_module.time,
                           'monotonic',
                           side_effect=lambda: clock[0]), mock.patch.object(
                               lb_module.serve_utils,
                               'get_lb_sync_auth_tokens',
                               return_value=('old-token', 'new-token')):
        with mock.patch.object(lb_module.aiohttp,
                               'ClientSession',
                               return_value=_Session()):
            asyncio.run(lb._sync_system_recovery_route_lease_once())

    assert [post[2] for post in posts] == [9, 4.0]
    assert posts[0][1]['Authorization'] == 'Bearer old-token'
    assert posts[1][1]['Authorization'] == 'Bearer new-token'
    assert all(post[1][constants.SERVICE_HASH_HEADER] == 'service-incarnation'
               for post in posts)
    assert lb._system_recovery_route_lease_deadlines[_MARKED_URL] == 120.0
