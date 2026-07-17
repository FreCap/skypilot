"""Lifecycle and worker-churn tests for the local async router."""

# pylint: disable=protected-access

import asyncio
import dataclasses
import json
import time
from typing import Any

import aiohttp
from aiohttp import test_utils
from aiohttp import web
import pytest

from sky.serve import load_balancer
from sky.serve import local_async_router

_ASYNC_PATH = '/async'
_READINESS_PATH = '/ready'


@dataclasses.dataclass
class _RejectingWorker:
    """Worker that can reject a fixed number of predict requests."""

    rejects_remaining: int = 0
    capacity: int = 1
    running: int = 0
    predicts: int = 0
    predict_started: asyncio.Event | None = None
    predict_gate: asyncio.Event | None = None

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post(_ASYNC_PATH, self.handle_async)
        app.router.add_get(_READINESS_PATH, self.handle_readiness)
        return app

    async def handle_async(self, request: web.Request) -> web.Response:
        payload = await request.json()
        action = payload['action']
        if action == 'async_capacity':
            return web.json_response({
                'status': 'READY',
                'running_count': self.running,
                'predict_concurrency': self.capacity,
            })
        if action == 'async_predict':
            self.predicts += 1
            if self.predict_started is not None:
                self.predict_started.set()
            if self.predict_gate is not None:
                await self.predict_gate.wait()
            if self.rejects_remaining:
                self.rejects_remaining -= 1
                return web.json_response({'status': 'busy'}, status=429)
            if self.running >= self.capacity:
                return web.json_response({'status': 'busy'}, status=429)
            self.running += 1
            return web.json_response({
                'request_id': payload['request_id'],
                'status': 'IN_PROGRESS',
            })
        if action == 'async_status':
            return web.json_response({
                'request_id': payload['request_id'],
                'status': 'NOT_FOUND',
            })
        raise AssertionError(action)

    async def handle_readiness(self, request: web.Request) -> web.Response:
        del request
        return web.json_response({'ready': True})


async def _start_worker(worker: _RejectingWorker) -> test_utils.TestServer:
    server = test_utils.TestServer(worker.app(), handler_cancellation=True)
    await server.start_server()
    return server


async def _start_router(
    worker_servers: list[test_utils.TestServer],
    **overrides: Any,
) -> tuple[test_utils.TestServer, local_async_router.LocalAsyncRouter]:
    options = {
        'probe_cache_seconds': 60,
        'reservation_grace_seconds': 0,
        'request_timeout_seconds': 1,
    }
    options.update(overrides)
    router = local_async_router.LocalAsyncRouter(
        [str(server.make_url('/')).rstrip('/') for server in worker_servers],
        _ASYNC_PATH,
        _READINESS_PATH,
        **options,
    )
    server = test_utils.TestServer(router.create_app(),
                                   handler_cancellation=True)
    await server.start_server()
    return server, router


async def _post(session: aiohttp.ClientSession, server: test_utils.TestServer,
                payload: dict[str, Any]) -> aiohttp.ClientResponse:
    return await session.post(server.make_url(_ASYNC_PATH), json=payload)


@pytest.mark.asyncio
async def test_retry_exhaustion_releases_every_reservation() -> None:
    workers = [_RejectingWorker(rejects_remaining=1) for _ in range(2)]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server, router = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            rejected = await _post(
                session, router_server, {
                    'action': 'async_predict',
                    'request_id': 'rejected-by-every-worker',
                })
            assert rejected.status == 429
            assert all(not child.reservations for child in router._children)

            capacity = await _post(session, router_server,
                                   {'action': 'async_capacity'})
            assert (await capacity.json())['running_count'] == 0

            accepted = await _post(
                session, router_server, {
                    'action': 'async_predict',
                    'request_id': 'accepted-after-rejections',
                })
            assert accepted.status == 200
            assert [worker.predicts for worker in workers] == [2, 1]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_dead_worker_reservation_is_conservative_until_recovery(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        probe_cache_seconds=60,
        reservation_grace_seconds=0,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    router._last_probe_finished_at = float('inf')

    async def _unreachable_worker(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(router, '_request_child', _unreachable_worker)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    payload = {
        'action': 'async_predict',
        'request_id': 'possibly-accepted-before-worker-death',
    }
    body = json.dumps(payload).encode()

    response = await router._dispatch_predict(request, body, payload)
    assert response.status == 502
    reservation = next(iter(router._children[0].reservations.values()))
    assert reservation.settled_at is not None

    failed_probe_at = reservation.settled_at + 1
    router._apply_probe(
        0,
        local_async_router._ProbeSample(failed_probe_at,
                                        capacity=None,
                                        running=None))
    unavailable = await router._capacity_response()
    assert json.loads(unavailable.body) == {
        'status': 'UNKNOWN',
        'pod_name': local_async_router.socket.gethostname(),
        'running_count': 1,
        'predict_concurrency': 0,
    }
    assert await router._reserve(()) is None

    router._apply_probe(
        0,
        local_async_router._ProbeSample(failed_probe_at + 1,
                                        capacity=1,
                                        running=0))
    assert not router._children[0].reservations
    recovered = await router._capacity_response()
    recovered_payload = json.loads(recovered.body)
    assert recovered_payload['status'] == 'READY'
    assert recovered_payload['running_count'] == 0
    assert recovered_payload['predict_concurrency'] == 1


@pytest.mark.asyncio
async def test_settled_reservation_survives_probe_until_grace_elapses() -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        reservation_grace_seconds=10,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    reservation = await router._reserve(())
    assert reservation is not None
    child_index, token = reservation
    await router._settle(child_index, token)
    settled_at = router._children[0].reservations[token].settled_at
    assert settled_at is not None

    router._apply_probe(
        0, local_async_router._ProbeSample(settled_at - 1,
                                           capacity=1,
                                           running=0))
    assert token in router._children[0].reservations

    router._apply_probe(
        0, local_async_router._ProbeSample(settled_at + 5,
                                           capacity=1,
                                           running=0))
    assert token in router._children[0].reservations
    assert await router._reserve(()) is None

    router._apply_probe(
        0,
        local_async_router._ProbeSample(settled_at + 10, capacity=1, running=0))
    assert token not in router._children[0].reservations


@pytest.mark.asyncio
async def test_capacity_status_tracks_unknown_draining_and_ready() -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH, _READINESS_PATH)

    unknown = json.loads((await router._capacity_response()).body)
    assert unknown['status'] == 'UNKNOWN'
    assert unknown['predict_concurrency'] == 0

    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=0, running=0))
    draining = json.loads((await router._capacity_response()).body)
    assert draining['status'] == 'DRAINING'
    assert draining['predict_concurrency'] == 0

    router._apply_probe(
        0, local_async_router._ProbeSample(2.0, capacity=2, running=1))
    ready = json.loads((await router._capacity_response()).body)
    assert ready['status'] == 'READY'
    assert ready['running_count'] == 1
    assert ready['predict_concurrency'] == 2


@pytest.mark.asyncio
async def test_partial_probe_failure_cannot_prove_machine_idle() -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081', 'http://127.0.0.1:8082'],
        _ASYNC_PATH,
        _READINESS_PATH,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=2, running=1))
    router._apply_probe(
        1, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    router._apply_probe(
        0, local_async_router._ProbeSample(2.0, capacity=None, running=None))

    partial = json.loads((await router._capacity_response()).body)
    assert partial == {
        'status': 'UNKNOWN',
        'pod_name': local_async_router.socket.gethostname(),
        'running_count': 1,
        'predict_concurrency': 1,
    }


@pytest.mark.asyncio
async def test_first_partial_probe_after_restart_is_conservatively_busy(
) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081', 'http://127.0.0.1:8082'],
        _ASYNC_PATH,
        _READINESS_PATH,
    )
    router._apply_probe(
        1, local_async_router._ProbeSample(1.0, capacity=1, running=0))

    partial = json.loads((await router._capacity_response()).body)
    assert partial['status'] == 'UNKNOWN'
    assert partial['running_count'] == 1
    assert partial['predict_concurrency'] == 1


@pytest.mark.asyncio
async def test_reported_running_count_is_never_clipped_to_capacity() -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH, _READINESS_PATH)
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=2))

    response = json.loads((await router._capacity_response()).body)
    assert response['status'] == 'READY'
    assert response['running_count'] == 2
    assert response['predict_concurrency'] == 1


def test_stale_probe_cannot_overwrite_recovered_worker() -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH, _READINESS_PATH)

    router._apply_probe(
        0, local_async_router._ProbeSample(20.0, capacity=4, running=1))
    router._apply_probe(
        0, local_async_router._ProbeSample(10.0, capacity=None, running=None))

    child = router._children[0]
    assert child.known
    assert child.capacity == 4
    assert child.running == 1
    assert child.last_probe_started_at == 20.0


@pytest.mark.asyncio
async def test_application_cleanup_closes_session_and_allows_restart() -> None:
    worker_server = await _start_worker(_RejectingWorker())
    upstream = str(worker_server.make_url('/')).rstrip('/')
    router = local_async_router.LocalAsyncRouter([upstream], _ASYNC_PATH,
                                                 _READINESS_PATH)
    first_server = test_utils.TestServer(router.create_app())
    second_server: test_utils.TestServer | None = None
    await first_server.start_server()
    try:
        first_session = router._session
        assert first_session is not None
        assert not first_session.closed

        await first_server.close()
        assert first_session.closed
        assert router._session is None

        second_server = test_utils.TestServer(router.create_app())
        await second_server.start_server()
        second_session = router._session
        assert second_session is not None
        assert second_session is not first_session
        assert not second_session.closed

        async with aiohttp.ClientSession() as session:
            capacity = await _post(session, second_server,
                                   {'action': 'async_capacity'})
            assert capacity.status == 200
            assert (await capacity.json())['predict_concurrency'] == 1

        await second_server.close()
        assert second_session.closed
        assert router._session is None
    finally:
        await first_server.close()
        if second_server is not None:
            await second_server.close()
        await worker_server.close()


@pytest.mark.asyncio
async def test_graceful_shutdown_drains_inflight_prediction() -> None:
    predict_started = asyncio.Event()
    finish_predict = asyncio.Event()
    worker = _RejectingWorker(predict_started=predict_started,
                              predict_gate=finish_predict)
    worker_server = await _start_worker(worker)
    router_server, router = await _start_router([worker_server])
    close_task: asyncio.Task[None] | None = None
    try:
        async with aiohttp.ClientSession() as session:
            request = asyncio.create_task(
                _post(
                    session, router_server, {
                        'action': 'async_predict',
                        'request_id': 'inflight-during-shutdown',
                    }))
            await predict_started.wait()
            upstream_session = router._session
            assert upstream_session is not None

            close_task = asyncio.create_task(router_server.close())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not close_task.done()
            assert not upstream_session.closed

            finish_predict.set()
            response = await request
            assert response.status == 200
            assert (await response.json())['request_id'] == (
                'inflight-during-shutdown')
            await close_task
            assert upstream_session.closed
            assert router._session is None
    finally:
        finish_predict.set()
        if close_task is not None:
            await close_task
        await router_server.close()
        await worker_server.close()


@pytest.mark.asyncio
async def test_cancelled_refresh_releases_lock_and_next_refresh_recovers(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH, _READINESS_PATH)
    probe_started = asyncio.Event()
    probe_cancelled = asyncio.Event()

    async def _blocked_probe(_index: int) -> local_async_router._ProbeSample:
        probe_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            probe_cancelled.set()
            raise

    monkeypatch.setattr(router, '_probe_child', _blocked_probe)
    refresh = asyncio.create_task(router._refresh_capacity(force=True))
    await probe_started.wait()
    refresh.cancel()
    with pytest.raises(asyncio.CancelledError):
        await refresh
    await probe_cancelled.wait()
    assert not router._probe_lock.locked()
    assert not router._children[0].known

    async def _successful_probe(_index: int) -> local_async_router._ProbeSample:
        return local_async_router._ProbeSample(time.monotonic(),
                                               capacity=1,
                                               running=0)

    monkeypatch.setattr(router, '_probe_child', _successful_probe)
    await router._refresh_capacity(force=True)
    assert router._children[0].known
    assert router._children[0].capacity == 1


@pytest.mark.asyncio
async def test_synthetic_status_probe_rewrites_entity_headers(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH, _READINESS_PATH)
    captured: dict[str, Any] = {}

    async def _capture_request(
            *args: Any, **_kwargs: Any) -> local_async_router._ChildResponse:
        captured['body'] = args[3]
        captured['headers'] = args[4]
        return local_async_router._ChildResponse(200, b'{}', ())

    monkeypatch.setattr(router, '_request_child', _capture_request)
    request = test_utils.make_mocked_request(
        'POST',
        _ASYNC_PATH,
        headers={
            'Authorization': 'Bearer internal',
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/octet-stream',
        },
    )

    await router._request_status_child(request, 'job-1', 0)

    assert json.loads(captured['body']) == {
        'action': 'async_status',
        'request_id': 'job-1',
    }
    headers = captured['headers']
    assert headers['Authorization'] == 'Bearer internal'
    assert headers['Content-Type'] == 'application/json'
    assert 'Content-Encoding' not in headers


@pytest.mark.asyncio
async def test_sticky_owner_lru_is_bounded_for_long_lived_router() -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        max_sticky_requests=2,
    )

    await router._remember_owner('old-but-recently-read', 0)
    await router._remember_owner('least-recently-used', 0)
    assert await router._owner('old-but-recently-read') == 0
    await router._remember_owner('new', 0)

    assert await router._owner('least-recently-used') is None
    assert await router._owner('old-but-recently-read') == 0
    assert await router._owner('new') == 0


@pytest.mark.asyncio
async def test_confirmed_owner_lru_cannot_evict_ambiguous_claim(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        max_sticky_requests=1,
    )
    assert await router._remember_owner('ambiguous-original', 0, ambiguous=True)
    assert await router._remember_owner('newer-confirmed', 0)
    assert await router._owner('ambiguous-original') == 0
    assert await router._owner('newer-confirmed') is None

    async def _not_found(*_args: Any,
                         **_kwargs: Any) -> local_async_router._ChildResponse:
        return local_async_router._ChildResponse(
            200,
            json.dumps({
                'request_id': 'ambiguous-original',
                'status': 'NOT_FOUND',
            }).encode(),
            (),
        )

    dispatched = False

    async def _must_not_dispatch(*_args: Any, **_kwargs: Any) -> web.Response:
        nonlocal dispatched
        dispatched = True
        return web.json_response({'status': 'unexpected'})

    monkeypatch.setattr(router, '_request_status_child', _not_found)
    monkeypatch.setattr(router, '_dispatch_predict', _must_not_dispatch)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    payload = {
        'action': 'async_predict',
        'request_id': 'ambiguous-original',
    }

    response = await router._handle_predict(request,
                                            json.dumps(payload).encode(),
                                            payload)

    assert response.status == 502
    assert not dispatched


@pytest.mark.asyncio
async def test_full_ambiguity_budget_rejects_before_dispatch(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        max_sticky_requests=1,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    router._last_probe_finished_at = float('inf')
    assert await router._remember_owner('unresolved', 0, ambiguous=True)

    async def _must_not_reach_worker(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError('request reached a worker')

    monkeypatch.setattr(router, '_request_child', _must_not_reach_worker)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    payload = {
        'action': 'async_predict',
        'request_id': 'new-request',
    }

    response = await router._dispatch_predict(request,
                                              json.dumps(payload).encode(),
                                              payload)

    assert response.status == 429
    assert router._children[0].reservations == {}
    assert await router._owner('new-request') is None


@pytest.mark.asyncio
@pytest.mark.parametrize('worker_count', [1, 4, 8])
async def test_full_ambiguity_budget_advertises_zero_free_slots_and_recovers(
        worker_count: int) -> None:
    router = local_async_router.LocalAsyncRouter(
        [f'http://127.0.0.1:{8081 + index}' for index in range(worker_count)],
        _ASYNC_PATH,
        _READINESS_PATH,
        max_sticky_requests=1,
    )
    for index in range(worker_count):
        router._apply_probe(
            index, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    assert await router._remember_owner('unresolved', 0, ambiguous=True)

    blocked = json.loads((await router._capacity_response()).body)
    assert blocked == {
        'status': 'READY',
        'pod_name': blocked['pod_name'],
        'running_count': worker_count,
        'predict_concurrency': worker_count,
    }
    assert load_balancer.SkyServeLoadBalancer._parse_replica_occupancy(
        blocked) == (worker_count, 0, worker_count)

    await router._forget_owner('unresolved')
    recovered = json.loads((await router._capacity_response()).body)
    assert recovered == {
        'status': 'READY',
        'pod_name': recovered['pod_name'],
        'running_count': 0,
        'predict_concurrency': worker_count,
    }
    assert load_balancer.SkyServeLoadBalancer._parse_replica_occupancy(
        recovered) == (0, worker_count, worker_count)


@pytest.mark.asyncio
@pytest.mark.parametrize('worker_count', [1, 4, 8])
async def test_full_ambiguity_budget_preserves_unknown_worker_status(
        worker_count: int) -> None:
    router = local_async_router.LocalAsyncRouter(
        [f'http://127.0.0.1:{8081 + index}' for index in range(worker_count)],
        _ASYNC_PATH,
        _READINESS_PATH,
        max_sticky_requests=1,
    )
    for index in range(worker_count):
        router._apply_probe(
            index, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    assert await router._remember_owner('unresolved', 0, ambiguous=True)
    router._apply_probe(
        worker_count - 1,
        local_async_router._ProbeSample(2.0, capacity=None, running=None))

    blocked = json.loads((await router._capacity_response()).body)
    assert blocked['status'] == 'UNKNOWN'
    assert blocked['running_count'] == worker_count
    assert blocked['predict_concurrency'] == worker_count
    assert load_balancer.SkyServeLoadBalancer._parse_replica_occupancy(
        blocked) is None

    await router._forget_owner('unresolved')
    budget_recovered = json.loads((await router._capacity_response()).body)
    assert budget_recovered['status'] == 'UNKNOWN'
    assert load_balancer.SkyServeLoadBalancer._parse_replica_occupancy(
        budget_recovered) is None

    router._apply_probe(
        worker_count - 1,
        local_async_router._ProbeSample(3.0, capacity=1, running=0))
    worker_recovered = json.loads((await router._capacity_response()).body)
    assert worker_recovered['status'] == 'READY'
    assert load_balancer.SkyServeLoadBalancer._parse_replica_occupancy(
        worker_recovered) == (0, worker_count, worker_count)


@pytest.mark.asyncio
async def test_cancellation_during_owner_preclaim_leaves_reconcilable_slot(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        reservation_grace_seconds=0,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    router._last_probe_finished_at = float('inf')
    preclaim_started = asyncio.Event()

    async def _blocked_preclaim(*_args: Any, **_kwargs: Any) -> bool:
        preclaim_started.set()
        await asyncio.Event().wait()
        return True

    monkeypatch.setattr(router, '_remember_owner', _blocked_preclaim)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    payload = {
        'action': 'async_predict',
        'request_id': 'cancel-during-preclaim',
    }
    task = asyncio.create_task(
        router._dispatch_predict(request,
                                 json.dumps(payload).encode(), payload))
    await preclaim_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    reservation = next(iter(router._children[0].reservations.values()))
    assert reservation.settled_at is not None
    router._apply_probe(
        0,
        local_async_router._ProbeSample(reservation.settled_at + 1,
                                        capacity=1,
                                        running=0))
    assert router._children[0].reservations == {}
    assert await router._reserve(()) is not None


@pytest.mark.asyncio
async def test_cancellation_during_budget_release_leaves_reconcilable_slot(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        max_sticky_requests=1,
        reservation_grace_seconds=0,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    router._last_probe_finished_at = float('inf')
    assert await router._remember_owner('unresolved', 0, ambiguous=True)
    release_started = asyncio.Event()

    async def _blocked_release(*_args: Any, **_kwargs: Any) -> None:
        release_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(router, '_release', _blocked_release)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    payload = {
        'action': 'async_predict',
        'request_id': 'cancel-during-budget-release',
    }
    task = asyncio.create_task(
        router._dispatch_predict(request,
                                 json.dumps(payload).encode(), payload))
    await release_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    reservation = next(iter(router._children[0].reservations.values()))
    assert reservation.settled_at is not None
    router._apply_probe(
        0,
        local_async_router._ProbeSample(reservation.settled_at + 1,
                                        capacity=1,
                                        running=0))
    assert router._children[0].reservations == {}
    assert await router._reserve(()) is not None


@pytest.mark.asyncio
async def test_cancellation_after_retriable_rejection_releases_owner(
        monkeypatch: pytest.MonkeyPatch) -> None:
    router = local_async_router.LocalAsyncRouter(
        ['http://127.0.0.1:8081'],
        _ASYNC_PATH,
        _READINESS_PATH,
        reservation_grace_seconds=0,
    )
    router._apply_probe(
        0, local_async_router._ProbeSample(1.0, capacity=1, running=0))
    router._last_probe_finished_at = float('inf')
    request_started = asyncio.Event()
    rejection_returned = asyncio.Event()
    return_rejection = asyncio.Event()

    async def _reject(*_args: Any,
                      **_kwargs: Any) -> local_async_router._ChildResponse:
        request_started.set()
        await return_rejection.wait()
        rejection_returned.set()
        return local_async_router._ChildResponse(
            429,
            json.dumps({
                'status': 'busy'
            }).encode(), ())

    monkeypatch.setattr(router, '_request_child', _reject)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    payload = {
        'action': 'async_predict',
        'request_id': 'cancel-after-rejection',
    }
    task = asyncio.create_task(
        router._dispatch_predict(request,
                                 json.dumps(payload).encode(), payload))
    await request_started.wait()

    await router._state_lock.acquire()
    try:
        return_rejection.set()
        await rejection_returned.wait()
        task.cancel()
    finally:
        router._state_lock.release()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert await router._owner('cancel-after-rejection') is None
    assert router._children[0].reservations == {}
