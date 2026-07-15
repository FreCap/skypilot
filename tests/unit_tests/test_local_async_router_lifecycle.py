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

    response = await router._handle_predict(request, body, payload)
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
