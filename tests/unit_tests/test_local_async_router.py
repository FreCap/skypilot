"""Tests for the workload-agnostic local SkyServe async router."""

# pylint: disable=protected-access

import asyncio
import dataclasses
import gzip
import json
from typing import Any

import aiohttp
from aiohttp import test_utils
from aiohttp import web
from multidict import CIMultiDict
import pytest

from sky.serve import local_async_router

_ASYNC_PATH = '/async'
_READINESS_PATH = '/ready'


@dataclasses.dataclass
class _FakeWorker:
    """Controllable implementation of the generic async worker contract."""

    capacity: int = 1
    running: int = 0
    ready: bool = True
    predict_statuses: list[int] = dataclasses.field(default_factory=list)
    predict_delay: float = 0
    capacity_delay: float = 0
    compress_predict_response: bool = False
    capacity_payload: Any | None = None
    readiness_gate: asyncio.Event | None = None
    status_gate: asyncio.Event | None = None
    predict_response_headers: list[tuple[str, str]] = dataclasses.field(
        default_factory=list)
    status_http_status: int | None = None
    predicts: int = 0
    capacity_probes: int = 0
    statuses: int = 0
    jobs: dict[str, str] = dataclasses.field(default_factory=dict)
    predict_headers: CIMultiDict[str] = dataclasses.field(
        default_factory=CIMultiDict)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post(_ASYNC_PATH, self.handle_async)
        app.router.add_get(_READINESS_PATH, self.handle_readiness)
        return app

    async def handle_async(self, request: web.Request) -> web.Response:
        payload = await request.json()
        action = payload['action']
        if action == 'async_capacity':
            self.capacity_probes += 1
            if self.capacity_delay:
                await asyncio.sleep(self.capacity_delay)
            if self.capacity_payload is not None:
                return web.json_response(self.capacity_payload)
            return web.json_response({
                'status': 'READY',
                'running_count': self.running,
                'predict_concurrency': self.capacity,
            })
        if action == 'async_predict':
            self.predicts += 1
            self.predict_headers = CIMultiDict(request.headers)
            if self.predict_delay:
                await asyncio.sleep(self.predict_delay)
            if self.predict_statuses:
                status = self.predict_statuses.pop(0)
                return web.json_response({'status': 'rejected'}, status=status)
            if self.running >= self.capacity:
                return web.json_response({'status': 'busy'}, status=429)
            request_id = payload['request_id']
            self.running += 1
            self.jobs[request_id] = 'IN_PROGRESS'
            response_payload = {
                'request_id': request_id,
                'status': 'IN_PROGRESS',
            }
            if self.compress_predict_response:
                return web.Response(body=gzip.compress(
                    json.dumps(response_payload).encode()),
                                    content_type='application/json',
                                    headers={'Content-Encoding': 'gzip'})
            return web.json_response(response_payload,
                                     headers=CIMultiDict(
                                         self.predict_response_headers))
        if action == 'async_status':
            if self.status_gate is not None:
                await self.status_gate.wait()
            self.statuses += 1
            if self.status_http_status is not None:
                return web.json_response({'status': 'error'},
                                         status=self.status_http_status)
            request_id = payload['request_id']
            return web.json_response({
                'request_id': request_id,
                'status': self.jobs.get(request_id, 'NOT_FOUND'),
            })
        if action == 'async_cancel':
            request_id = payload['request_id']
            found = request_id in self.jobs
            if found:
                self.jobs[request_id] = 'CANCELED'
                self.running -= 1
            return web.json_response({
                'request_id': request_id,
                'canceled': found,
                'status': 'CANCELED' if found else 'NOT_FOUND',
            })
        raise AssertionError(action)

    async def handle_readiness(self, request: web.Request) -> web.Response:
        del request
        if self.readiness_gate is not None:
            await self.readiness_gate.wait()
        return web.json_response({'ready': self.ready},
                                 status=200 if self.ready else 503)

    def complete(self, request_id: str) -> None:
        self.jobs[request_id] = 'SUCCEEDED'
        self.running -= 1


async def _start_worker(worker: _FakeWorker) -> test_utils.TestServer:
    server = test_utils.TestServer(worker.app(), handler_cancellation=True)
    await server.start_server()
    return server


async def _start_router(workers: list[test_utils.TestServer],
                        **overrides) -> test_utils.TestServer:
    options = {
        'probe_cache_seconds': 60,
        'reservation_grace_seconds': 0,
        'request_timeout_seconds': 1,
    }
    options.update(overrides)
    router = local_async_router.LocalAsyncRouter(
        [str(worker.make_url('/')).rstrip('/') for worker in workers],
        _ASYNC_PATH, _READINESS_PATH, **options)
    server = test_utils.TestServer(router.create_app(),
                                   handler_cancellation=True)
    await server.start_server()
    return server


async def _post(session: aiohttp.ClientSession, server: test_utils.TestServer,
                payload: dict) -> aiohttp.ClientResponse:
    return await session.post(server.make_url(_ASYNC_PATH), json=payload)


@pytest.mark.asyncio
@pytest.mark.parametrize('worker_count', [1, 4, 8])
async def test_reports_one_slot_per_local_worker(worker_count: int) -> None:
    workers = [_FakeWorker() for _ in range(worker_count)]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            response = await _post(session, router_server,
                                   {'action': 'async_capacity'})
            payload = await response.json()
            assert payload['predict_concurrency'] == worker_count
            assert payload['running_count'] == 0
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_aggregates_and_fills_every_worker_slot() -> None:
    workers = [_FakeWorker() for _ in range(4)]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            capacity = await _post(session, router_server,
                                   {'action': 'async_capacity'})
            assert capacity.status == 200
            assert await capacity.json() == {
                'status': 'READY',
                'pod_name': local_async_router.socket.gethostname(),
                'running_count': 0,
                'predict_concurrency': 4,
            }

            for index in range(4):
                response = await _post(session, router_server, {
                    'action': 'async_predict',
                    'request_id': f'job-{index}',
                })
                assert response.status == 200
            assert [worker.predicts for worker in workers] == [1, 1, 1, 1]

            saturated = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'job-overflow',
            })
            assert saturated.status == 429

            workers[2].complete('job-2')
            refreshed = await _post(session, router_server,
                                    {'action': 'async_capacity'})
            assert (await refreshed.json())['running_count'] == 3
            replacement = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'job-replacement',
            })
            assert replacement.status == 200
            assert workers[2].predicts == 2
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_retries_only_explicit_capacity_rejections() -> None:
    workers = [_FakeWorker(predict_statuses=[503]), _FakeWorker()]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers,
                                        retriable_status_codes=(503,))
    try:
        async with aiohttp.ClientSession() as session:
            response = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'job-retry',
            })
            assert response.status == 200
            assert [worker.predicts for worker in workers] == [1, 1]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_does_not_replay_ambiguous_worker_failure() -> None:
    workers = [_FakeWorker(predict_statuses=[503]), _FakeWorker()]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            response = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'job-ambiguous',
            })
            assert response.status == 503
            assert [worker.predicts for worker in workers] == [1, 0]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_preserves_content_encoding_when_relaying_response() -> None:
    workers = [_FakeWorker(compress_predict_response=True)]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            response = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'job-compressed',
            })
            assert response.status == 200
            assert await response.json() == {
                'request_id': 'job-compressed',
                'status': 'IN_PROGRESS',
            }
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_preserves_duplicate_headers_and_strips_connection_options(
) -> None:
    worker = _FakeWorker(predict_response_headers=[
        ('Set-Cookie', 'first=1'),
        ('Set-Cookie', 'second=2'),
        ('Connection', 'X-Response-Hop'),
        ('Proxy-Connection', 'keep-alive'),
        ('X-Response-Hop', 'secret'),
        ('X-End-To-End', 'preserved'),
    ])
    worker_server = await _start_worker(worker)
    router_server = await _start_router([worker_server])
    try:
        async with aiohttp.ClientSession() as session:
            response = await session.post(router_server.make_url(_ASYNC_PATH),
                                          json={
                                              'action': 'async_predict',
                                              'request_id': 'job-headers',
                                          },
                                          headers={
                                              'Host': 'public.example',
                                              'Connection': 'X-Request-Hop',
                                              'Proxy-Connection': 'keep-alive',
                                              'X-Request-Hop': 'secret',
                                              'X-End-To-End': 'preserved',
                                          })

            assert response.status == 200
            assert response.headers.getall('Set-Cookie') == [
                'first=1',
                'second=2',
            ]
            assert response.headers['X-End-To-End'] == 'preserved'
            assert 'X-Response-Hop' not in response.headers
            assert 'Proxy-Connection' not in response.headers
            assert 'X-Request-Hop' not in worker.predict_headers
            assert 'Proxy-Connection' not in worker.predict_headers
            assert worker.predict_headers['X-End-To-End'] == 'preserved'
            assert worker.predict_headers['Host'] != 'public.example'
    finally:
        await router_server.close()
        await worker_server.close()


@pytest.mark.asyncio
async def test_does_not_replay_worker_timeout() -> None:
    workers = [_FakeWorker(predict_delay=0.05), _FakeWorker()]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers,
                                        request_timeout_seconds=0.01)
    try:
        async with aiohttp.ClientSession() as session:
            response = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'job-timeout',
            })
            assert response.status == 502
            assert [worker.predicts for worker in workers] == [1, 0]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_cancelled_handler_leaves_reconcilable_reservation(
        monkeypatch) -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH,
                                                 _READINESS_PATH,
                                                 probe_cache_seconds=60,
                                                 reservation_grace_seconds=0)
    child = router._children[0]
    child.known = True
    child.capacity = 1
    router._last_probe_finished_at = float('inf')
    request_started = asyncio.Event()

    async def _blocked_request(*_args, **_kwargs):
        request_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(router, '_request_child', _blocked_request)
    request = test_utils.make_mocked_request('POST', _ASYNC_PATH)
    body = json.dumps({
        'action': 'async_predict',
        'request_id': 'job-cancelled-client',
    }).encode()
    task = asyncio.create_task(
        router._handle_predict(request, body, json.loads(body)))
    await request_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(child.reservations) == 1
    assert next(iter(child.reservations.values())).settled_at is not None
    router._apply_probe(
        0, local_async_router._ProbeSample(float('inf'), capacity=1, running=0))
    assert child.reservations == {}


@pytest.mark.asyncio
async def test_concurrent_forced_capacity_refreshes_coalesce(
        monkeypatch) -> None:
    router = local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                                 _ASYNC_PATH, _READINESS_PATH)
    release_probe = asyncio.Event()
    probe_started = asyncio.Event()
    probe_calls = 0

    async def _blocked_probe(_index: int):
        nonlocal probe_calls
        probe_calls += 1
        probe_started.set()
        await release_probe.wait()
        return local_async_router._ProbeSample(1.0, capacity=1, running=0)

    monkeypatch.setattr(router, '_probe_child', _blocked_probe)
    refreshes = [
        asyncio.create_task(router._refresh_capacity(force=True))
        for _ in range(32)
    ]
    await probe_started.wait()
    await asyncio.sleep(0)
    release_probe.set()
    await asyncio.gather(*refreshes)

    assert probe_calls == 1


@pytest.mark.asyncio
async def test_concurrent_submissions_never_overdispatch() -> None:
    workers = [_FakeWorker(), _FakeWorker()]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            responses = await asyncio.gather(*[
                _post(session, router_server, {
                    'action': 'async_predict',
                    'request_id': f'job-{index}',
                }) for index in range(20)
            ])
            assert [response.status for response in responses].count(200) == 2
            assert [response.status for response in responses].count(429) == 18
            assert [worker.predicts for worker in workers] == [1, 1]
            assert [worker.running for worker in workers] == [1, 1]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_repeated_concurrent_saturation_recovers_without_slot_leaks(
) -> None:
    workers = [_FakeWorker(capacity=2) for _ in range(4)]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            for round_index in range(3):
                responses = await asyncio.gather(*[
                    _post(
                        session, router_server, {
                            'action': 'async_predict',
                            'request_id': f'round-{round_index}-job-{index}',
                        }) for index in range(32)
                ])
                accepted_ids = []
                for response in responses:
                    payload = await response.json()
                    if response.status == 200:
                        accepted_ids.append(payload['request_id'])
                assert len(accepted_ids) == 8
                assert sum(worker.running for worker in workers) == 8
                assert max(worker.running for worker in workers) == 2

                canceled = await asyncio.gather(*[
                    _post(session, router_server, {
                        'action': 'async_cancel',
                        'request_id': request_id,
                    }) for request_id in accepted_ids
                ])
                assert all(response.status == 200 for response in canceled)
                await asyncio.gather(*(response.read() for response in canceled)
                                    )
                capacity = await _post(session, router_server,
                                       {'action': 'async_capacity'})
                assert (await capacity.json())['running_count'] == 0
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_malformed_worker_capacity_fails_closed() -> None:
    workers = [
        _FakeWorker(),
        _FakeWorker(capacity_payload={
            'running_count': True,
            'predict_concurrency': 8,
        }),
    ]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            capacity = await _post(session, router_server,
                                   {'action': 'async_capacity'})
            assert (await capacity.json())['predict_concurrency'] == 1

            accepted = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'valid-slot',
            })
            saturated = await _post(
                session, router_server, {
                    'action': 'async_predict',
                    'request_id': 'must-not-use-malformed-worker',
                })
            assert accepted.status == 200
            assert saturated.status == 429
            assert [worker.predicts for worker in workers] == [1, 0]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_status_is_sticky_and_recovers_missing_ownership() -> None:
    workers = [_FakeWorker(), _FakeWorker()]
    workers[1].jobs['preexisting'] = 'SUCCEEDED'
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            submitted = await _post(session, router_server, {
                'action': 'async_predict',
                'request_id': 'sticky',
            })
            assert submitted.status == 200
            sticky = await _post(session, router_server, {
                'action': 'async_status',
                'request_id': 'sticky',
            })
            assert (await sticky.json())['status'] == 'IN_PROGRESS'
            assert [worker.statuses for worker in workers] == [1, 0]

            workers[0].status_http_status = 500

            recovered = await _post(session, router_server, {
                'action': 'async_status',
                'request_id': 'preexisting',
            })
            assert (await recovered.json())['status'] == 'SUCCEEDED'
            assert [worker.statuses for worker in workers] == [2, 1]
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_missing_owner_status_returns_before_unrelated_slow_worker(
) -> None:
    never_respond = asyncio.Event()
    workers = [
        _FakeWorker(jobs={'preexisting': 'IN_PROGRESS'}),
        _FakeWorker(status_gate=never_respond),
    ]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers,
                                        status_timeout_seconds=1)
    try:
        async with aiohttp.ClientSession() as session:
            response = await asyncio.wait_for(_post(
                session, router_server, {
                    'action': 'async_status',
                    'request_id': 'preexisting',
                }),
                                              timeout=0.2)
            assert response.status == 200
            assert (await response.json())['status'] == 'IN_PROGRESS'
    finally:
        never_respond.set()
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_readiness_succeeds_when_any_worker_is_ready() -> None:
    workers = [_FakeWorker(ready=False), _FakeWorker(ready=True)]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers)
    try:
        async with aiohttp.ClientSession() as session:
            response = await session.get(router_server.make_url(_READINESS_PATH)
                                        )
            assert response.status == 200
            assert await response.json() == {'ready': True}
            workers[1].ready = False
            unavailable = await session.get(
                router_server.make_url(_READINESS_PATH))
            assert unavailable.status == 503
    finally:
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_readiness_returns_before_unrelated_slow_worker() -> None:
    never_respond = asyncio.Event()
    workers = [
        _FakeWorker(ready=True),
        _FakeWorker(readiness_gate=never_respond),
    ]
    worker_servers = [await _start_worker(worker) for worker in workers]
    router_server = await _start_router(worker_servers,
                                        readiness_timeout_seconds=1)
    try:
        async with aiohttp.ClientSession() as session:
            response = await asyncio.wait_for(session.get(
                router_server.make_url(_READINESS_PATH)),
                                              timeout=0.2)
            assert response.status == 200
    finally:
        never_respond.set()
        await router_server.close()
        await asyncio.gather(*(server.close() for server in worker_servers))


@pytest.mark.asyncio
async def test_rejects_oversized_request_before_worker_dispatch() -> None:
    worker = _FakeWorker()
    worker_server = await _start_worker(worker)
    router_server = await _start_router([worker_server], client_max_size=64)
    try:
        async with aiohttp.ClientSession() as session:
            response = await session.post(router_server.make_url(_ASYNC_PATH),
                                          json={
                                              'action': 'async_predict',
                                              'request_id': 'oversized',
                                              'payload': 'x' * 256,
                                          })
            assert response.status == 413
            assert worker.predicts == 0
    finally:
        await router_server.close()
        await worker_server.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('body', [
    b'{',
    b'[]',
    json.dumps({
        'action': 'unknown'
    }).encode(),
])
async def test_rejects_malformed_protocol_before_worker_dispatch(
        body: bytes) -> None:
    worker = _FakeWorker()
    worker_server = await _start_worker(worker)
    router_server = await _start_router([worker_server])
    try:
        async with aiohttp.ClientSession() as session:
            response = await session.post(router_server.make_url(_ASYNC_PATH),
                                          data=body)
            assert response.status == 400
            assert worker.predicts == 0
            assert worker.capacity_probes == 0
    finally:
        await router_server.close()
        await worker_server.close()


@pytest.mark.parametrize('upstream', [
    '127.0.0.1:8081',
    'file:///tmp/socket',
    'http://127.0.0.1:8081/worker',
    'http://127.0.0.1:8081?token=secret',
])
def test_rejects_non_base_upstream_urls(upstream: str) -> None:
    with pytest.raises(ValueError, match='HTTP\\(S\\) base URL'):
        local_async_router.LocalAsyncRouter([upstream], _ASYNC_PATH,
                                            _READINESS_PATH)


def test_rejects_duplicate_upstreams() -> None:
    with pytest.raises(ValueError, match='must be unique'):
        local_async_router.LocalAsyncRouter([
            'http://127.0.0.1:8081',
            'http://127.0.0.1:8081/',
        ], _ASYNC_PATH, _READINESS_PATH)


@pytest.mark.parametrize('kwargs', [
    {
        'probe_timeout_seconds': float('nan')
    },
    {
        'request_timeout_seconds': float('inf')
    },
    {
        'probe_cache_seconds': float('nan')
    },
    {
        'client_max_size': 0
    },
    {
        'retriable_status_codes': (True,)
    },
    {
        'retriable_status_codes': (429.5,)
    },
])
def test_rejects_invalid_runtime_limits(kwargs) -> None:
    with pytest.raises(ValueError):
        local_async_router.LocalAsyncRouter(['http://127.0.0.1:8081'],
                                            _ASYNC_PATH, _READINESS_PATH,
                                            **kwargs)


def test_cli_builds_contiguous_local_upstreams() -> None:
    args = local_async_router._parser().parse_args([
        '--upstream-count',
        '3',
        '--upstream-port-start',
        '9001',
        '--async-path',
        _ASYNC_PATH,
        '--readiness-path',
        _READINESS_PATH,
    ])
    assert local_async_router._resolve_upstreams(args) == [
        'http://127.0.0.1:9001',
        'http://127.0.0.1:9002',
        'http://127.0.0.1:9003',
    ]


def test_cli_rejects_mixed_upstream_forms() -> None:
    args = local_async_router._parser().parse_args([
        '--upstream',
        'http://127.0.0.1:8081',
        '--upstream-count',
        '2',
        '--async-path',
        _ASYNC_PATH,
        '--readiness-path',
        _READINESS_PATH,
    ])
    with pytest.raises(ValueError,
                       match='either --upstream or --upstream-count'):
        local_async_router._resolve_upstreams(args)
