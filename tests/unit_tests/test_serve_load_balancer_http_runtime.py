"""Characterization tests for the load balancer's ASGI runtime boundary."""
# pylint: disable=protected-access
import asyncio
import pickle
from unittest import mock

import fastapi
import uvicorn

from sky.serve import constants
from sky.serve import load_balancer
from sky.serve import load_balancer_http
from sky.serve import serve_utils


def test_runtime_helpers_are_available_from_load_balancer_facade():
    assert (load_balancer._DrainableServer
            is load_balancer_http._DrainableServer)
    assert (load_balancer._InboundAuthMiddleware
            is load_balancer_http._InboundAuthMiddleware)
    assert (load_balancer._ReleasingStreamingResponse
            is load_balancer_http._ReleasingStreamingResponse)
    assert load_balancer._DrainableServer.__module__ == load_balancer.__name__
    assert (load_balancer._InboundAuthMiddleware.__module__ ==
            load_balancer.__name__)
    assert (load_balancer._ReleasingStreamingResponse.__module__ ==
            load_balancer.__name__)
    for runtime_type in (load_balancer._DrainableServer,
                         load_balancer._InboundAuthMiddleware,
                         load_balancer._ReleasingStreamingResponse):
        assert pickle.loads(pickle.dumps(runtime_type)) is runtime_type


def test_inbound_auth_consumes_only_load_balancer_credential(monkeypatch):
    seen_scopes = []

    async def app(scope, receive, send):
        del receive, send
        seen_scopes.append(scope)

    monkeypatch.setattr(serve_utils, 'is_lb_data_plane_auth_enabled',
                        lambda: True)
    monkeypatch.setattr(serve_utils, 'get_lb_auth_tokens', lambda required:
                        ('edge-token',) if required else ())
    middleware = load_balancer._InboundAuthMiddleware(app)
    scope = {
        'type': 'http',
        'method': 'POST',
        'path': '/predict',
        'headers': [
            (constants.LB_AUTHORIZATION_HEADER_BYTES, b'Bearer edge-token'),
            (b'authorization', b'Bearer replica-token'),
        ],
    }

    asyncio.run(middleware(scope, mock.AsyncMock(), mock.AsyncMock()))

    assert len(seen_scopes) == 1
    assert seen_scopes[0]['headers'] == [(b'authorization',
                                          b'Bearer replica-token')]
    assert scope['headers'][0][0] == constants.LB_AUTHORIZATION_HEADER_BYTES


def test_streaming_cleanup_chains_and_releases_each_owner_once():
    releases = []

    async def upstream_release():
        releases.append('upstream')

    async def admission_release():
        releases.append('admission')

    response = load_balancer._ReleasingStreamingResponse(
        content=iter(()), release=upstream_release)
    response.hold_cleanup_until_complete(admission_release)

    asyncio.run(response._release())
    asyncio.run(response._release())

    assert releases == ['upstream', 'admission', 'upstream']


def test_drainable_server_drains_once_then_forces_exit():
    drained = mock.Mock()
    loop = mock.Mock()
    server = load_balancer._DrainableServer(uvicorn.Config(fastapi.FastAPI()),
                                            on_drain=drained)

    server._handle_sigterm(loop)
    server._handle_sigterm(loop)

    drained.assert_called_once_with()
    loop.call_later.assert_called_once()
    assert server.should_exit is True
    assert server.force_exit is True
