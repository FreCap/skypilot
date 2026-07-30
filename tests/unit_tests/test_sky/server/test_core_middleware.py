"""Characterization tests for API server core middleware."""

import types
from unittest import mock

import fastapi
from fastapi.testclient import TestClient

from sky.server import core_middleware
from sky.server import server


def _make_app(middleware_class):
    app = fastapi.FastAPI()
    app.add_middleware(middleware_class)

    @app.get('/route')
    async def route(request: fastapi.Request):
        return {'request_id': getattr(request.state, 'request_id', None)}

    @app.get('/api/route')
    async def api_route():
        return {'ok': True}

    return app


def test_request_id_middleware_preserves_request_and_response_identity():
    app = _make_app(server.RequestIDMiddleware)
    with mock.patch.object(server.requests_lib,
                           'get_new_request_id',
                           return_value='request-123'):
        response = TestClient(app).get('/route')

    assert response.status_code == 200
    assert response.json() == {'request_id': 'request-123'}
    assert response.headers['X-Skypilot-Request-ID'] == 'request-123'


def test_graceful_shutdown_preserves_control_api_access():
    app = _make_app(server.GracefulShutdownMiddleware)
    with mock.patch.object(server.state,
                           'get_block_requests',
                           return_value=True):
        with TestClient(app) as client:
            blocked = client.get('/route')
            control = client.get('/api/route')

    assert blocked.status_code == 503
    assert blocked.json() == {
        'detail': 'Server is shutting down, please try again later.'
    }
    assert control.status_code == 200


def test_request_store_cutover_blocks_new_submissions_but_not_control_api():
    app = _make_app(server.GracefulShutdownMiddleware)
    with mock.patch.object(server.state,
                           'get_block_requests',
                           return_value=False), \
         mock.patch.object(core_middleware.request_cutover,
                           'legacy_submissions_blocked',
                           return_value=True):
        with TestClient(app) as client:
            blocked = client.get('/route')
            control = client.get('/api/route')

    assert blocked.status_code == 503
    assert blocked.json() == {
        'detail': ('The API request store is being migrated to PostgreSQL, '
                   'please try again later.')
    }
    assert control.status_code == 200


def test_api_version_middleware_preserves_context_and_headers():
    app = _make_app(server.APIVersionMiddleware)
    version_info = types.SimpleNamespace(api_version=42,
                                         version='1.2.3',
                                         error=None)
    with mock.patch.object(server.versions,
                           'check_compatibility_at_server',
                           return_value=version_info), \
         mock.patch.object(server.versions,
                           'set_remote_api_version') as set_api_version, \
         mock.patch.object(server.versions,
                           'set_remote_version') as set_version, \
         mock.patch.object(server.versions,
                           'get_local_readable_version',
                           return_value='1.2.4'):
        response = TestClient(app).get('/route')

    assert response.status_code == 200
    set_api_version.assert_called_once_with(42)
    set_version.assert_called_once_with('1.2.3')
    assert response.headers[server.server_constants.API_VERSION_HEADER] == str(
        server.server_constants.API_VERSION)
    assert response.headers[server.server_constants.VERSION_HEADER] == '1.2.4'


def test_core_middleware_order_is_characterized():
    middleware_classes = [item.cls for item in server.app.user_middleware]
    core_classes = [
        middleware_class for middleware_class in middleware_classes
        if middleware_class in {
            server.RequestIDMiddleware,
            server.SecurityHeadersMiddleware,
            server.GracefulShutdownMiddleware,
            server.APIVersionMiddleware,
        }
    ]
    assert core_classes == [
        server.SecurityHeadersMiddleware,
        server.RequestIDMiddleware,
        server.GracefulShutdownMiddleware,
        server.APIVersionMiddleware,
    ]


def test_server_facade_preserves_core_middleware_identity():
    for name in (
            'RequestIDMiddleware',
            'SecurityHeadersMiddleware',
            'GracefulShutdownMiddleware',
            'APIVersionMiddleware',
    ):
        server_class = getattr(server, name)
        assert server_class is getattr(core_middleware, name)
        assert server_class.__module__ == 'sky.server.server'
