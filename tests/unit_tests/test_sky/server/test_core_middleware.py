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


def test_controller_generation_admission_accepts_only_current_owner():
    app = _make_app(server.ControllerGenerationMiddleware)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
    }
    with mock.patch.dict('os.environ',
                         {'SKYPILOT_API_REQUEST_BACKEND': 'postgres'}), \
         mock.patch(
             'sky.server.requests.postgres.controller_leadership_is_current',
             side_effect=[True, False]) as is_current:
        with TestClient(app) as client:
            accepted = client.get('/route', headers=headers)
            fenced = client.get('/route', headers=headers)

    assert accepted.status_code == 200
    assert fenced.status_code == 409
    assert fenced.json() == {
        'detail': 'Controller generation is no longer current.'
    }
    assert is_current.call_args_list == [
        mock.call(instance_id, 22),
        mock.call(instance_id, 22),
    ]


def test_controller_generation_admission_rejects_partial_or_malformed_origin():
    app = _make_app(server.ControllerGenerationMiddleware)
    with TestClient(app) as client:
        partial = client.get(
            '/route',
            headers={
                server.server_constants.CONTROLLER_GENERATION_HEADER: '22'
            })
        malformed = client.get(
            '/route',
            headers={
                server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: 'not-a-uuid',
                server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
            })

    assert partial.status_code == 400
    assert malformed.status_code == 400


def test_controller_generation_admission_fails_closed_without_database_proof():
    app = _make_app(server.ControllerGenerationMiddleware)
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
    }
    with TestClient(app) as client:
        with mock.patch.dict('os.environ',
                             {'SKYPILOT_API_REQUEST_BACKEND': 'sqlite'}):
            wrong_backend = client.get('/route', headers=headers)
        with mock.patch.dict('os.environ',
                             {'SKYPILOT_API_REQUEST_BACKEND': 'postgres'}), \
             mock.patch(
                 'sky.server.requests.postgres.'
                 'controller_leadership_is_current',
                 side_effect=RuntimeError('database unavailable')):
            unavailable = client.get('/route', headers=headers)

    assert wrong_backend.status_code == 409
    assert unavailable.status_code == 503
    assert unavailable.json() == {
        'detail': 'Could not verify controller generation: RuntimeError.'
    }


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
            server.ControllerGenerationMiddleware,
            server.APIVersionMiddleware,
        }
    ]
    assert core_classes == [
        server.SecurityHeadersMiddleware,
        server.RequestIDMiddleware,
        server.ControllerGenerationMiddleware,
        server.GracefulShutdownMiddleware,
        server.APIVersionMiddleware,
    ]


def test_server_facade_preserves_core_middleware_identity():
    for name in (
            'RequestIDMiddleware',
            'SecurityHeadersMiddleware',
            'GracefulShutdownMiddleware',
            'ControllerGenerationMiddleware',
            'APIVersionMiddleware',
    ):
        server_class = getattr(server, name)
        assert server_class is getattr(core_middleware, name)
        assert server_class.__module__ == 'sky.server.server'
