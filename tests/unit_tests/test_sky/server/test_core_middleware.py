"""Characterization tests for API server core middleware."""

import types
from unittest import mock

import fastapi
from fastapi.testclient import TestClient
import starlette.middleware.base

from sky.server import core_middleware
from sky.server import server

_CONTROLLER_CAPABILITY = 'A' * 43
_GUESSED_CAPABILITY = 'B' + 'A' * 42


class _AuthenticatedRequestStateMiddleware(
        starlette.middleware.base.BaseHTTPMiddleware):

    def __init__(self, app, *, authenticated: bool):
        super().__init__(app)
        self._authenticated = authenticated

    async def dispatch(self, request, call_next):
        request.state.auth_user = object() if self._authenticated else None
        request.state.controller_origin = None
        request.state.managed_job_origin = None
        return await call_next(request)


def _make_app(middleware_class, *, authenticated: bool = True):
    app = fastapi.FastAPI()
    app.add_middleware(middleware_class)
    app.add_middleware(_AuthenticatedRequestStateMiddleware,
                       authenticated=authenticated)

    @app.get('/route')
    async def route(request: fastapi.Request):
        return {'request_id': getattr(request.state, 'request_id', None)}

    @app.get('/api/route')
    async def api_route():
        return {'ok': True}

    @app.get('/origin')
    async def origin(request: fastapi.Request):
        return {
            'controller_origin': request.state.controller_origin,
            'managed_job_origin': request.state.managed_job_origin,
            'managed_job_context': server.versions.get_managed_job_origin(),
        }

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


def test_controller_generation_admission_requires_current_capability():
    app = _make_app(server.ControllerGenerationMiddleware)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
    }
    guessed_headers = dict(headers)
    guessed_headers[server.server_constants.
                    CONTROLLER_ORIGIN_CAPABILITY_HEADER] = (_GUESSED_CAPABILITY)
    with mock.patch.dict('os.environ',
                         {'SKYPILOT_API_REQUEST_BACKEND': 'postgres'}), \
         mock.patch(
             'sky.server.requests.postgres.'
             'controller_origin_capability_is_current',
             side_effect=[True, False]) as is_authenticated:
        with TestClient(app) as client:
            accepted = client.get('/route', headers=headers)
            fenced = client.get('/route', headers=guessed_headers)

    assert accepted.status_code == 200
    assert fenced.status_code == 401
    assert fenced.json() == {
        'detail': 'Controller origin is not authenticated.'
    }
    assert is_authenticated.call_args_list == [
        mock.call(instance_id, 22, _CONTROLLER_CAPABILITY),
        mock.call(instance_id, 22, _GUESSED_CAPABILITY),
    ]


def test_controller_generation_admission_does_not_replace_normal_auth():
    app = _make_app(server.ControllerGenerationMiddleware, authenticated=False)
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
    }
    with mock.patch('sky.server.requests.postgres.'
                    'controller_origin_capability_is_current') as verify:
        response = TestClient(app).get('/route', headers=headers)

    assert response.status_code == 401
    assert response.json() == {
        'detail': 'Controller origin requires normal authentication.'
    }
    verify.assert_not_called()


def test_controller_generation_admission_preserves_normal_loopback_auth():
    app = _make_app(server.ControllerGenerationMiddleware, authenticated=False)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
    }
    with mock.patch.object(
            core_middleware.auth_loopback, 'is_loopback_request',
            return_value=True), mock.patch(
                'sky.server.requests.postgres.'
                'controller_origin_capability_is_current',
                return_value=True) as verify, mock.patch.dict(
                    'os.environ', {'SKYPILOT_API_REQUEST_BACKEND': 'postgres'}):
        response = TestClient(app).get('/route', headers=headers)

    assert response.status_code == 200
    verify.assert_called_once_with(instance_id, 22, _CONTROLLER_CAPABILITY)


def test_authenticated_managed_job_origin_populates_only_verified_context():
    app = _make_app(server.ControllerGenerationMiddleware)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    attempt = '907b2c34-2f1f-4d79-ab14-43e5324e8a70'
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
        server.server_constants.MANAGED_JOB_ID_HEADER: '7',
        server.server_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_HEADER: '2',
        server.server_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_HEADER: attempt,
    }
    with mock.patch.dict('os.environ',
                         {'SKYPILOT_API_REQUEST_BACKEND': 'postgres'}), \
         mock.patch(
             'sky.server.requests.postgres.'
             'controller_origin_capability_is_current', return_value=True):
        response = TestClient(app).get('/origin', headers=headers)

    assert response.status_code == 200
    expected = [7, instance_id, 22, 2, attempt]
    assert response.json() == {
        'controller_origin': [instance_id, 22],
        'managed_job_origin': expected,
        'managed_job_context': expected,
    }


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
                server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
            })

    assert partial.status_code == 400
    assert malformed.status_code == 400


def test_controller_generation_admission_fails_closed_without_database_proof():
    app = _make_app(server.ControllerGenerationMiddleware)
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
    }
    with TestClient(app) as client:
        with mock.patch.dict('os.environ',
                             {'SKYPILOT_API_REQUEST_BACKEND': 'sqlite'}):
            no_local_authority = client.get('/route', headers=headers)
        with mock.patch.dict('os.environ',
                             {'SKYPILOT_API_REQUEST_BACKEND': 'postgres'}), \
             mock.patch(
                 'sky.server.requests.postgres.'
                 'controller_origin_capability_is_current',
                 side_effect=RuntimeError('database unavailable')):
            unavailable = client.get('/route', headers=headers)

    assert no_local_authority.status_code == 401
    assert unavailable.status_code == 503
    assert unavailable.json() == {
        'detail': 'Could not verify controller origin: RuntimeError.'
    }


def test_controller_generation_admission_supports_same_host_authority():
    app = _make_app(server.ControllerGenerationMiddleware)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
    }
    authority_path = '/private/controller-authority.json'
    with mock.patch.dict(
            'os.environ',
        {
            'SKYPILOT_API_REQUEST_BACKEND': 'sqlite',
            server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR: authority_path,
        }), mock.patch.object(core_middleware.controller_capability,
                              'verify_local_authority',
                              return_value=True) as verify:
        response = TestClient(app).get('/route', headers=headers)

    assert response.status_code == 200
    verify.assert_called_once_with(authority_path, instance_id, 22,
                                   _CONTROLLER_CAPABILITY)


def test_controller_generation_admission_derives_same_host_authority_path():
    app = _make_app(server.ControllerGenerationMiddleware)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    headers = {
        server.server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
        server.server_constants.CONTROLLER_GENERATION_HEADER: '22',
        server.server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER: _CONTROLLER_CAPABILITY,
    }
    derived_path = '/private/derived-controller-authority.json'
    with mock.patch.dict('os.environ',
                         {'SKYPILOT_API_REQUEST_BACKEND': 'sqlite'},
                         clear=True), mock.patch.object(
                             core_middleware.controller_capability,
                             'local_authority_path',
                             return_value=derived_path) as derive, \
         mock.patch.object(
             core_middleware.controller_capability,
             'verify_local_authority', return_value=True) as verify:
        response = TestClient(app).get('/route', headers=headers)

    assert response.status_code == 200
    derive.assert_called_once_with(instance_id)
    verify.assert_called_once_with(derived_path, instance_id, 22,
                                   _CONTROLLER_CAPABILITY)


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
