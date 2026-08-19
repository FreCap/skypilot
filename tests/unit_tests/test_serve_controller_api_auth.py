"""Tests for split-role SkyServe controller authentication to the API."""

import asyncio
from unittest import mock

import fastapi
import pytest

from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import server
from sky.utils import common_utils


def _request(path: str,
             method: str,
             authorization: str | None,
             dedicated_auth: bool = True) -> fastapi.Request:
    headers = []
    if authorization is not None:
        headers.append((b'authorization', authorization.encode('utf-8')))
    if dedicated_auth:
        headers.append((
            server_constants.SERVE_CONTROLLER_API_AUTH_HEADER.lower().encode(
                'ascii'),
            server_constants.SERVE_CONTROLLER_API_AUTH_HEADER_VALUE.encode(
                'ascii'),
        ))
    return fastapi.Request({
        'type': 'http',
        'http_version': '1.1',
        'method': method,
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode('ascii'),
        'query_string': b'',
        'headers': headers,
        'client': ('10.0.0.1', 1234),
        'server': ('api', 80),
    })


def test_client_auth_is_scoped_to_exact_launch_operations(monkeypatch):
    monkeypatch.setattr(server_common.service_account_auth,
                        'get_service_account_headers', lambda: {})
    request = mock.Mock()
    token_reads = []

    def tokens():
        token_reads.append(True)
        return ('current', 'previous')

    with mock.patch.object(server_common.rest, 'request',
                           return_value=request) as rest_request:
        with server_common.serve_controller_api_auth(tokens):
            server_common.make_authenticated_request(
                'POST',
                server_constants.NON_POOL_LAUNCH_BINDING_PATH,
                server_url='http://api',
                headers={'authorization': 'Bearer caller-controlled'})
            allowed_kwargs = rest_request.call_args.kwargs
            server_common.make_authenticated_request(
                'GET',
                server_constants.NON_POOL_LAUNCH_BINDING_PATH,
                server_url='http://api')
            wrong_method_kwargs = rest_request.call_args.kwargs
            server_common.make_authenticated_request(
                'POST',
                '/launch',
                server_url='http://api',
                headers={
                    server_constants.SERVE_CONTROLLER_API_AUTH_HEADER: 'token'
                })
            wrong_path_kwargs = rest_request.call_args.kwargs

        server_common.make_authenticated_request(
            'POST',
            server_constants.NON_POOL_LAUNCH_BINDING_PATH,
            server_url='http://api')
        outside_scope_kwargs = rest_request.call_args.kwargs

    assert allowed_kwargs['headers'] == {
        'Authorization': 'Bearer previous',
        server_constants.SERVE_CONTROLLER_API_AUTH_HEADER:
            (server_constants.SERVE_CONTROLLER_API_AUTH_HEADER_VALUE),
    }
    assert token_reads == [True]
    assert 'Authorization' not in wrong_method_kwargs['headers']
    assert 'Authorization' not in wrong_path_kwargs['headers']
    assert 'Authorization' not in outside_scope_kwargs['headers']
    assert (server_constants.SERVE_CONTROLLER_API_AUTH_HEADER
            not in wrong_path_kwargs['headers'])


def test_client_auth_normalizes_query_path(monkeypatch):
    monkeypatch.setattr(server_common.service_account_auth,
                        'get_service_account_headers', lambda: {})
    with mock.patch.object(server_common.rest, 'request') as rest_request, \
         server_common.serve_controller_api_auth(
             lambda: ('controller-token',)):
        server_common.make_authenticated_request(
            'GET', '/api/get?request_id=request-id', server_url='http://api')

    assert rest_request.call_args.kwargs['headers'][
        'Authorization'] == 'Bearer controller-token'


def _run_middleware(monkeypatch,
                    authorization,
                    tokens,
                    path=server_constants.NON_POOL_LAUNCH_BINDING_PATH,
                    method='POST',
                    dedicated_auth=True):
    reads = []

    def get_tokens(required=False):
        reads.append(required)
        if isinstance(tokens, Exception):
            raise tokens
        return tokens

    monkeypatch.setattr(server.serve_utils,
                        'get_controller_admin_auth_tokens',
                        get_tokens,
                        raising=False)
    middleware = server.InternalServeControllerApiAuthMiddleware(
        app=lambda scope, receive, send: None)
    request = _request(path,
                       method,
                       authorization,
                       dedicated_auth=dedicated_auth)
    request.state.auth_user = None
    downstream_users = []

    async def call_next(inner_request):
        downstream_users.append(inner_request.state.auth_user)
        return fastapi.responses.Response(status_code=204)

    response = asyncio.run(middleware.dispatch(request, call_next))
    return response, reads, downstream_users


@pytest.mark.parametrize('token', ['current', 'previous'])
def test_middleware_accepts_admin_overlap_ring(monkeypatch, token):
    response, reads, users = _run_middleware(monkeypatch, f'Bearer {token}',
                                             ('current', 'previous'))

    assert response.status_code == 204
    assert reads == [True]
    assert len(users) == 1
    assert users[0].id == 'skyserve'
    assert users[0].user_type == 'system'
    cluster_name = common_utils.make_cluster_name_on_cloud_for_user(
        'boltz-l4-fleet-54883-0698237f22', max_length=42, user_hash=users[0].id)
    assert len(cluster_name) <= 42
    assert cluster_name.endswith('-skyserve')


@pytest.mark.parametrize('authorization',
                         [None, 'Bearer wrong', 'Bearer current extra'])
def test_middleware_rejects_bad_token(monkeypatch, authorization):
    response, reads, users = _run_middleware(monkeypatch, authorization,
                                             ('current', 'previous'))

    assert response.status_code == 401
    assert reads == [True]
    assert not users


@pytest.mark.parametrize(
    'tokens', [(), (None,), RuntimeError('secret unavailable')])
def test_middleware_fails_closed_without_ring(monkeypatch, tokens):
    response, reads, users = _run_middleware(monkeypatch, 'Bearer current',
                                             tokens)

    assert response.status_code == 503
    assert reads == [True]
    assert not users


@pytest.mark.parametrize(
    ('path', 'method'), [(server_constants.NON_POOL_LAUNCH_BINDING_PATH, 'GET'),
                         ('/launch', 'POST')])
def test_middleware_does_not_authenticate_outside_allowlist(
        monkeypatch, path, method):
    response, reads, users = _run_middleware(monkeypatch,
                                             'Bearer current', ('current',),
                                             path=path,
                                             method=method)

    assert response.status_code == 204
    assert not reads
    assert users == [None]


def test_middleware_does_not_intercept_normal_api_auth(monkeypatch):
    response, reads, users = _run_middleware(
        monkeypatch,
        None,
        RuntimeError('ring must not be read'),
        path='/api/health',
        method='GET',
        dedicated_auth=False)

    assert response.status_code == 204
    assert not reads
    assert users == [None]


def test_middleware_wraps_normal_authentication():
    middleware_names = [
        middleware.cls.__name__ for middleware in server.app.user_middleware
    ]
    initialize_index = middleware_names.index(
        'InitializeRequestAuthUserMiddleware')
    controller_api_index = middleware_names.index(
        'InternalServeControllerApiAuthMiddleware')
    bearer_index = middleware_names.index('BearerTokenMiddleware')
    oauth_index = middleware_names.index('OAuth2ProxyMiddleware')

    assert initialize_index < controller_api_index < bearer_index
    assert controller_api_index < oauth_index
