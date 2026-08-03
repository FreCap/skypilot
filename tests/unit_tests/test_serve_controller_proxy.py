"""Tests for the stable external-LB to SkyServe-controller proxy."""
# pylint: disable=protected-access

import asyncio
from typing import List, Optional, Tuple
from unittest import mock

import aiohttp
import fastapi
from fastapi import testclient
import pytest

from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve.server import controller_proxy
from sky.server import server


def _request(path: str,
             authorization: Optional[str] = 'Bearer sync-token',
             service_hash: Optional[str] = 'service-incarnation-a',
             body: bytes = b'{"request_aggregator": {}}') -> fastapi.Request:
    headers = [(b'content-type', b'application/json')]
    if authorization is not None:
        headers.append((b'authorization', authorization.encode('utf-8')))
    if service_hash is not None:
        headers.append((constants.SERVICE_HASH_HEADER.lower().encode('ascii'),
                        service_hash.encode('utf-8')))
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {'type': 'http.disconnect'}
        sent = True
        return {
            'type': 'http.request',
            'body': body,
            'more_body': False,
        }

    return fastapi.Request(
        {
            'type': 'http',
            'http_version': '1.1',
            'method': 'POST',
            'scheme': 'http',
            'path': path,
            'raw_path': path.encode('ascii'),
            'query_string': b'',
            'headers': headers,
            'client': ('10.0.0.1', 1234),
            'server': ('api', 80),
        },
        receive=receive)


class _FakeControllerResponse:
    """Minimal aiohttp response context manager for proxy tests."""

    def __init__(self,
                 body: bytes = b'{"replica_info": {}}',
                 status: int = 200,
                 content_type: str = 'application/json'):
        self._body = body
        self.status = status
        self.headers = {'Content-Type': content_type}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback

    async def read(self):
        return self._body


class _FakeClientSession:
    """Record the proxy's outbound requests without network I/O."""

    def __init__(self, calls: List[dict], response=None):
        self._calls = calls
        self._response = response or _FakeControllerResponse()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback

    def post(self, url, **kwargs):
        self._calls.append({'url': url, **kwargs})
        return self._response


def _patch_owner_reads(monkeypatch, owners: List[Optional[Tuple[str, int, str,
                                                                int]]]):
    remaining = list(owners)

    async def read_owner(service_name):
        assert service_name == 'svc'
        return remaining.pop(0)

    monkeypatch.setattr(controller_proxy, '_read_controller_owner', read_owner)


def _owner(controller_pid=1234,
           controller_ip='10.2.3.4',
           controller_port=20001,
           service_hash='service-incarnation-a'):
    return service_hash, controller_pid, controller_ip, controller_port


def _owner_record(**overrides):
    record = {
        'hash': 'service-incarnation-a',
        'status': serve_state.ServiceStatus.READY,
        'controller_pid': 1234,
        'controller_ip': '10.2.3.4',
        'controller_port': 20001,
    }
    record.update(overrides)
    return record


def test_proxy_forwards_raw_body_once_and_preserves_response(monkeypatch):
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    upstream = _FakeControllerResponse(body=b'{"ok": true}', status=202)
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls, response=upstream))
    body = b'{"request_aggregator":{"timestamps":[1,2]}}'

    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync',
                     body=body)))

    assert response.status_code == 202
    assert response.body == b'{"ok": true}'
    assert response.headers['content-type'] == 'application/json'
    assert len(calls) == 1
    assert calls[0]['url'] == (
        'http://10.2.3.4:20001/controller/load_balancer_sync')
    assert calls[0]['data'] == body
    assert calls[0]['headers']['Authorization'] == 'Bearer sync-token'
    assert calls[0]['headers']['Content-Type'] == 'application/json'
    assert calls[0]['headers'][constants.CONTROLLER_OWNER_HEADER] == (
        serve_utils.make_controller_owner_fingerprint(*owner))
    assert calls[0]['allow_redirects'] is False
    assert calls[0]['timeout'].total == (
        constants.LB_CONTROLLER_PROXY_TIMEOUT_SECONDS)


def test_proxy_forwards_history_only_sync_to_distinct_controller_path(
        monkeypatch):
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls))
    path = ('/api/internal/serve/svc/controller/'
            'load_balancer_request_history_sync')

    response = asyncio.run(
        controller_proxy.proxy_load_balancer_request_history_sync(
            'svc', _request(path, body=b'{"request_history": {}}')))

    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0]['url'] == ('http://10.2.3.4:20001/controller/'
                               'load_balancer_request_history_sync')
    assert calls[0]['data'] == b'{"request_history": {}}'


def test_proxy_forwards_system_recovery_lease_with_tight_timeout(monkeypatch):
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    upstream = _FakeControllerResponse(body=b'{"version":1,"entries":[]}')
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls, response=upstream))
    path = ('/api/internal/serve/svc/controller/'
            'system_recovery_route_lease')

    response = asyncio.run(
        controller_proxy.proxy_load_balancer_system_recovery_route_lease(
            'svc', _request(path, body=b'{}')))

    assert response.status_code == 200
    assert response.body == b'{"version":1,"entries":[]}'
    assert calls[0]['url'] == (
        'http://10.2.3.4:20001/controller/system_recovery_route_lease')
    assert calls[0]['timeout'].total == (
        constants.LB_SYSTEM_RECOVERY_LEASE_PROXY_TIMEOUT_SECONDS)


def test_proxy_forwards_role_heartbeat_to_distinct_controller_path(monkeypatch):
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    upstream = _FakeControllerResponse(body=b'{"role":"ACTIVE","generation":1}')
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls, response=upstream))
    path = '/api/internal/serve/svc/controller/load_balancer_role'

    response = asyncio.run(
        controller_proxy.proxy_load_balancer_role(
            'svc', _request(path, body=b'{"lb_slot":"a"}')))

    assert response.status_code == 200
    assert response.body == b'{"role":"ACTIVE","generation":1}'
    assert len(calls) == 1
    assert calls[0]['url'] == (
        'http://10.2.3.4:20001/controller/load_balancer_role')
    assert calls[0]['data'] == b'{"lb_slot":"a"}'


def test_proxy_rejects_response_if_owner_changes(monkeypatch):
    _patch_owner_reads(monkeypatch, [
        _owner(),
        _owner(controller_pid=5678, controller_ip='10.2.3.5'),
    ])
    calls = []
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls))

    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync')))

    assert response.status_code == 503
    assert b'ownership changed' in response.body
    assert len(calls) == 1


def test_proxy_detects_same_address_reused_by_new_owner(monkeypatch):
    _patch_owner_reads(monkeypatch, [_owner(), _owner(controller_pid=5678)])
    calls = []
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls))
    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync')))
    assert response.status_code == 503


def test_proxy_detects_same_endpoint_reused_by_new_service_row(monkeypatch):
    _patch_owner_reads(monkeypatch, [
        _owner(service_hash='service-incarnation-a'),
        _owner(service_hash='service-incarnation-b'),
    ])
    calls = []
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls))
    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync')))
    assert response.status_code == 503
    assert b'ownership changed' in response.body
    assert len(calls) == 1


@pytest.mark.parametrize('service_hash', [None, 'service-incarnation-b'])
def test_proxy_rejects_stale_lb_before_forward(monkeypatch, service_hash):
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner])
    calls = []
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls))

    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync',
                     service_hash=service_hash)))

    assert response.status_code == 409
    assert not calls


@pytest.mark.parametrize('record', [
    None,
    _owner_record(controller_ip=None),
    _owner_record(controller_port=None),
    _owner_record(controller_ip='not-an-ip'),
    _owner_record(controller_pid=None),
    _owner_record(hash=None),
    _owner_record(status=serve_state.ServiceStatus.SHUTTING_DOWN),
    _owner_record(status=serve_state.ServiceStatus.FAILED_CLEANUP),
])
def test_proxy_rejects_missing_owner_without_forward(monkeypatch, record):
    monkeypatch.setattr(controller_proxy.serve_state,
                        'get_service_controller_owner',
                        lambda service_name: record)

    def unexpected_session():
        raise AssertionError('must not connect without a complete owner')

    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        unexpected_session)
    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync')))
    assert response.status_code == 503


def test_controller_failed_owner_remains_routable_for_lb_recovery(monkeypatch):
    record = _owner_record(status=serve_state.ServiceStatus.CONTROLLER_FAILED)
    monkeypatch.setattr(controller_proxy.serve_state,
                        'get_service_controller_owner',
                        lambda service_name: record)

    assert controller_proxy._get_controller_owner('svc') == _owner()


def test_proxy_connection_failure_is_503_without_retry(monkeypatch):
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner])
    calls = []

    class FailingRequest:

        async def __aenter__(self):
            raise aiohttp.ClientConnectionError('refused')

        async def __aexit__(self, exc_type, exc, traceback):
            del exc_type, exc, traceback

    monkeypatch.setattr(
        controller_proxy.aiohttp, 'ClientSession',
        lambda: _FakeClientSession(calls, response=FailingRequest()))
    response = asyncio.run(
        controller_proxy.proxy_load_balancer_sync(
            'svc',
            _request('/api/internal/serve/svc/controller/load_balancer_sync')))
    assert response.status_code == 503
    assert len(calls) == 1


@pytest.mark.parametrize('path,expected', [
    ('/api/internal/serve/svc/controller/load_balancer_sync', True),
    ('/api/internal/serve/svc/controller/load_balancer_role', True),
    ('/api/internal/serve/svc/controller/system_recovery_route_lease', True),
    ('/api/internal/serve/svc/controller/'
     'load_balancer_request_history_sync', True),
    ('/api/internal/serve//controller/load_balancer_sync', False),
    ('/api/internal/serve//controller/'
     'load_balancer_request_history_sync', False),
    ('/api/internal/serve//controller/load_balancer_role', False),
    ('/api/internal/serve//controller/system_recovery_route_lease', False),
    ('/api/internal/serve/a/b/controller/load_balancer_sync', False),
    ('/api/internal/serve/svc/controller/load_balancer_sync/more', False),
    ('/api/internal/serve/svc/controller/update_service', False),
])
def test_internal_route_match_is_exact(path, expected):
    assert controller_proxy.is_controller_sync_path(path) is expected


def test_internal_route_is_hidden_from_openapi():
    app = fastapi.FastAPI()
    app.include_router(controller_proxy.router)
    assert controller_proxy.CONTROLLER_SYNC_ROUTE_PATH not in app.openapi(
    )['paths']
    assert controller_proxy.CONTROLLER_ROLE_ROUTE_PATH not in app.openapi(
    )['paths']
    assert (controller_proxy.CONTROLLER_SYSTEM_RECOVERY_LEASE_ROUTE_PATH
            not in app.openapi()['paths'])
    assert (controller_proxy.CONTROLLER_HISTORY_SYNC_ROUTE_PATH
            not in app.openapi()['paths'])


def test_api_server_route_authenticates_and_proxies(monkeypatch):
    monkeypatch.setattr(server.serve_utils,
                        'get_lb_sync_auth_tokens',
                        lambda required=False: ('sync-token',))
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls))

    client = testclient.TestClient(server.app)
    rejected = client.post(
        '/api/internal/serve/svc/controller/load_balancer_sync',
        headers={'Authorization': 'Bearer wrong'},
        json={'request_aggregator': {}})
    assert rejected.status_code == 401
    assert not calls

    response = client.post(
        '/api/internal/serve/svc/controller/load_balancer_sync',
        headers={
            'Authorization': 'Bearer sync-token',
            constants.SERVICE_HASH_HEADER: 'service-incarnation-a',
        },
        json={'request_aggregator': {}})

    assert response.status_code == 200
    assert response.json() == {'replica_info': {}}
    assert len(calls) == 1


def test_api_server_history_route_uses_sync_auth(monkeypatch):
    monkeypatch.setattr(server.serve_utils,
                        'get_lb_sync_auth_tokens',
                        lambda required=False: ('sync-token',))
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    upstream = _FakeControllerResponse(
        body=b'{"request_history_accepted": true}')
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls, response=upstream))
    path = ('/api/internal/serve/svc/controller/'
            'load_balancer_request_history_sync')
    client = testclient.TestClient(server.app)

    rejected = client.post(path,
                           headers={'Authorization': 'Bearer wrong'},
                           json={'request_history': {}})
    assert rejected.status_code == 401
    assert not calls

    response = client.post(
        path,
        headers={
            'Authorization': 'Bearer sync-token',
            constants.SERVICE_HASH_HEADER: 'service-incarnation-a',
        },
        json={'request_history': {}})

    assert response.status_code == 200
    assert response.json() == {'request_history_accepted': True}
    assert len(calls) == 1


def test_api_server_role_route_uses_sync_auth(monkeypatch):
    monkeypatch.setattr(server.serve_utils,
                        'get_lb_sync_auth_tokens',
                        lambda required=False: ('sync-token',))
    owner = _owner()
    _patch_owner_reads(monkeypatch, [owner, owner])
    calls = []
    upstream = _FakeControllerResponse(body=b'{"role":"ACTIVE","generation":1}')
    monkeypatch.setattr(controller_proxy.aiohttp, 'ClientSession',
                        lambda: _FakeClientSession(calls, response=upstream))
    path = '/api/internal/serve/svc/controller/load_balancer_role'
    client = testclient.TestClient(server.app)

    rejected = client.post(path,
                           headers={'Authorization': 'Bearer wrong'},
                           json={'lb_slot': 'a'})
    assert rejected.status_code == 401
    assert not calls

    response = client.post(
        path,
        headers={
            'Authorization': 'Bearer sync-token',
            constants.SERVICE_HASH_HEADER: 'service-incarnation-a',
        },
        json={'lb_slot': 'a'})

    assert response.status_code == 200
    assert response.json() == {'role': 'ACTIVE', 'generation': 1}
    assert len(calls) == 1


def _run_auth_middleware(monkeypatch,
                         authorization,
                         tokens,
                         path=('/api/internal/serve/svc/controller/'
                               'load_balancer_sync')):
    reads = []

    def get_tokens(required=False):
        reads.append(required)
        if isinstance(tokens, Exception):
            raise tokens
        return tokens

    monkeypatch.setattr(server.serve_utils,
                        'get_lb_sync_auth_tokens',
                        get_tokens,
                        raising=False)
    middleware = server.InternalServeControllerSyncAuthMiddleware(
        app=lambda scope, receive, send: None)
    request = _request(path, authorization)
    request.state.auth_user = None
    downstream_users = []

    async def call_next(inner_request):
        downstream_users.append(inner_request.state.auth_user)
        return fastapi.responses.Response(status_code=204)

    response = asyncio.run(middleware.dispatch(request, call_next))
    return response, reads, downstream_users


@pytest.mark.parametrize(
    'authorization',
    [None, '', 'Basic current', 'Bearer wrong', 'Bearer current extra'])
def test_internal_auth_rejects_missing_or_bad_bearer(monkeypatch,
                                                     authorization):
    response, reads, downstream_users = _run_auth_middleware(
        monkeypatch, authorization, ('current', 'previous'))
    assert response.status_code == 401
    assert reads == [True]
    assert not downstream_users


@pytest.mark.parametrize('token', ['current', 'previous'])
def test_internal_auth_accepts_any_overlap_token(monkeypatch, token):
    response, reads, downstream_users = _run_auth_middleware(
        monkeypatch, f'Bearer {token}', ('current', 'previous'))
    assert response.status_code == 204
    assert reads == [True]
    assert len(downstream_users) == 1
    assert downstream_users[0].user_type == 'system'


def test_system_recovery_lease_route_uses_internal_sync_auth(monkeypatch):
    path = ('/api/internal/serve/svc/controller/'
            'system_recovery_route_lease')
    rejected, rejected_reads, rejected_users = _run_auth_middleware(
        monkeypatch, 'Bearer wrong', ('sync-token',), path)
    accepted, accepted_reads, accepted_users = _run_auth_middleware(
        monkeypatch, 'Bearer sync-token', ('sync-token',), path)

    assert rejected.status_code == 401
    assert rejected_reads == [True]
    assert rejected_users == []
    assert accepted.status_code == 204
    assert accepted_reads == [True]
    assert accepted_users[0].user_type == 'system'


@pytest.mark.parametrize('tokens', [(), RuntimeError('secret unavailable')])
def test_internal_auth_fails_closed_when_token_ring_unavailable(
        monkeypatch, tokens):
    response, reads, downstream_users = _run_auth_middleware(
        monkeypatch, 'Bearer token', tokens)
    assert response.status_code == 503
    assert reads == [True]
    assert not downstream_users


def test_internal_auth_fails_closed_on_cross_domain_overlap(
        monkeypatch, tmp_path):
    sync_ring = tmp_path / 'sync.tokens'
    sync_ring.write_text('sync\nshared\n', encoding='utf-8')
    admin_ring = tmp_path / 'admin.tokens'
    admin_ring.write_text('admin\nshared\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                       str(sync_ring))
    monkeypatch.setenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                       str(admin_ring))
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    middleware = server.InternalServeControllerSyncAuthMiddleware(
        app=lambda scope, receive, send: None)
    request = _request('/api/internal/serve/svc/controller/load_balancer_sync',
                       'Bearer shared')
    request.state.auth_user = None
    downstream_called = False

    async def call_next(unused_request):
        nonlocal downstream_called
        downstream_called = True
        return fastapi.responses.Response(status_code=204)

    response = asyncio.run(middleware.dispatch(request, call_next))
    assert response.status_code == 503
    assert not downstream_called


def test_api_server_startup_validates_controller_auth_isolation(monkeypatch):
    monkeypatch.setattr(server.serve_utils, 'is_external_load_balancer_mode',
                        lambda: True)
    validate = mock.Mock(
        side_effect=serve_utils.AuthTokenConfigurationError('rings overlap'))
    monkeypatch.setattr(server.serve_utils,
                        'validate_controller_auth_token_isolation', validate)

    with pytest.raises(serve_utils.AuthTokenConfigurationError,
                       match='rings overlap'):
        asyncio.run(server.lifespan(None).__aenter__())
    validate.assert_called_once_with(required=True)


def test_internal_auth_middleware_wraps_normal_auth():
    middleware_names = [
        middleware.cls.__name__ for middleware in server.app.user_middleware
    ]
    initialize_index = middleware_names.index(
        'InitializeRequestAuthUserMiddleware')
    internal_index = middleware_names.index(
        'InternalServeControllerSyncAuthMiddleware')
    bearer_index = middleware_names.index('BearerTokenMiddleware')
    oauth_index = middleware_names.index('OAuth2ProxyMiddleware')
    assert initialize_index < internal_index < bearer_index
    assert internal_index < oauth_index
