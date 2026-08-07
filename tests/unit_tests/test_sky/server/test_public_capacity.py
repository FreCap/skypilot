"""Tests for the unauthenticated public capacity API."""

# pylint: disable=protected-access

import contextlib
import datetime
from types import SimpleNamespace
from unittest import mock

import fastapi
from fastapi import testclient
import pytest

from sky import models
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import public_capacity
from sky.server import server
from sky.server.auth import middleware as auth_middleware
from sky.server.auth import oauth2_proxy


def _node(gpu_type='H200',
          total=8,
          available=2,
          preemptible=3,
          *,
          ready=True,
          cordoned=False,
          taints=None):
    return models.KubernetesNodeInfo(
        name='node',
        accelerator_type=gpu_type,
        total={'accelerator_count': total},
        free={'accelerators_available': available},
        is_ready=ready,
        is_cordoned=cordoned,
        taints=[] if taints is None else taints,
        accelerators_preemptible=preemptible,
    )


def _nodes_info(*nodes):
    return models.KubernetesNodesInfo(node_info_dict={
        f'node-{index}': node for index, node in enumerate(nodes)
    },
                                      hint='')


def _snapshot(generated_at=None):
    if generated_at is None:
        generated_at = datetime.datetime(2026,
                                         8,
                                         7,
                                         tzinfo=datetime.timezone.utc)
    return public_capacity.PublicCapacityResponse(
        generated_at=generated_at,
        partial=False,
        clusters=(),
        jobs_by_user=(public_capacity.PublicUserJobs(user='user@example.com',
                                                     active_jobs=1,
                                                     statuses={'RUNNING': 1}),),
        jobs_status='ok')


def test_public_auth_predicate_is_exact_get_only():
    path = server_constants.PUBLIC_CAPACITY_PATH

    assert server_constants.is_unauthenticated_public_request('GET', path)
    assert not server_constants.is_unauthenticated_public_request('POST', path)
    assert not server_constants.is_unauthenticated_public_request(
        'GET', f'{path}/extra')
    assert not server_constants.is_unauthenticated_public_request(
        'GET', '/api/v1/public')


def test_gpu_capacity_buckets_are_mutually_exclusive():
    nodes_info = _nodes_info(
        _node(total=8, available=2, preemptible=3),
        _node(total=4, available=0, preemptible=0, ready=False),
        _node(gpu_type='A100', total=4, available=4, preemptible=None),
    )

    rows, partial = public_capacity._aggregate_gpu_capacity(nodes_info)

    assert not partial
    assert [row.gpu_type for row in rows] == ['A100', 'H200']
    assert rows[0].model_dump(by_alias=True) == {
        'type': 'A100',
        'total': 4,
        'used': 0,
        'preemptible': 0,
        'available': 4,
        'unavailable': 0,
    }
    assert rows[1].model_dump(by_alias=True) == {
        'type': 'H200',
        'total': 12,
        'used': 3,
        'preemptible': 3,
        'available': 2,
        'unavailable': 4,
    }


@pytest.mark.parametrize('node', [
    _node(available=-1, preemptible=None),
    _node(available=2, preemptible=7),
    _node(available=9, preemptible=0),
])
def test_gpu_capacity_preserves_unknown_allocation(node):
    rows, partial = public_capacity._aggregate_gpu_capacity(_nodes_info(node))

    assert partial
    assert len(rows) == 1
    assert rows[0].total == 8
    assert rows[0].used is None
    assert rows[0].preemptible is None
    assert rows[0].available is None
    assert rows[0].unavailable == 0


def test_observe_context_returns_sanitized_partial_failure(monkeypatch):
    monkeypatch.setattr(public_capacity.kubernetes_utils,
                        'get_kubernetes_node_info',
                        mock.Mock(side_effect=RuntimeError('secret path')))

    cluster, partial = public_capacity._observe_context('research', 'default')

    assert partial
    assert cluster.model_dump() == {
        'name': 'research',
        'status': 'temporarily_unavailable',
        'gpus': (),
    }


def test_context_discovery_is_deduplicated_and_deterministic(monkeypatch):
    refresh = mock.Mock()
    monkeypatch.setattr(public_capacity.server_common,
                        'refresh_workspace_state_for_sync_handler', refresh)
    monkeypatch.setattr(public_capacity.workspaces_core,
                        'get_configured_workspace_names',
                        lambda: {'zeta', 'alpha', 'broken'})
    monkeypatch.setattr(public_capacity.skypilot_config,
                        'local_active_workspace_ctx',
                        lambda _: contextlib.nullcontext())
    existing_contexts = mock.Mock(side_effect=[
        ['shared', 'alpha-only'],
        RuntimeError('private kubeconfig path'),
        ['zeta-only', 'shared'],
    ])
    monkeypatch.setattr(public_capacity.clouds.Kubernetes,
                        'existing_allowed_contexts', existing_contexts)

    contexts, partial = public_capacity._discover_contexts()

    assert partial
    assert contexts == [
        ('alpha-only', 'alpha'),
        ('shared', 'alpha'),
        ('zeta-only', 'zeta'),
    ]
    refresh.assert_called_once_with()


def test_jobs_are_deduplicated_and_grouped_by_user(monkeypatch):
    monkeypatch.setattr(
        public_capacity, '_active_managed_job_rows', lambda: [
            {
                'job_id': 1,
                'user_hash': 'alice',
                'status': 'RUNNING'
            },
            {
                'job_id': 1,
                'user_hash': 'alice',
                'status': 'SUCCEEDED'
            },
            {
                'job_id': 2,
                'user_hash': 'alice',
                'status': 'PENDING'
            },
            {
                'job_id': 2,
                'user_hash': 'alice',
                'status': 'RUNNING'
            },
            {
                'job_id': 3,
                'user_hash': 'missing',
                'status': 'PENDING'
            },
            {
                'job_id': 4,
                'user_hash': 'alice',
                'status': 'SUCCEEDED'
            },
        ])
    monkeypatch.setattr(
        public_capacity.global_user_state, 'get_all_users', lambda: [
            SimpleNamespace(id='alice', name='alice@example.com'),
        ])

    rows, status = public_capacity._aggregate_jobs_by_user()

    assert status == 'ok'
    assert [row.model_dump() for row in rows] == [
        {
            'user': 'alice@example.com',
            'active_jobs': 2,
            'statuses': {
                'MIXED': 1,
                'RUNNING': 1
            },
        },
        {
            'user': None,
            'active_jobs': 1,
            'statuses': {
                'PENDING': 1
            },
        },
    ]


@pytest.mark.parametrize('consolidation_mode', [True, False])
def test_active_job_source_is_read_only_and_all_users(monkeypatch,
                                                      consolidation_mode):
    expected = [{'job_id': 1, 'user_hash': 'alice', 'status': 'RUNNING'}]
    consolidated_read = mock.Mock(return_value=(expected, 1))
    legacy_read = mock.Mock(return_value=(expected, 1, {}, 1))
    monkeypatch.setattr(public_capacity.managed_jobs_utils,
                        'is_consolidation_mode', lambda: consolidation_mode)
    monkeypatch.setattr(public_capacity.managed_job_state_queries,
                        'get_managed_jobs_with_filters', consolidated_read)
    monkeypatch.setattr(public_capacity.managed_jobs_core, 'queue_v2',
                        legacy_read)

    assert public_capacity._active_managed_job_rows() == expected
    if consolidation_mode:
        consolidated_read.assert_called_once_with(
            fields=['job_id', 'user_hash', 'status'], skip_finished=True)
        legacy_read.assert_not_called()
    else:
        legacy_read.assert_called_once_with(
            refresh=False,
            skip_finished=True,
            all_users=True,
            fields=['job_id', 'user_hash', 'status'])
        consolidated_read.assert_not_called()


def test_jobs_failure_is_sanitized(monkeypatch):
    monkeypatch.setattr(
        public_capacity, '_active_managed_job_rows',
        mock.Mock(side_effect=RuntimeError('postgresql://secret')))

    rows, status = public_capacity._aggregate_jobs_by_user()

    assert not rows
    assert status == 'temporarily_unavailable'


def test_public_capacity_cache_is_single_flight_and_returns_copies(monkeypatch):
    public_capacity._reset_cache_for_tests()
    build = mock.Mock(side_effect=[_snapshot(), _snapshot()])
    monotonic = mock.Mock(side_effect=[0.0, 0.0, 1.0, 16.0, 16.0])
    monkeypatch.setattr(public_capacity, '_build_public_capacity', build)
    monkeypatch.setattr(public_capacity.time, 'monotonic', monotonic)

    first = public_capacity.get_public_capacity()
    first.jobs_by_user[0].statuses['RUNNING'] = 99
    second = public_capacity.get_public_capacity()
    third = public_capacity.get_public_capacity()

    assert build.call_count == 2
    assert second.jobs_by_user[0].statuses == {'RUNNING': 1}
    assert third.jobs_by_user[0].statuses == {'RUNNING': 1}


def test_public_capacity_route_is_registered():
    included = [
        route for route in server.app.routes
        if getattr(route, 'original_router', None) is public_capacity.router
    ]
    matches = [
        route for route in public_capacity.router.routes
        if getattr(route, 'path', None) == server_constants.PUBLIC_CAPACITY_PATH
    ]

    assert len(included) == 1
    assert len(matches) == 1
    assert matches[0].methods == {'GET'}


def test_public_route_end_to_end_allows_invalid_credential(monkeypatch):
    monkeypatch.setattr(public_capacity, 'get_public_capacity', _snapshot)

    response = testclient.TestClient(server.app).get(
        server_constants.PUBLIC_CAPACITY_PATH,
        headers={'Authorization': 'Bearer sky_intentionally_invalid'})

    assert response.status_code == 200
    assert response.headers['Cache-Control'] == 'public, max-age=15'
    assert response.json() == {
        'version': 1,
        'generated_at': '2026-08-07T00:00:00Z',
        'partial': False,
        'clusters': [],
        'jobs_by_user': [{
            'user': 'user@example.com',
            'active_jobs': 1,
            'statuses': {
                'RUNNING': 1
            },
        }],
        'jobs_status': 'ok',
    }


@pytest.mark.asyncio
async def test_public_capacity_handler_sets_public_cache_header(monkeypatch):
    snapshot = _snapshot()
    monkeypatch.setattr(public_capacity, 'get_public_capacity',
                        lambda: snapshot)
    response = fastapi.Response()

    result = await public_capacity.public_capacity(response)

    assert result == snapshot
    assert response.headers['Cache-Control'] == 'public, max-age=15'


def _request(path=None, method='GET', headers=None):
    request = mock.Mock(spec=fastapi.Request)
    request.method = method
    request.url = mock.Mock()
    request.url.path = path or server_constants.PUBLIC_CAPACITY_PATH
    request.state = mock.Mock()
    request.state.auth_user = None
    request.headers = {} if headers is None else headers
    request.cookies = {}
    return request


@pytest.mark.asyncio
@pytest.mark.parametrize('middleware_class', [
    auth_middleware.BasicAuthMiddleware,
    auth_middleware.BearerTokenMiddleware,
])
async def test_public_route_bypasses_credential_middleware(middleware_class):
    middleware = middleware_class(app=mock.AsyncMock()).middleware
    request = _request(headers={'authorization': 'Bearer sky_invalid'})
    call_next = mock.AsyncMock(return_value=fastapi.Response(status_code=200))

    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200
    assert request.state.anonymous_user is True
    call_next.assert_awaited_once_with(request)


@pytest.mark.asyncio
async def test_public_route_bypasses_header_auth_without_registration(
        monkeypatch):
    config = server_config.ExternalProxyConfig(
        enabled=True,
        header_name='X-Auth-Request-Email',
        header_format='plaintext')
    monkeypatch.setattr(auth_middleware.server_config,
                        'load_external_proxy_config', lambda: config)
    middleware = auth_middleware.AuthProxyMiddleware(
        app=mock.AsyncMock()).middleware
    request = _request(headers={'X-Auth-Request-Email': 'attacker@example.com'})
    call_next = mock.AsyncMock(return_value=fastapi.Response(status_code=200))
    registration = mock.AsyncMock(side_effect=AssertionError('must not run'))
    monkeypatch.setattr(auth_middleware.user_registration,
                        'add_or_update_user_with_default_role', registration)

    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200
    assert request.state.anonymous_user is True
    registration.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_route_bypasses_oauth_proxy(monkeypatch):
    monkeypatch.setenv(server_constants.OAUTH2_PROXY_ENABLED_ENV_VAR, 'true')
    monkeypatch.setenv(server_constants.OAUTH2_PROXY_BASE_URL_ENV_VAR,
                       'http://oauth-proxy')
    middleware = oauth2_proxy.OAuth2ProxyMiddleware(
        app=mock.AsyncMock()).middleware
    request = _request()
    call_next = mock.AsyncMock(return_value=fastapi.Response(status_code=200))

    with mock.patch('aiohttp.ClientSession') as session:
        response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200
    assert request.state.anonymous_user is True
    session.assert_not_called()
