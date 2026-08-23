"""Tests for external-LB auth: inbound data-plane bearer + control-plane sync.

Independent tokens:
  - LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR gates INBOUND inference requests (data
    plane) via a dedicated header consumed by the LB. Legacy deployments fall
    back to token presence when the capability env is absent. The readiness
    route is exempt so the k8s probe still works.
  - LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR is presented by the LB on every sync so
    the controller accepts it. The legacy controller-admin singleton never
    grants sync access. The request aggregator must be cleared only after a
    SUCCESSFUL sync -- a failed sync (e.g. 401) must not drop the load signal
    the controller never received.

Logic-only: no assertions on log or exception message text.
"""
# pylint: disable=invalid-name,protected-access,missing-class-docstring
# pylint: disable=unused-argument,use-implicit-booleaness-not-comparison
import asyncio
import dataclasses
import hashlib
import inspect
import json
import pickle
from unittest import mock

import aiohttp
import fastapi
from fastapi.testclient import TestClient
import httpx
import pytest

from sky.serve import async_request_ledger_client
from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import load_balancer
from sky.serve import serve_utils
from sky.serve.server import controller_proxy


@pytest.fixture(autouse=True)
def _clear_token_file_envs(monkeypatch, tmp_path):
    for env_var in (constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                    constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                    constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                    constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR):
        monkeypatch.delenv(env_var, raising=False)
    sync_ring = tmp_path / 'default-sync.tokens'
    sync_ring.write_text('default-sync-token\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                       str(sync_ring))
    monkeypatch.setenv(constants.LB_POD_UID_ENV_VAR, 'test-pod-uid')


def _make_lb() -> load_balancer.SkyServeLoadBalancer:
    return load_balancer.SkyServeLoadBalancer(controller_url='http://ctrl:8001',
                                              load_balancer_port=8890,
                                              service_hash='incarnation-a')


def _run(coro):
    return asyncio.run(coro)


def _scope(path, method='GET', headers=None):
    # ASGI http scope: headers are (name, value) latin-1 byte tuples.
    hdrs = [(k.encode('latin-1'), v.encode('latin-1'))
            for k, v in (headers or {}).items()]
    return {'type': 'http', 'method': method, 'path': path, 'headers': hdrs}


def _authorized(scope) -> bool:
    return load_balancer._InboundAuthMiddleware._authorized(scope)


def _edge_auth(token: str):
    return {constants.LB_AUTHORIZATION_HEADER: f'Bearer {token}'}


def test_auth_token_public_facade_contract():
    symbols = (
        serve_utils.AuthTokenConfigurationError,
        serve_utils.is_lb_data_plane_auth_enabled,
        serve_utils.validate_controller_auth_token_isolation,
        serve_utils.get_lb_sync_auth_tokens,
        serve_utils.get_controller_admin_auth_tokens,
        serve_utils.get_lb_auth_tokens,
    )
    assert all(symbol.__module__ == serve_utils.__name__ for symbol in symbols)

    assert not inspect.signature(
        serve_utils.is_lb_data_plane_auth_enabled).parameters
    for getter in symbols[2:]:
        parameters = inspect.signature(getter).parameters
        assert tuple(parameters) == ('required',)
        assert parameters['required'].default is False


def test_auth_token_configuration_error_pickle_round_trip():
    error = serve_utils.AuthTokenConfigurationError('invalid token ring')
    restored = pickle.loads(pickle.dumps(error))

    assert type(restored) is serve_utils.AuthTokenConfigurationError
    assert restored.args == error.args


# --------------------------------------------------------------------------- #
# Inbound data-plane bearer middleware (pure-ASGI _authorized decision)
# --------------------------------------------------------------------------- #
def test_inbound_auth_disabled_authorizes_all(monkeypatch):
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    assert _authorized(_scope('/predict'))


def test_explicit_disabled_overrides_stale_auth_material(monkeypatch, tmp_path):
    ring = tmp_path / 'stale.tokens'
    ring.write_text('stale\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 'also-stale')
    monkeypatch.setenv(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR, 'false')

    assert not serve_utils.is_lb_data_plane_auth_enabled()
    assert _authorized(_scope('/predict'))


def test_explicit_enabled_requires_auth_material(monkeypatch):
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    monkeypatch.setenv(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR, 'true')

    assert serve_utils.is_lb_data_plane_auth_enabled()
    with pytest.raises(serve_utils.AuthTokenConfigurationError):
        _authorized(_scope('/predict'))


def test_malformed_data_plane_capability_fails_closed(monkeypatch):
    monkeypatch.setenv(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR, 'TRUE')

    with pytest.raises(serve_utils.AuthTokenConfigurationError,
                       match='exactly'):
        _authorized(_scope('/predict'))


class _NoopDrainableServer:

    def __init__(self, *args, **kwargs):
        del args, kwargs

    async def serve_with_drain(self):
        return


def test_lb_process_starts_without_optional_data_auth(monkeypatch):
    monkeypatch.setenv(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR, 'false')
    sync_tokens = mock.Mock(return_value=('sync',))
    data_tokens = mock.Mock(
        side_effect=AssertionError('disabled data auth must not be required'))
    monkeypatch.setattr(serve_utils, 'get_lb_sync_auth_tokens', sync_tokens)
    monkeypatch.setattr(serve_utils, 'get_lb_auth_tokens', data_tokens)
    monkeypatch.setattr(load_balancer, '_DrainableServer', _NoopDrainableServer)
    config_args = []
    monkeypatch.setattr(
        load_balancer.uvicorn, 'Config',
        lambda *args, **kwargs: config_args.append((args, kwargs)) or object())
    lb = _make_lb()
    monkeypatch.setattr(lb, '_get_lb_session_id', lambda: 'pod-uid')

    lb.run()

    sync_tokens.assert_called_once_with(required=True)
    data_tokens.assert_not_called()
    assert config_args == [((lb._app,), {
        'host': '0.0.0.0',
        'port': 8890,
    })]


def test_lb_process_requires_data_ring_when_enabled(monkeypatch):
    monkeypatch.setenv(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR, 'true')
    sync_tokens = mock.Mock(return_value=('sync',))
    data_tokens = mock.Mock(return_value=('data',))
    monkeypatch.setattr(serve_utils, 'get_lb_sync_auth_tokens', sync_tokens)
    monkeypatch.setattr(serve_utils, 'get_lb_auth_tokens', data_tokens)
    monkeypatch.setattr(load_balancer, '_DrainableServer', _NoopDrainableServer)
    lb = _make_lb()
    monkeypatch.setattr(lb, '_get_lb_session_id', lambda: 'pod-uid')

    lb.run()

    sync_tokens.assert_called_once_with(required=True)
    data_tokens.assert_called_once_with(required=True)


def test_inbound_health_get_head_exempt(monkeypatch):
    # The readiness probe must reach /_lb/health even with auth enabled, or k8s
    # would never mark the pod Ready.
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    assert _authorized(_scope(constants.LB_HEALTH_ENDPOINT_PATH, 'GET'))
    assert _authorized(_scope(constants.LB_HEALTH_ENDPOINT_PATH, 'HEAD'))


def test_inbound_non_get_health_not_exempt(monkeypatch):
    # Only GET/HEAD are exempt; other methods on the health path fall through to
    # the authed proxy, so they must require the token.
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    for method in ('POST', 'PUT', 'DELETE'):
        assert not _authorized(_scope(constants.LB_HEALTH_ENDPOINT_PATH,
                                      method))


def test_inbound_correct_token_accepted(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    assert _authorized(_scope('/predict', headers=_edge_auth('s3cret')))


def test_standard_authorization_is_reserved_for_replica(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    assert not _authorized(
        _scope('/predict', headers={'authorization': 'Bearer s3cret'}))


@pytest.mark.parametrize(
    'bad',
    [
        None,
        'Bearer wrong',
        's3cret',  # missing the "Bearer " scheme
        'Bearer ',
        '',
        'Bearer ñ',  # non-ASCII must be rejected, not crash compare_digest
    ])
def test_inbound_wrong_or_missing_rejected(monkeypatch, bad):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    headers = ({} if bad is None else {constants.LB_AUTHORIZATION_HEADER: bad})
    assert not _authorized(_scope('/predict', headers=headers))


def test_file_token_ring_is_live_and_legacy_env_is_only_fallback(
        monkeypatch, tmp_path):
    ring = tmp_path / 'lb.tokens'
    ring.write_text('new\nold\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 'legacy')
    assert serve_utils.get_lb_auth_tokens() == ('new', 'old')

    # The same process observes a projected-secret rewrite on the next call.
    ring.write_text('next\nnew\n', encoding='utf-8')
    assert serve_utils.get_lb_auth_tokens() == ('next', 'new')

    monkeypatch.delenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR)
    assert serve_utils.get_lb_auth_tokens() == ('legacy',)


@pytest.mark.parametrize('contents',
                         ['', '\n', 'ok\n\nbad\n', 'bad token\n', 'nñ\n'])
def test_file_token_ring_rejects_empty_or_malformed(monkeypatch, tmp_path,
                                                    contents):
    ring = tmp_path / 'lb.tokens'
    ring.write_text(contents, encoding='utf-8')
    monkeypatch.setenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    with pytest.raises(serve_utils.AuthTokenConfigurationError):
        serve_utils.get_lb_auth_tokens(required=True)


def test_required_token_ring_rejects_missing_or_unreadable(
        monkeypatch, tmp_path):
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    with pytest.raises(serve_utils.AuthTokenConfigurationError):
        serve_utils.get_lb_auth_tokens(required=True)

    monkeypatch.setenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                       str(tmp_path / 'missing'))
    with pytest.raises(serve_utils.AuthTokenConfigurationError):
        serve_utils.get_lb_auth_tokens(required=True)


def test_inbound_token_ring_accepts_overlap(monkeypatch, tmp_path):
    ring = tmp_path / 'lb.tokens'
    ring.write_text('new\nold\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    for token in ('new', 'old'):
        assert _authorized(_scope('/predict', headers=_edge_auth(token)))
    assert not _authorized(_scope('/predict', headers=_edge_auth('stale')))


def test_inbound_and_control_plane_tokens_are_independent(monkeypatch):
    # Setting the control-plane token must NOT enable inbound auth, and vice
    # versa -- an inference client's token must never reach the controller.
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, 'ctrl')
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    assert not serve_utils.get_lb_auth_tokens()
    # Inbound auth disabled -> authorized despite the control-plane token.
    assert _authorized(_scope('/predict'))


# --------------------------------------------------------------------------- #
# Control-plane token on sync + aggregator-clear ordering
# --------------------------------------------------------------------------- #
class _FakeResp:

    def __init__(self, status, captured):
        self._status = status
        self._captured = captured

    def raise_for_status(self):
        if self._status >= 400:
            raise aiohttp.ClientResponseError(request_info=mock.MagicMock(),
                                              history=(),
                                              status=self._status)

    @property
    def status(self):
        return self._status

    async def json(self):
        return self._captured.get('response_json', {
            'replica_info': {},
            'routing_spec': None,
        })

    async def __aenter__(self):
        on_response_enter = self._captured.get('on_response_enter')
        if on_response_enter is not None:
            on_response_enter()
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:

    def __init__(self, status, captured):
        self._statuses = list(status) if isinstance(status, list) else [status]
        self._captured = captured

    def post(self, *args, **kwargs):
        self._captured['url'] = args[0]
        self._captured['headers'] = kwargs.get('headers')
        self._captured['json'] = kwargs.get('json')
        self._captured['timeout'] = kwargs.get('timeout')
        self._captured.setdefault('headers_history',
                                  []).append(kwargs.get('headers'))
        self._captured.setdefault('json_history', []).append(kwargs.get('json'))
        status = self._statuses.pop(0)
        return _FakeResp(status, self._captured)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _sync_once(monkeypatch, lb, status, captured):
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession',
                        lambda *a, **k: _FakeSession(status, captured))
    _run(lb._sync_with_controller_once())


def test_sync_sends_control_plane_bearer(monkeypatch, tmp_path):
    ring = tmp_path / 'sync.tokens'
    ring.write_text('ctrl-tok\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    lb = _make_lb()
    captured = {}
    _sync_once(monkeypatch, lb, 200, captured)
    assert captured['headers'] == {
        'Authorization': 'Bearer ctrl-tok',
        constants.SERVICE_HASH_HEADER: 'incarnation-a',
    }


def test_role_heartbeat_client_url_matches_registered_proxy_route(monkeypatch):
    controller_url = 'http://api/api/internal/serve/svc'
    lb = load_balancer.SkyServeLoadBalancer(controller_url=controller_url,
                                            load_balancer_port=8890,
                                            service_hash='incarnation-a',
                                            lb_slot='a')
    captured = {
        'response_json': {
            'role': lb_ha.LbRole.ACTIVE.value,
            'generation': 1,
        }
    }
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession',
                        lambda *a, **k: _FakeSession(200, captured))
    monkeypatch.setattr(lb, '_ha_role_payload', lambda: {'lb_slot': 'a'})

    _run(lb._sync_role_with_controller_once())

    expected_path = controller_proxy.CONTROLLER_ROLE_ROUTE_PATH.replace(
        '{service_name}', 'svc')
    assert captured['url'] == f'http://api{expected_path}'
    assert captured['url'] == (controller_url +
                               constants.LB_CONTROLLER_ROLE_PATH)
    assert captured['timeout'].total == 8
    assert lb._lb_role is lb_ha.LbRole.ACTIVE


def test_sync_without_token_fails_before_http_request(monkeypatch):
    monkeypatch.delenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR)
    lb = _make_lb()
    captured = {}
    with pytest.raises(serve_utils.AuthTokenConfigurationError):
        _sync_once(monkeypatch, lb, 200, captured)
    assert captured == {}


def test_sync_ring_falls_back_only_after_401_without_redraining(
        monkeypatch, tmp_path):
    ring = tmp_path / 'sync.tokens'
    ring.write_text('primary\noverlap\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    lb = _make_lb()
    lb._request_aggregator.timestamps.extend([1, 2, 3])
    captured = {}

    _sync_once(monkeypatch, lb, [401, 200], captured)

    assert captured['headers_history'] == [
        {
            'Authorization': 'Bearer primary',
            constants.SERVICE_HASH_HEADER: 'incarnation-a',
        },
        {
            'Authorization': 'Bearer overlap',
            constants.SERVICE_HASH_HEADER: 'incarnation-a',
        },
    ]
    assert len(captured['json_history']) == 2
    assert captured['json_history'][0] is captured['json_history'][1]
    assert captured['json_history'][0]['request_aggregator']['timestamps'] == [
        1, 2, 3
    ]
    assert lb._request_aggregator.to_dict()['timestamps'] == []


def test_sync_ring_does_not_fallback_on_non_401(monkeypatch, tmp_path):
    ring = tmp_path / 'sync.tokens'
    ring.write_text('primary\noverlap\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR, str(ring))
    lb = _make_lb()
    lb._request_aggregator.timestamps.append(1)
    captured = {}

    _sync_once(monkeypatch, lb, [500], captured)

    assert captured['headers_history'] == [{
        'Authorization': 'Bearer primary',
        constants.SERVICE_HASH_HEADER: 'incarnation-a',
    }]
    assert lb._request_aggregator.to_dict()['timestamps'] == [1]


def _client_with_routes(lb,
                        *,
                        include_receipt_lookup: bool = True) -> TestClient:
    """Register middleware + routes exactly like SkyServeLoadBalancer.run(), with
    a stub catch-all proxy, so tests exercise the REAL FastAPI stack (middleware
    + route matching), not just the middleware method in isolation."""
    lb._app.add_middleware(load_balancer._InboundAuthMiddleware)
    lb._app.add_api_route(constants.LB_HEALTH_ENDPOINT_PATH,
                          lb._health,
                          methods=['GET'])
    lb._app.add_api_route(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                          lb._prediction_completed,
                          methods=['POST'])
    if include_receipt_lookup:
        lb._app.add_api_route(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                              lb._async_request_receipt,
                              methods=['POST'])

    async def _proxy(request: fastapi.Request):
        del request
        return fastapi.responses.PlainTextResponse('PROXY')

    lb._app.add_api_route('/{path:path}',
                          _proxy,
                          methods=['GET', 'POST', 'PUT', 'DELETE'])
    return TestClient(lb._app)


def _receipt_lookup_payload() -> dict:
    return {
        'ledger_protocol_version': 1,
        'request_id': 'job-exact-1',
        'intent_sha256': 'a' * 64,
    }


def test_async_receipt_lookup_is_authenticated_and_read_only(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    receipt = dataclasses.replace(_exact_completion_receipt(attempt_id),
                                  state='ACCEPTED',
                                  revision=2,
                                  duplicate=True)
    lookup = mock.AsyncMock(return_value=receipt)
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    client = _client_with_routes(lb)

    assert client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                       json=_receipt_lookup_payload()).status_code == 401
    response = client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                           json=_receipt_lookup_payload(),
                           headers=_edge_auth('s3cret'))

    assert response.status_code == 200
    assert response.json() == dataclasses.asdict(receipt)
    assert response.headers[constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER] == '1'
    assert response.headers[constants.LB_ASYNC_ATTEMPT_ID_HEADER] == attempt_id
    assert response.headers[constants.LB_ASYNC_ATTEMPT_NO_HEADER] == '1'
    assert response.headers[constants.LB_ASYNC_LEDGER_REVISION_HEADER] == '2'
    assert response.headers[
        constants.LB_ASYNC_LEDGER_STATE_HEADER] == 'ACCEPTED'
    lookup.assert_awaited_once_with('job-exact-1', 'a' * 64)


def test_async_receipt_lookup_advertises_exact_not_found_without_receipt(
        monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    lookup = mock.AsyncMock(return_value=None)
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    client = _client_with_routes(lb)

    response = client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                           json=_receipt_lookup_payload(),
                           headers=_edge_auth('s3cret'))

    assert response.status_code == 404
    assert response.json() == {'detail': 'No durable request attempt exists.'}
    assert response.headers[constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER] == '1'
    for header in (constants.LB_ASYNC_ATTEMPT_ID_HEADER,
                   constants.LB_ASYNC_ATTEMPT_NO_HEADER,
                   constants.LB_ASYNC_LEDGER_REVISION_HEADER,
                   constants.LB_ASYNC_LEDGER_STATE_HEADER):
        assert header not in response.headers
    lookup.assert_awaited_once()


def test_async_receipt_lookup_does_not_advertise_unsynchronized_authority(
        monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = None
    lookup = mock.AsyncMock()
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    client = _client_with_routes(lb)

    response = client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                           json=_receipt_lookup_payload(),
                           headers=_edge_auth('s3cret'))

    assert response.status_code == 503
    assert constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER not in response.headers
    lookup.assert_not_awaited()


@pytest.mark.parametrize('payload', [{}, {
    'ledger_protocol_version': True,
    'request_id': 'job-exact-1',
    'intent_sha256': 'a' * 64,
}, {
    **_receipt_lookup_payload(),
    'extra': True,
}, {
    **_receipt_lookup_payload(),
    'intent_sha256': 'not-a-digest',
}])
def test_async_receipt_lookup_rejects_malformed_identity(monkeypatch, payload):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    lookup = mock.AsyncMock()
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    client = _client_with_routes(lb)

    response = client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                           json=payload,
                           headers=_edge_auth('s3cret'))

    assert response.status_code == 422
    lookup.assert_not_awaited()


def test_async_receipt_lookup_rejects_duplicates_and_oversized_body(
        monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    monkeypatch.setattr(constants, 'LB_ASYNC_REQUEST_LEDGER_MAX_BYTES', 256)
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    lookup = mock.AsyncMock()
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    client = _client_with_routes(lb)
    headers = {
        **_edge_auth('s3cret'),
        'content-type': 'application/json',
    }

    duplicate = client.post(
        constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
        content=('{' + '"ledger_protocol_version":1,' * 2 +
                 '"request_id":"job-exact-1","intent_sha256":"' + 'a' * 64 +
                 '"}'),
        headers=headers)
    oversized = client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                            content=b'x' * 257,
                            headers=headers)

    assert duplicate.status_code == 422
    assert oversized.status_code == 413
    lookup.assert_not_awaited()


def test_old_lb_receipt_lookup_absence_never_advertises_replay_authority(
        monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb(), include_receipt_lookup=False)

    response = client.post(constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH,
                           json=_receipt_lookup_payload(),
                           headers=_edge_auth('s3cret'))

    assert response.status_code == 200
    assert response.text == 'PROXY'
    assert constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER not in response.headers


def test_stack_health_get_exempt_but_proxy_requires_auth(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())
    # GET /_lb/health is the readiness probe -> exempt (reaches health handler,
    # which returns 503 until first sync; the point is it is NOT 401).
    assert client.get(constants.LB_HEALTH_ENDPOINT_PATH).status_code != 401
    # Inference path requires auth.
    assert client.get('/predict').status_code == 401
    assert client.get('/predict',
                      headers=_edge_auth('s3cret')).status_code == 200


def test_prediction_completion_callback_records_and_deduplicates(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    client = _client_with_routes(lb)
    payload = {
        'request_id': 'job-1',
        'status': 'SUCCEEDED',
        'processing_time_ms': 5000,
    }

    assert client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                       json=payload).status_code == 401
    for _ in range(2):
        response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                               headers=_edge_auth('s3cret'),
                               json=payload)
        assert response.status_code == 204
    # A fallback async_status observation for the same request stays a no-op.
    assert lb._record_async_prediction_status(
        b'{"request_id":"job-1","status":"SUCCEEDED",'
        b'"processing_time_ms":5000}', '')
    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers=_edge_auth('s3cret'),
                           json={
                               'request_id': 'job-2',
                               'status': 'FAILED',
                               'processing_time_ms': 30000,
                           })
    assert response.status_code == 204

    counts = lb._request_aggregator.prediction_time_history_snapshot(
    )['buckets'][0]['outcome_counts']
    assert counts['succeeded'][5] == 1
    assert counts['failed'][7] == 1
    assert sum(counts['succeeded']) + sum(counts['failed']) == 2


def _exact_completion_payload(attempt_id: str) -> dict:
    return {
        'ledger_protocol_version': 1,
        'request_id': 'job-exact-1',
        'intent_sha256': 'a' * 64,
        'attempt_id': attempt_id,
        'attempt_no': 1,
        'expected_revision': 4,
        'status': 'SUCCEEDED',
        'processing_time_us': 5_000_000,
    }


def _exact_completion_receipt(attempt_id: str, *, duplicate: bool = False):
    return async_request_ledger_client.AsyncLedgerReceipt(
        request_key_sha256=hashlib.sha256(b'job-exact-1').hexdigest(),
        attempt_id=attempt_id,
        attempt_no=1,
        state='SUCCEEDED',
        revision=5,
        duplicate=duplicate,
        dispatch_authorized=False)


def _install_exact_completion_lookup(monkeypatch,
                                     lb,
                                     attempt_id: str,
                                     revision: int = 4):
    current = dataclasses.replace(_exact_completion_receipt(attempt_id),
                                  state='ACCEPTED',
                                  revision=revision,
                                  duplicate=True)
    lookup = mock.AsyncMock(return_value=current)
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    return lookup


def test_exact_completion_persists_before_compatibility_histogram(monkeypatch):
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    _install_exact_completion_lookup(monkeypatch, lb, attempt_id)
    events = []

    async def _post(payload):
        del payload
        events.append('persist')
        return _exact_completion_receipt(attempt_id)

    monkeypatch.setattr(lb, '_post_async_ledger', _post)
    monkeypatch.setattr(lb, '_record_prediction_time',
                        lambda *_args: events.append('aggregate'))

    response = _run(
        lb._record_exact_async_prediction_payload(
            _exact_completion_payload(attempt_id)))

    assert response.status_code == 204
    assert events == ['persist', 'aggregate']


def test_exact_completion_rejects_inexact_receipt_before_histogram(monkeypatch):
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    _install_exact_completion_lookup(monkeypatch, lb, attempt_id)
    inexact = _exact_completion_receipt('22222222-2222-4222-8222-222222222222')
    monkeypatch.setattr(lb, '_post_async_ledger',
                        mock.AsyncMock(return_value=inexact))
    record = mock.Mock()
    monkeypatch.setattr(lb, '_record_prediction_time', record)

    with pytest.raises(fastapi.HTTPException) as error:
        _run(
            lb._record_exact_async_prediction_payload(
                _exact_completion_payload(attempt_id)))

    assert error.value.status_code == 503
    record.assert_not_called()


def test_exact_completion_duplicate_does_not_double_count(monkeypatch):
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    _install_exact_completion_lookup(monkeypatch, lb, attempt_id)
    monkeypatch.setattr(
        lb, '_post_async_ledger',
        mock.AsyncMock(
            return_value=_exact_completion_receipt(attempt_id, duplicate=True)))
    record = mock.Mock()
    monkeypatch.setattr(lb, '_record_prediction_time', record)

    response = _run(
        lb._record_exact_async_prediction_payload(
            _exact_completion_payload(attempt_id)))

    assert response.status_code == 204
    record.assert_not_called()


@pytest.mark.parametrize('status_code', [409, 503])
def test_exact_completion_preserves_ledger_failure_status(
        monkeypatch, status_code):
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    _install_exact_completion_lookup(monkeypatch, lb, attempt_id)
    monkeypatch.setattr(
        lb, '_post_async_ledger',
        mock.AsyncMock(
            side_effect=(async_request_ledger_client.AsyncLedgerTransportError(
                status_code, 'ledger failure'))))
    record = mock.Mock()
    monkeypatch.setattr(lb, '_record_prediction_time', record)

    with pytest.raises(fastapi.HTTPException) as error:
        _run(
            lb._record_exact_async_prediction_payload(
                _exact_completion_payload(attempt_id)))

    assert error.value.status_code == status_code
    assert error.value.detail == 'ledger failure'
    if status_code == 503:
        assert error.value.headers == {
            'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)
        }
    else:
        assert error.value.headers is None
    record.assert_not_called()


def test_exact_completion_rejects_noncanonical_canceled_status(monkeypatch):
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    _install_exact_completion_lookup(monkeypatch, lb, attempt_id)
    payload = _exact_completion_payload(attempt_id)
    payload['status'] = 'CANCELED'
    post = mock.AsyncMock()
    monkeypatch.setattr(lb, '_post_async_ledger', post)
    record = mock.Mock()
    monkeypatch.setattr(lb, '_record_prediction_time', record)

    with pytest.raises(fastapi.HTTPException) as error:
        _run(lb._record_exact_async_prediction_payload(payload))

    assert error.value.status_code == 422
    post.assert_not_awaited()
    record.assert_not_called()


def test_exact_completion_resolves_lb_accepted_revision_handoff(monkeypatch):
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    attempt_id = '11111111-1111-4111-8111-111111111111'
    lookup = _install_exact_completion_lookup(monkeypatch,
                                              lb,
                                              attempt_id,
                                              revision=2)
    payload = _exact_completion_payload(attempt_id)
    payload['expected_revision'] = 1
    post = mock.AsyncMock(return_value=dataclasses.replace(
        _exact_completion_receipt(attempt_id), revision=3))
    monkeypatch.setattr(lb, '_post_async_ledger', post)

    response = _run(lb._record_exact_async_prediction_payload(payload))

    assert response.status_code == 204
    lookup.assert_awaited_once_with('job-exact-1', 'a' * 64)
    assert post.await_args.args[0]['expected_revision'] == 2


@pytest.mark.parametrize('body', [
    b'{"ledger_protocol_version":true,"request_id":"job-exact-1",'
    b'"intent_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    b'aaaaaaaaaaaaaaaa","attempt_id":"11111111-1111-4111-8111-111111111111",'
    b'"attempt_no":1,"expected_revision":4,"status":"SUCCEEDED",'
    b'"processing_time_us":5000000}',
    b'{"ledger_protocol_version":1.0,"request_id":"job-exact-1",'
    b'"intent_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    b'aaaaaaaaaaaaaaaa","attempt_id":"11111111-1111-4111-8111-111111111111",'
    b'"attempt_no":1,"expected_revision":4,"status":"SUCCEEDED",'
    b'"processing_time_us":5000000}',
    b'{"ledger_protocol_version":1,"ledger_protocol_version":1,'
    b'"request_id":"job-exact-1","intent_sha256":"aaaaaaaaaaaaaaaaaaaaaaaa'
    b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"attempt_id":"11111111-1111-4111-8111-111111111111","attempt_no":1,'
    b'"expected_revision":4,"status":"SUCCEEDED",'
    b'"processing_time_us":5000000}',
])
def test_exact_completion_rejects_inexact_json_before_ledger(monkeypatch, body):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    lb._async_request_ledger_protocol_version = 1
    lookup = mock.AsyncMock()
    monkeypatch.setattr(lb, '_lookup_async_ledger_receipt', lookup)
    client = _client_with_routes(lb)

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers={
                               **_edge_auth('s3cret'),
                               'Content-Type': 'application/json',
                           },
                           content=body)

    assert response.status_code == 422
    lookup.assert_not_awaited()


@pytest.mark.parametrize('payload', [{}, {
    'request_id': 'job-1',
    'status': 'IN_PROGRESS',
    'processing_time_ms': 1,
}, {
    'request_id': 'job-1',
    'status': 'SUCCEEDED',
    'processing_time_ms': -1,
}, {
    'request_id': 'job-1',
    'status': 'SUCCEEDED',
    'processing_time_ms': '1',
}])
def test_prediction_completion_callback_rejects_invalid_payload(
        monkeypatch, payload):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers=_edge_auth('s3cret'),
                           json=payload)

    assert response.status_code == 422


def test_prediction_completion_callback_rejects_oversized_body(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())
    body = b'x' * (constants.LB_PREDICTION_COMPLETION_BODY_MAX_BYTES + 1)

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers={
                               **_edge_auth('s3cret'),
                               'Content-Type': 'application/json',
                           },
                           content=body)

    assert response.status_code == 413


@pytest.mark.parametrize('headers', [{
    'Content-Type': 'text/application/jsonish',
}, {
    'Content-Type': 'application/json',
    'Content-Encoding': 'gzip',
}])
def test_prediction_completion_callback_rejects_unsupported_encoding(
        monkeypatch, headers):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers={
                               **_edge_auth('s3cret'),
                               **headers,
                           },
                           content=b'{}')

    assert response.status_code == 415


def test_prediction_completion_callback_rejects_overlong_request_id(
        monkeypatch):
    """The dedup map must stay byte-bounded, not just entry-bounded.

    ``LB_ASYNC_PREDICTION_DEDUP_CAP`` bounds how many ids are retained, so an
    unbounded id length lets one authenticated caller choose the retained size.
    """
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    client = _client_with_routes(lb)
    overlong = 'x' * (constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS + 1)

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers=_edge_auth('s3cret'),
                           json={
                               'request_id': overlong,
                               'status': 'SUCCEEDED',
                               'processing_time_ms': 5000,
                           })

    assert response.status_code == 422
    assert overlong not in lb._completed_async_prediction_ids


def test_completed_prediction_ids_stay_byte_bounded(monkeypatch):
    """Worst-case retention must fit the load balancer memory limit."""
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    client = _client_with_routes(lb)
    at_cap = 'y' * constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers=_edge_auth('s3cret'),
                           json={
                               'request_id': at_cap,
                               'status': 'SUCCEEDED',
                               'processing_time_ms': 5000,
                           })

    assert response.status_code == 204
    assert all(
        len(key) <= constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS
        for key in lb._completed_async_prediction_ids)
    worst_case_bytes = (constants.LB_ASYNC_PREDICTION_DEDUP_CAP *
                        constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS)
    assert worst_case_bytes <= 64 * 1024 * 1024


def test_prediction_completion_callback_rejects_deeply_nested_body(monkeypatch):
    """A malformed under-cap body is a client error, not a server error."""
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())
    body = b'[' * 4000

    response = client.post(constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
                           headers={
                               **_edge_auth('s3cret'),
                               'Content-Type': 'application/json',
                           },
                           content=body)

    assert len(body) <= constants.LB_PREDICTION_COMPLETION_BODY_MAX_BYTES
    assert response.status_code == 422


def test_async_status_path_also_bounds_request_id():
    """The proxy-side observation path shares the same retention bound."""
    lb = _make_lb()
    overlong = 'z' * (constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS + 1)

    recorded = lb._record_async_prediction_status(
        json.dumps({
            'request_id': overlong,
            'status': 'SUCCEEDED',
            'processing_time_ms': 5000,
        }).encode('utf-8'), '')

    assert not recorded
    assert overlong not in lb._completed_async_prediction_ids


def test_async_status_path_survives_deeply_nested_replica_body():
    """A replica body that cannot be parsed must not escape the proxy path."""
    lb = _make_lb()

    assert not lb._record_async_prediction_status(b'[' * 4000, '')


def test_stack_consumes_edge_auth_but_preserves_replica_auth(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 'edge-secret')
    lb = _make_lb()
    lb._app.add_middleware(load_balancer._InboundAuthMiddleware)

    async def _headers(request: fastapi.Request):
        return {
            'edge': request.headers.get(constants.LB_AUTHORIZATION_HEADER),
            'replica': request.headers.get('Authorization'),
        }

    lb._app.add_api_route('/predict', _headers, methods=['GET'])
    client = TestClient(lb._app)
    response = client.get('/predict',
                          headers={
                              **_edge_auth('edge-secret'),
                              'Authorization': 'Bearer model-secret',
                          })
    assert response.status_code == 200
    assert response.json() == {
        'edge': None,
        'replica': 'Bearer model-secret',
    }


def test_stack_non_get_health_path_still_requires_auth(monkeypatch):
    # Regression: the exemption must be method-scoped. POST/PUT/DELETE on the
    # health path are not handled by the GET-only health route and fall through
    # to the catch-all proxy, so they MUST require auth -- else they reach a
    # replica unauthenticated.
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())
    for method in ('post', 'put', 'delete'):
        resp = getattr(client, method)(constants.LB_HEALTH_ENDPOINT_PATH)
        assert resp.status_code == 401, method


def test_external_stack_missing_auth_fails_closed_but_health_is_exempt(
        monkeypatch):
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    monkeypatch.setenv(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR, 'true')
    client = _client_with_routes(_make_lb())

    assert client.get('/predict').status_code == 503
    # The probe still reaches the real health handler. A cold LB is 503 for
    # readiness, but not because auth rejected it (and it becomes 200 synced).
    lb = _make_lb()
    lb._ready = True
    client = _client_with_routes(lb)
    assert client.get(constants.LB_HEALTH_ENDPOINT_PATH).status_code == 200


def test_stack_streaming_response_passes_through(monkeypatch):
    # The pure-ASGI middleware must NOT buffer/break streaming responses (the
    # reason it is raw ASGI rather than BaseHTTPMiddleware): a chunked/SSE
    # inference body streams through untouched once authenticated, and an
    # unauthenticated request is rejected before the stream starts.
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    lb = _make_lb()
    lb._app.add_middleware(load_balancer._InboundAuthMiddleware)

    async def _stream(request: fastapi.Request):
        del request

        async def _gen():
            for chunk in (b'a', b'b', b'c'):
                yield chunk

        return fastapi.responses.StreamingResponse(_gen())

    lb._app.add_api_route('/stream', _stream, methods=['GET'])
    client = TestClient(lb._app)
    ok = client.get('/stream', headers=_edge_auth('s3cret'))
    assert ok.status_code == 200 and ok.content == b'abc'
    assert client.get('/stream').status_code == 401


def test_request_aggregator_is_bounded():
    # Regression: retaining the batch across a failed sync must not grow without
    # bound. The aggregator keeps at most LB_REQUEST_TIMESTAMP_CAP samples.
    agg = serve_utils.RequestTimestamp()
    for _ in range(constants.LB_REQUEST_TIMESTAMP_CAP + 250):
        agg.add(None)
    assert len(
        agg.to_dict()['timestamps']) == constants.LB_REQUEST_TIMESTAMP_CAP


def test_request_history_uses_cumulative_minute_counters(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    agg = serve_utils.RequestTimestamp()

    agg.add(None)
    agg.add(None)
    first = agg.request_history_snapshot()
    assert first == {
        'bucket_seconds': 60,
        'buckets': [{
            'bucket_start': 120,
            'request_count': 2,
            'rejected_count': 0,
        }],
    }

    agg.mark_request_history_accepted(first)
    assert agg.request_history_snapshot() is None
    agg.add(None)
    assert agg.request_history_snapshot()['buckets'] == [{
        'bucket_start': 120,
        'request_count': 3,
        'rejected_count': 0,
    }]


def test_request_history_ack_preserves_arrivals_during_sync(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    agg = serve_utils.RequestTimestamp()
    agg.add(None)
    snapshot = agg.request_history_snapshot()

    agg.add(None)
    agg.mark_request_history_accepted(snapshot)

    assert agg.request_history_snapshot()['buckets'] == [{
        'bucket_start': 120,
        'request_count': 2,
        'rejected_count': 0,
    }]


def test_rejection_history_is_acknowledged_independently(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    agg = serve_utils.RequestTimestamp()
    agg.add(None)
    first = agg.request_history_snapshot()
    agg.mark_request_history_accepted(first)

    agg.add_rejection()
    rejected = agg.request_history_snapshot()
    assert rejected == {
        'bucket_seconds': 60,
        'buckets': [{
            'bucket_start': 120,
            'request_count': 1,
            'rejected_count': 1,
        }],
    }

    agg.add_rejection()
    agg.mark_request_history_accepted(rejected)
    assert agg.request_history_snapshot()['buckets'] == [{
        'bucket_start': 120,
        'request_count': 1,
        'rejected_count': 2,
    }]


def test_prediction_time_history_uses_fixed_outcome_histograms(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    agg = serve_utils.RequestTimestamp()

    agg.add_prediction_time(0.1, 'succeeded')
    agg.add_prediction_time(0.11, 'succeeded')
    agg.add_prediction_time(30.0, 'failed')
    agg.add_prediction_time(3601.0, 'failed')

    snapshot = agg.prediction_time_history_snapshot()
    assert snapshot['bucket_seconds'] == 60
    assert snapshot['histogram_version'] == 1
    bucket = snapshot['buckets'][0]
    assert bucket['bucket_start'] == 120
    assert bucket['outcome_counts']['succeeded'][:2] == [1, 1]
    assert sum(bucket['outcome_counts']['succeeded']) == 2
    assert bucket['outcome_counts']['failed'][7] == 1
    assert bucket['outcome_counts']['failed'][-1] == 1

    agg.mark_prediction_time_history_accepted(snapshot)
    assert agg.prediction_time_history_snapshot() is None


def test_prediction_time_ack_preserves_completion_during_sync(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    agg = serve_utils.RequestTimestamp()
    agg.add_prediction_time(1.0, 'succeeded')
    snapshot = agg.prediction_time_history_snapshot()

    agg.add_prediction_time(2.0, 'succeeded')
    agg.mark_prediction_time_history_accepted(snapshot)

    pending = agg.prediction_time_history_snapshot()['buckets'][0]
    assert sum(pending['outcome_counts']['succeeded']) == 2


def test_async_prediction_status_uses_reported_time_and_deduplicates(
        monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    body = (b'{"request_id":"job-1","status":"SUCCEEDED",'
            b'"processing_time_ms":5000}')

    lb._record_async_prediction_status(body, '')
    lb._record_async_prediction_status(body, '')
    lb._record_async_prediction_status(
        b'{"request_id":"job-2","status":"FAILED",'
        b'"processing_time_ms":30000}', '')
    lb._record_async_prediction_status(
        b'{"request_id":"job-3","status":"IN_PROGRESS",'
        b'"processing_time_ms":1}', '')

    counts = lb._request_aggregator.prediction_time_history_snapshot(
    )['buckets'][0]['outcome_counts']
    assert counts['succeeded'][5] == 1
    assert counts['failed'][7] == 1
    assert sum(counts['succeeded']) + sum(counts['failed']) == 2


def test_request_action_skips_oversized_json_without_parsing(monkeypatch):
    lb = _make_lb()
    body = b'x' * (constants.LB_ASYNC_ACTION_BODY_MAX_BYTES + 1)
    delivered = False

    async def _receive():
        nonlocal delivered
        if delivered:
            return {'type': 'http.disconnect'}
        delivered = True
        return {
            'type': 'http.request',
            'body': body,
            'more_body': False,
        }

    request = fastapi.Request(
        {
            'type': 'http',
            'method': 'POST',
            'scheme': 'http',
            'server': ('load-balancer', 80),
            'path': '/predict',
            'raw_path': b'/predict',
            'query_string': b'',
            'headers': [
                (b'content-type', b'application/json'),
                (b'content-length', str(len(body)).encode()),
            ],
        }, _receive)
    monkeypatch.setattr(load_balancer.json, 'loads',
                        lambda _: pytest.fail('oversized body was parsed'))

    assert _run(lb._request_action(request)) is None
    assert _run(request.body()) == body


def test_proxy_records_sync_and_terminal_async_but_not_async_ack(monkeypatch):

    async def _run_test():
        monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
        lb = _make_lb()
        url = 'http://worker:8000'
        responses = [
            (200, {
                'result': 'ok'
            }),
            (202, {
                'request_id': 'job-1',
                'status': 'QUEUED',
            }),
            (202, {
                'request_id': 'job-header',
                'status': 'QUEUED',
            }),
            (200, {
                'request_id': 'job-1',
                'status': 'SUCCEEDED',
                'processing_time_ms': 5000,
            }),
            (200, {
                'request_id': 'job-2',
                'status': 'SUCCEEDED',
                'processing_time_ms': 1,
                'padding': 'x' * constants.LB_ASYNC_STATUS_BODY_MAX_BYTES,
            }),
            (400, {
                'request_id': 'job-3',
                'status': 'FAILED',
                'processing_time_ms': 1,
            }),
            (500, {
                'error': 'prediction failed',
            }),
            (503, {
                'error': 'warming',
            }),
            (200, {
                'request_id': 'job-gzip',
                'status': 'SUCCEEDED',
                'processing_time_ms': 1,
            }, {
                'content-encoding': 'gzip',
            }),
        ]

        async def _handler(request):
            del request
            response_spec = responses.pop(0)
            status_code, payload = response_spec[:2]
            headers = {'content-type': 'application/json'}
            if len(response_spec) == 3:
                headers.update(response_spec[2])
            return httpx.Response(status_code,
                                  headers=headers,
                                  stream=httpx.ByteStream(
                                      json.dumps(payload).encode('utf-8')))

        client = httpx.AsyncClient(transport=httpx.MockTransport(_handler),
                                   base_url=url)
        lb._client_pool[url] = client
        lb._load_balancing_policy.set_ready_replicas([url])

        def _request(body, *, job_id=None):
            delivered = False

            async def _receive():
                nonlocal delivered
                if delivered:
                    return {'type': 'http.disconnect'}
                delivered = True
                return {
                    'type': 'http.request',
                    'body': body,
                    'more_body': False,
                }

            headers = [(b'content-type', b'application/json')]
            if job_id is not None:
                headers.append((constants.LB_JOB_ID_HEADER.lower().encode(),
                                job_id.encode()))
            return fastapi.Request(
                {
                    'type': 'http',
                    'method': 'POST',
                    'scheme': 'http',
                    'server': ('load-balancer', 80),
                    'path': '/predict',
                    'raw_path': b'/predict',
                    'query_string': b'',
                    'headers': headers,
                }, _receive)

        sync_response = await lb._proxy_request_to(url, _request(b'{"x":1}'))
        assert not isinstance(sync_response, Exception)
        async for _ in sync_response.body_iterator:
            pass

        async_ack = await lb._proxy_request_to(
            url, _request(b'{"action":"async_predict","request_id":"job-1"}'))
        assert not isinstance(async_ack, Exception)
        async for _ in async_ack.body_iterator:
            pass
        header_async_ack = await lb._proxy_request_to(
            url, _request(b'{"request_id":"job-header"}', job_id='job-header'))
        assert not isinstance(header_async_ack, Exception)
        async for _ in header_async_ack.body_iterator:
            pass
        async_status = await lb._proxy_request_to(
            url, _request(b'{"action":"async_status","request_id":"job-1"}'))
        assert not isinstance(async_status, Exception)
        async for _ in async_status.body_iterator:
            pass
        oversized_status = await lb._proxy_request_to(
            url, _request(b'{"action":"async_status","request_id":"job-2"}'))
        assert not isinstance(oversized_status, Exception)
        forwarded_body = bytearray()
        async for chunk in oversized_status.body_iterator:
            forwarded_body.extend(chunk)
        assert len(forwarded_body) > constants.LB_ASYNC_STATUS_BODY_MAX_BYTES
        rejected_status = await lb._proxy_request_to(
            url, _request(b'{"action":"async_status","request_id":"job-3"}'))
        assert not isinstance(rejected_status, Exception)
        assert rejected_status.status_code == 400
        async for _ in rejected_status.body_iterator:
            pass
        failed_sync = await lb._proxy_request_to(url, _request(b'{"x":2}'))
        assert not isinstance(failed_sync, Exception)
        assert failed_sync.status_code == 500
        async for _ in failed_sync.body_iterator:
            pass
        lb._retriable_status_codes = frozenset({503})
        retriable_sync = await lb._proxy_request_to(url, _request(b'{"x":3}'))
        assert isinstance(retriable_sync, load_balancer._RetriableStatusError)
        lb._retriable_status_codes = frozenset()
        compressed_status = await lb._proxy_request_to(
            url,
            _request(b'{"action":"async_status",'
                     b'"request_id":"job-gzip"}'))
        assert not isinstance(compressed_status, Exception)
        async for _ in compressed_status.body_iterator:
            pass
        await client.aclose()

        snapshot = lb._request_aggregator.prediction_time_history_snapshot()
        counts = snapshot['buckets'][0]['outcome_counts']
        assert sum(counts['succeeded']) == 2
        assert counts['succeeded'][5] == 1
        assert sum(counts['failed']) == 1

    asyncio.run(_run_test())


def test_terminal_rejection_feeds_exact_history(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    request = fastapi.Request(_scope('/predict'))
    lb._request_aggregator.add(request)

    lb._record_rejection(request)

    assert lb._request_aggregator.request_history_snapshot()['buckets'] == [{
        'bucket_start': 120,
        'request_count': 1,
        'rejected_count': 1,
    }]


def test_terminal_classification_requires_eligibility_and_is_exact_once(
        monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    pre_admission = fastapi.Request(_scope('/predict'))
    eligible = fastapi.Request(_scope('/predict'))

    # Body-budget and drain/role rejection paths never open the eligibility
    # fence, so their legacy rejection telemetry cannot enter the denominator.
    lb._record_rejection(pre_admission)
    assert not lb._request_aggregator.request_classification_history_snapshot(
    )['buckets']

    lb._mark_request_classification_eligible(eligible)
    lb._record_rejection(eligible)
    lb._record_rejection(eligible)
    assert lb._request_aggregator.request_classification_history_snapshot(
    )['buckets'] == [{
        'bucket_start': 120,
        'classified_request_count': 1,
        'counted_rejected_count': 1,
    }]


@pytest.mark.parametrize('terminal', ['response', 'error', 'cancellation'])
def test_admitted_request_final_guard_classifies_non_rejected_once(
        monkeypatch, terminal):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    request = fastapi.Request(_scope('/predict'))

    if terminal == 'response':
        lb._proxy_with_retries_inner = mock.AsyncMock(
            return_value=fastapi.responses.Response(status_code=500))
    elif terminal == 'error':
        lb._proxy_with_retries_inner = mock.AsyncMock(
            side_effect=fastapi.HTTPException(status_code=502))
    else:
        lb._proxy_with_retries_inner = mock.AsyncMock(
            side_effect=asyncio.CancelledError())

    async def _run_terminal():
        if terminal == 'response':
            response = await lb._proxy_with_retries(request)
            assert response.status_code == 500
        else:
            expected = (asyncio.CancelledError if terminal == 'cancellation'
                        else fastapi.HTTPException)
            with pytest.raises(expected):
                await lb._proxy_with_retries(request)

    asyncio.run(_run_terminal())

    assert lb._request_aggregator.request_classification_history_snapshot(
    )['buckets'] == [{
        'bucket_start': 120,
        'classified_request_count': 1,
        'counted_rejected_count': 0,
    }]


def test_request_history_is_bounded_to_recent_hour(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    agg = serve_utils.RequestTimestamp()
    for minute in range(constants.LB_REQUEST_HISTORY_MAX_BUCKETS + 5):
        now[0] = float(minute * 60)
        agg.add(None)

    snapshot = agg.request_history_snapshot()

    assert len(snapshot['buckets']) == constants.LB_REQUEST_HISTORY_MAX_BUCKETS
    assert snapshot['buckets'][0]['bucket_start'] == 5 * 60


def test_request_history_pruning_is_minute_boundary_work(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    agg = serve_utils.RequestTimestamp()
    prune = mock.Mock(wraps=agg._prune_request_history)  # pylint: disable=protected-access
    monkeypatch.setattr(agg, '_prune_request_history', prune)

    for _ in range(100):
        agg.add(None)
    assert prune.call_count == 1

    now[0] += constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
    agg.add(None)
    agg.add(None)
    assert prune.call_count == 2


def test_aggregator_drained_on_success_and_restored_on_failure(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)

    # Failed sync (401): the batch must be retained for the next report.
    lb = _make_lb()
    agg = serve_utils.RequestTimestamp()
    agg.timestamps.extend([1, 2, 3])
    lb._request_aggregator = agg
    _sync_once(monkeypatch, lb, 401, {})
    assert agg.to_dict()['timestamps'] == [1, 2, 3]

    # Successful sync (200): only the delivered batch is acknowledged.
    lb2 = _make_lb()
    agg2 = serve_utils.RequestTimestamp()
    agg2.timestamps.extend([1, 2, 3])
    lb2._request_aggregator = agg2
    _sync_once(monkeypatch, lb2, 200, {})
    assert agg2.to_dict()['timestamps'] == []


def test_request_history_requires_independent_controller_ack(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    lb = _make_lb()
    lb._request_aggregator.add(None)
    captured = {}

    _sync_once(monkeypatch, lb, 200, captured)

    assert captured['json']['request_history']['buckets'][0][
        'request_count'] == 1
    assert lb._request_aggregator.request_history_snapshot() is not None

    captured = {
        'response_json': {
            'replica_info': {},
            'routing_spec': None,
            'request_history_accepted': True,
        }
    }
    _sync_once(monkeypatch, lb, 200, captured)
    assert lb._request_aggregator.request_history_snapshot() is None


def test_prediction_time_history_requires_new_controller_ack(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    lb._request_aggregator.add_prediction_time(1.0, 'succeeded')
    captured = {}

    _sync_once(monkeypatch, lb, 200, captured)

    assert captured['json']['prediction_time_history'] is not None
    assert lb._request_aggregator.prediction_time_history_snapshot() is not None

    captured = {
        'response_json': {
            'replica_info': {},
            'routing_spec': None,
            'prediction_time_history_accepted': True,
        }
    }
    _sync_once(monkeypatch, lb, 200, captured)
    assert lb._request_aggregator.prediction_time_history_snapshot() is None


def test_classification_history_requires_independent_controller_ack(
        monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    request = fastapi.Request(_scope('/predict'))
    lb._mark_request_classification_eligible(request)
    lb._record_request_classification_once(request, rejected=True)
    captured = {
        'response_json': {
            'replica_info': {},
            'routing_spec': None,
            # A legacy request-history acknowledgement must not clear the new
            # independently durable terminal counters.
            'request_history_accepted': True,
        }
    }

    _sync_once(monkeypatch, lb, 200, captured)

    classification = captured['json']['request_classification_history']
    assert classification['classification_version'] == 1
    assert classification['buckets'][0]['classified_request_count'] == 1
    assert lb._request_aggregator.request_classification_history_snapshot(
    )['buckets']

    captured = {
        'response_json': {
            'replica_info': {},
            'routing_spec': None,
            'request_classification_history_accepted': True,
        }
    }
    _sync_once(monkeypatch, lb, 200, captured)
    assert not lb._request_aggregator.request_classification_history_snapshot(
    )['buckets']


def test_request_history_ack_does_not_erase_arrival_during_sync(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    lb = _make_lb()
    lb._request_aggregator.add(None)
    captured = {
        'response_json': {
            'replica_info': {},
            'routing_spec': None,
            'request_history_accepted': True,
        },
        'on_response_enter': lambda: lb._request_aggregator.add(None),
    }

    _sync_once(monkeypatch, lb, 200, captured)

    assert lb._request_aggregator.request_history_snapshot()['buckets'] == [{
        'bucket_start': 120,
        'request_count': 2,
        'rejected_count': 0,
    }]


def test_drain_flush_uses_history_only_endpoint_and_acknowledges(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    lb = _make_lb()
    lb._request_aggregator.add(None)
    captured = {'response_json': {'request_history_accepted': True}}
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession',
                        lambda *a, **k: _FakeSession(200, captured))

    _run(lb._flush_request_history_on_drain())

    assert captured['url'].endswith(
        '/controller/load_balancer_request_history_sync')
    assert set(captured['json']) == {
        'request_history',
        'request_classification_history',
        'prediction_time_history',
        'request_history_session_id',
        'lb_session_id',
    }
    assert captured['json']['request_history']['buckets'] == [{
        'bucket_start': 120,
        'request_count': 1,
        'rejected_count': 0,
    }]
    assert (captured['timeout'].total ==
            constants.LB_DRAIN_HISTORY_FLUSH_TIMEOUT_SECONDS)
    assert lb._request_aggregator.request_history_snapshot() is None


def test_failed_drain_flush_is_bounded_and_retains_history(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    lb = _make_lb()
    lb._request_aggregator.add(None)
    captured = {}
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession',
                        lambda *a, **k: _FakeSession(500, captured))

    _run(lb._flush_request_history_on_drain())

    assert lb._request_aggregator.request_history_snapshot() is not None


def test_drain_flush_acknowledges_classification_independently(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    lb = _make_lb()
    lb._request_aggregator.add_request_classification(rejected=True)
    captured = {
        'response_json': {
            # The legacy ack deliberately proves it cannot clear classification.
            'request_history_accepted': True,
            'request_classification_history_accepted': True,
        }
    }
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession',
                        lambda *a, **k: _FakeSession(200, captured))

    _run(lb._flush_request_history_on_drain())

    assert captured['json']['request_history'] is None
    assert captured['json']['request_classification_history']['buckets'] == [{
        'bucket_start': 120,
        'classified_request_count': 1,
        'counted_rejected_count': 1,
    }]
    assert not lb._request_aggregator.request_classification_history_snapshot(
    )['buckets']


def test_aggregator_keeps_arrivals_during_successful_sync(monkeypatch):
    """A request arriving after drain but before the 2xx belongs to the
    next batch; acknowledging the sent batch must not erase it."""
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    lb = _make_lb()
    lb._request_aggregator.timestamps.append(1)
    captured = {
        'on_response_enter': lambda: lb._request_aggregator.timestamps.append(2)
    }

    _sync_once(monkeypatch, lb, 200, captured)

    assert captured['json']['request_aggregator']['timestamps'] == [1]
    assert lb._request_aggregator.to_dict()['timestamps'] == [2]


def test_aggregator_restores_failed_batch_before_new_arrivals(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    lb = _make_lb()
    lb._request_aggregator.timestamps.append(1)
    captured = {
        'on_response_enter': lambda: lb._request_aggregator.timestamps.append(2)
    }

    _sync_once(monkeypatch, lb, 401, captured)

    assert captured['json']['request_aggregator']['timestamps'] == [1]
    assert lb._request_aggregator.to_dict()['timestamps'] == [1, 2]


def test_aggregator_restore_cap_keeps_newest_samples():
    agg = serve_utils.RequestTimestamp()
    cap = constants.LB_REQUEST_TIMESTAMP_CAP
    agg.timestamps.extend(range(cap))
    drained = agg.drain()
    agg.timestamps.append(cap)  # New arrival while the old batch is in flight.

    agg.restore(drained)

    restored = agg.to_dict()['timestamps']
    assert len(restored) == cap
    assert restored[0] == 1
    assert restored[-1] == cap
