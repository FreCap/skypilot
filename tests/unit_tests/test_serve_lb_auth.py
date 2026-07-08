"""Tests for external-LB auth: inbound data-plane bearer + control-plane sync.

Two distinct tokens:
  - LB_AUTH_TOKEN_ENV_VAR gates INBOUND inference requests (data plane). The
    readiness route is exempt so the k8s probe still works; no-op when unset.
  - CONTROLLER_AUTH_TOKEN_ENV_VAR is presented by the LB on every sync so the
    (now-authenticated) controller accepts it. The request aggregator must be
    cleared only after a SUCCESSFUL sync -- a failed sync (e.g. 401) must not
    drop the load signal the controller never received.

Logic-only: no assertions on log or exception message text.
"""
# pylint: disable=invalid-name,protected-access
import asyncio
from unittest import mock

import aiohttp
import fastapi
from fastapi.testclient import TestClient
import pytest

from sky.serve import constants
from sky.serve import load_balancer
from sky.serve import serve_utils


def _make_lb() -> load_balancer.SkyServeLoadBalancer:
    return load_balancer.SkyServeLoadBalancer(controller_url='http://ctrl:8001',
                                              load_balancer_port=8890)


def _run(coro):
    return asyncio.run(coro)


def _scope(path, method='GET', headers=None):
    # ASGI http scope: headers are (name, value) latin-1 byte tuples.
    hdrs = [(k.encode('latin-1'), v.encode('latin-1'))
            for k, v in (headers or {}).items()]
    return {'type': 'http', 'method': method, 'path': path, 'headers': hdrs}


def _authorized(scope) -> bool:
    return load_balancer._InboundAuthMiddleware._authorized(scope)


# --------------------------------------------------------------------------- #
# Inbound data-plane bearer middleware (pure-ASGI _authorized decision)
# --------------------------------------------------------------------------- #
def test_inbound_auth_disabled_authorizes_all(monkeypatch):
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    assert _authorized(_scope('/predict'))


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
    assert _authorized(
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
    headers = {} if bad is None else {'authorization': bad}
    assert not _authorized(_scope('/predict', headers=headers))


def test_get_lb_auth_token_reads_env(monkeypatch):
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    assert serve_utils.get_lb_auth_token() is None
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 'tok')
    assert serve_utils.get_lb_auth_token() == 'tok'
    # Empty string is treated as unset (auth disabled).
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, '')
    assert serve_utils.get_lb_auth_token() is None


def test_inbound_and_control_plane_tokens_are_independent(monkeypatch):
    # Setting the control-plane token must NOT enable inbound auth, and vice
    # versa -- an inference client's token must never reach the controller.
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, 'ctrl')
    monkeypatch.delenv(constants.LB_AUTH_TOKEN_ENV_VAR, raising=False)
    assert serve_utils.get_lb_auth_token() is None
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

    async def json(self):
        return {'replica_info': {}, 'routing_spec': None}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:

    def __init__(self, status, captured):
        self._status = status
        self._captured = captured

    def post(self, *args, **kwargs):
        self._captured['headers'] = kwargs.get('headers')
        return _FakeResp(self._status, self._captured)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _sync_once(monkeypatch, lb, status, captured):
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession',
                        lambda *a, **k: _FakeSession(status, captured))
    _run(lb._sync_with_controller_once())


def test_sync_sends_control_plane_bearer(monkeypatch):
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, 'ctrl-tok')
    lb = _make_lb()
    captured = {}
    _sync_once(monkeypatch, lb, 200, captured)
    assert captured['headers'] == {'Authorization': 'Bearer ctrl-tok'}


def test_sync_no_token_sends_no_header(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    lb = _make_lb()
    captured = {}
    _sync_once(monkeypatch, lb, 200, captured)
    assert captured['headers'] is None


def _client_with_routes(lb) -> TestClient:
    """Register middleware + routes exactly like SkyServeLoadBalancer.run(), with
    a stub catch-all proxy, so tests exercise the REAL FastAPI stack (middleware
    + route matching), not just the middleware method in isolation."""
    lb._app.add_middleware(load_balancer._InboundAuthMiddleware)
    lb._app.add_api_route(constants.LB_HEALTH_ENDPOINT_PATH,
                          lb._health,
                          methods=['GET'])

    async def _proxy(request: fastapi.Request):
        del request
        return fastapi.responses.PlainTextResponse('PROXY')

    lb._app.add_api_route('/{path:path}',
                          _proxy,
                          methods=['GET', 'POST', 'PUT', 'DELETE'])
    return TestClient(lb._app)


def test_stack_health_get_exempt_but_proxy_requires_auth(monkeypatch):
    monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, 's3cret')
    client = _client_with_routes(_make_lb())
    # GET /_lb/health is the readiness probe -> exempt (reaches health handler,
    # which returns 503 until first sync; the point is it is NOT 401).
    assert client.get(constants.LB_HEALTH_ENDPOINT_PATH).status_code != 401
    # Inference path requires auth.
    assert client.get('/predict').status_code == 401
    assert client.get('/predict', headers={
        'Authorization': 'Bearer s3cret'
    }).status_code == 200


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
    ok = client.get('/stream', headers={'Authorization': 'Bearer s3cret'})
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


def test_aggregator_cleared_only_on_success(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)

    # Failed sync (401): the batch must be retained for the next report.
    lb = _make_lb()
    agg = mock.MagicMock()
    agg.to_dict.return_value = {'timestamps': [1, 2, 3]}
    lb._request_aggregator = agg
    _sync_once(monkeypatch, lb, 401, {})
    agg.clear.assert_not_called()

    # Successful sync (200): the batch is cleared to avoid unbounded growth.
    lb2 = _make_lb()
    agg2 = mock.MagicMock()
    agg2.to_dict.return_value = {'timestamps': [1, 2, 3]}
    lb2._request_aggregator = agg2
    _sync_once(monkeypatch, lb2, 200, {})
    agg2.clear.assert_called_once()
