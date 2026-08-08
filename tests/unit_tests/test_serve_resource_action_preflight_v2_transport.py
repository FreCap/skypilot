"""Disjoint V1/V2 private authority-preflight transport tests."""
# pylint: disable=protected-access,redefined-outer-name

import concurrent.futures
import json
import pathlib
import socket
import ssl
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest
import test_serve_resource_action_preflight_transport as v1_transport
import test_serve_resource_action_preflight_v2 as v2_fixtures

from sky.serve import constants
from sky.serve import resource_action_preflight_client as client
from sky.serve import resource_action_preflight_v2 as preflight_v2
from sky.serve import resource_actions
from sky.server import authority_preflight
from sky.server import runtime
from sky.server.requests import authority_worker_bootstrap as bootstrap_v1

_SERVICE_DNS = 'skypilot-authority-preflight.skypilot-system.svc'
_TOKEN = 'preflight_v2_private_token_1234567890'


def _wire(body: bytes,
          path: str,
          port: int,
          *,
          declared_length: int | None = None,
          send_body: bool = True,
          method: str = 'POST',
          http_version: str = 'HTTP/1.1') -> bytes:
    length = len(body) if declared_length is None else declared_length
    headers = [
        f'{method} {path} {http_version}',
        f'Host: {_SERVICE_DNS}:{port}',
        f'Authorization: Bearer {_TOKEN}',
        'Content-Type: application/json',
        'Accept: application/json',
        'Accept-Encoding: identity',
        f'Content-Length: {length}',
        'Connection: close',
    ]
    result = ('\r\n'.join(headers) + '\r\n\r\n').encode('ascii')
    return result + (body if send_body else b'')


@pytest.fixture
def dual_server(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    tls_directory, ca_file = v1_transport._tls_tree(tmp_path)
    token_file = tmp_path / 'tokens'
    token_file.write_text(_TOKEN + '\n', encoding='ascii')
    monkeypatch.setenv(
        constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR,
        str(token_file))
    for env_name in (constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
                     constants.LB_AUTH_TOKEN_ENV_VAR):
        monkeypatch.delenv(env_name, raising=False)

    calls: list[tuple[int, str]] = []

    def evaluate_v1(request):
        calls.append((1, request.action_kind.value))
        return (resource_actions.ProviderLaunchAuthorityPreflightResponseV1.
                unavailable(request))

    def evaluate_v2(request):
        calls.append((2, request.action_kind.value))
        return (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2.
                unavailable(request))

    server = authority_preflight.AuthorityPreflightServer(
        '127.0.0.1',
        0,
        _SERVICE_DNS,
        evaluate_v1,
        on_transport_invalid=lambda: None,
        evaluator_v2=evaluate_v2,
        tls_directory=str(tls_directory))
    server.start()
    try:
        yield server, ca_file, calls
    finally:
        server.stop()


def test_server_routes_v1_and_v2_only_to_their_exact_typed_evaluator(
        dual_server) -> None:
    server, ca_file, calls = dual_server
    v1_request = v1_transport.preflight_fixtures._launch_request()
    v2_request = v2_fixtures._launch_request()

    v1_wire = _wire(v1_request.canonical_bytes,
                    constants.RESOURCE_ACTION_PREFLIGHT_PATH_V1,
                    server.bound_port)
    body = v1_transport._assert_response(
        v1_transport._exchange(server.bound_port, ca_file, v1_wire), 200)
    parsed_v1 = (
        resource_actions.provider_authority_preflight_response_from_value_v1(
            json.loads(body)))
    parsed_v1.validate_request(v1_request)

    v2_wire = _wire(v2_request.canonical_bytes,
                    constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2,
                    server.bound_port)
    body = v1_transport._assert_response(
        v1_transport._exchange(server.bound_port, ca_file, v2_wire), 200)
    parsed_v2 = (
        preflight_v2.provider_authority_preflight_response_from_value_v2(
            json.loads(body)))
    parsed_v2.validate_request(v2_request)
    assert calls == [(1, 'launch'), (2, 'launch')]


@pytest.mark.parametrize('body_factory,path,error_version', [
    (lambda: v2_fixtures._launch_request().canonical_bytes,
     constants.RESOURCE_ACTION_PREFLIGHT_PATH_V1, 1),
    (lambda: v1_transport.preflight_fixtures._launch_request().canonical_bytes,
     constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, 2),
])
def test_crossed_route_body_is_bad_request_before_evaluation(
        dual_server, body_factory, path: str, error_version: int) -> None:
    server, ca_file, calls = dual_server
    response = v1_transport._exchange(
        server.bound_port, ca_file,
        _wire(body_factory(), path, server.bound_port))
    body = v1_transport._assert_response(response, 400)
    assert body == resource_actions.canonical_json_bytes({
        'version': error_version,
        'code': 'bad_request',
    })
    assert calls == []


@pytest.mark.parametrize('method,path,http_version,status,code,error_version', [
    ('GET', constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, 'HTTP/1.1', 405,
     'method_not_allowed', 2),
    ('POST', constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, 'HTTP/1.0', 400,
     'bad_request', 2),
    ('GET', '/internal/resource-actions/v2/unknown', 'HTTP/1.1', 405,
     'method_not_allowed', 1),
    ('POST', '/internal/resource-actions/v2/unknown', 'HTTP/1.1', 404,
     'not_found', 1),
])
def test_request_line_errors_use_recognized_route_protocol_version(
        dual_server, method: str, path: str, http_version: str, status: int,
        code: str, error_version: int) -> None:
    server, ca_file, calls = dual_server
    response = v1_transport._exchange(
        server.bound_port, ca_file,
        _wire(v2_fixtures._launch_request().canonical_bytes,
              path,
              server.bound_port,
              method=method,
              http_version=http_version))
    body = v1_transport._assert_response(response, status)
    assert body == resource_actions.canonical_json_bytes({
        'version': error_version,
        'code': code,
    })
    assert calls == []


@pytest.mark.parametrize('mutate', [
    lambda body: body.replace(b'"version":2', b'"version":2.0', 1),
    lambda body: body.replace(b'{', b'{"version":2,', 1),
])
def test_v2_route_rejects_float_and_duplicate_json_before_evaluation(
        dual_server, mutate) -> None:
    server, ca_file, calls = dual_server
    malformed = mutate(v2_fixtures._launch_request().canonical_bytes)
    response = v1_transport._exchange(
        server.bound_port, ca_file,
        _wire(malformed, constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2,
              server.bound_port))
    body = v1_transport._assert_response(response, 400)
    assert body == resource_actions.canonical_json_bytes({
        'version': 2,
        'code': 'bad_request',
    })
    assert calls == []


def test_v2_route_rejects_declared_65537_byte_request_without_reading_body(
        dual_server) -> None:
    server, ca_file, calls = dual_server
    response = v1_transport._exchange(
        server.bound_port, ca_file,
        _wire(b'',
              constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2,
              server.bound_port,
              declared_length=65_537,
              send_body=False))
    body = v1_transport._assert_response(response, 413)
    assert body == resource_actions.canonical_json_bytes({
        'version': 2,
        'code': 'body_too_large',
    })
    assert calls == []


def test_v2_route_without_evaluator_returns_fixed_v2_unavailable(
        dual_server) -> None:
    server, ca_file, calls = dual_server
    server._evaluator_v2 = None
    request = v2_fixtures._launch_request()
    response = v1_transport._exchange(
        server.bound_port, ca_file,
        _wire(request.canonical_bytes,
              constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, server.bound_port))
    body = v1_transport._assert_response(response, 503)
    assert body == resource_actions.canonical_json_bytes({
        'version': 2,
        'code': 'cohort_unavailable',
    })
    assert calls == []


def test_exact_v2_runtime_wiring_returns_typed_503_with_zero_executor_effects(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the production role wiring through the real TLS transport."""

    tls_directory, ca_file = v1_transport._tls_tree(tmp_path)
    token_file = tmp_path / 'tokens'
    token_file.write_text(_TOKEN + '\n', encoding='ascii')
    monkeypatch.setenv(
        constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR,
        str(token_file))
    for env_name in (constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
                     constants.LB_AUTH_TOKEN_ENV_VAR):
        monkeypatch.delenv(env_name, raising=False)

    request = v2_fixtures._launch_request()
    manifest = request.expected_cohort_manifest
    pod_uid = '11111111-1111-4111-8111-111111111111'
    pod_identity = bootstrap_v1.AuthorityWorkerPodIdentity(
        'authority-worker-v2', manifest.namespace, pod_uid)
    lease = mock.Mock(instance_id=pod_uid)
    state = runtime.RuntimeState('authority-worker', mock.Mock(), lease, False)
    coordinator = mock.Mock()
    coordinator.failure = None
    coordinator.accepted_manifest.return_value = manifest
    health_server = mock.Mock()
    executor_start = mock.Mock(
        side_effect=AssertionError('V2 membership started an executor'))
    real_server_type = authority_preflight.AuthorityPreflightServer
    servers = []

    class RuntimeServer(real_server_type):

        def __init__(self, *args, **kwargs):
            kwargs['tls_directory'] = str(tls_directory)
            super().__init__(*args, **kwargs)
            servers.append(self)

    monkeypatch.setattr(authority_preflight, 'AuthorityPreflightServer',
                        RuntimeServer)
    monkeypatch.setattr(authority_preflight, 'authority_preflight_service_dns',
                        lambda: _SERVICE_DNS)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *_args, **_kwargs: health_server)
    monkeypatch.setattr(runtime, '_load_authority_static_manifest',
                        lambda: manifest)
    monkeypatch.setattr(runtime, '_build_authority_bootstrap_coordinator',
                        lambda loaded, identity: coordinator)
    monkeypatch.setattr(runtime, '_build_authority_preflight_evaluator_v2',
                        lambda worker_instance_id: lambda candidate: None)
    monkeypatch.setattr(bootstrap_v1.AuthorityWorkerPodIdentity,
                        'from_environment', lambda: pod_identity)
    monkeypatch.setattr(runtime.executor, 'start', executor_start)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])
    response_body = []

    def exercise_transport(value) -> None:
        assert value is coordinator
        assert len(servers) == 1
        server = servers[0]
        assert server.is_transport_ready()
        response = v1_transport._exchange(
            server.bound_port, ca_file,
            _wire(request.canonical_bytes,
                  constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2,
                  server.bound_port))
        response_body.append(v1_transport._assert_response(response, 503))

    monkeypatch.setattr(runtime, '_wait_for_authority_shutdown',
                        exercise_transport)
    args = SimpleNamespace(host='127.0.0.1',
                           metrics_port=0,
                           role_health_port=0,
                           authority_preflight_port=0)

    runtime.run_role(state, args)

    assert response_body == [
        resource_actions.canonical_json_bytes({
            'version': 2,
            'code': 'cohort_unavailable',
        })
    ]
    executor_start.assert_not_called()
    coordinator.start.assert_called_once_with()
    coordinator.stop.assert_called_once_with()
    lease.set_ready.assert_called_once_with(
        False, health_detail={'phase': 'preflight-only'})


def test_v2_evaluator_exception_returns_fixed_v2_unavailable(
        dual_server) -> None:
    server, ca_file, calls = dual_server

    def fail_evaluation(_request):
        raise RuntimeError('must not reach the wire')

    server._evaluator_v2 = fail_evaluation
    request = v2_fixtures._launch_request()
    response = v1_transport._exchange(
        server.bound_port, ca_file,
        _wire(request.canonical_bytes,
              constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, server.bound_port))
    body = v1_transport._assert_response(response, 503)
    assert body == resource_actions.canonical_json_bytes({
        'version': 2,
        'code': 'cohort_unavailable',
    })
    assert calls == []


def test_v2_blocked_evaluator_is_zero_queue_and_late_result_is_discarded(
        dual_server, monkeypatch: pytest.MonkeyPatch) -> None:
    server, ca_file, calls = dual_server
    del calls
    request = v2_fixtures._launch_request()
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    invocation_count = 0
    invocation_lock = threading.Lock()

    def block_evaluation(candidate):
        nonlocal invocation_count
        with invocation_lock:
            invocation_count += 1
        entered.set()
        release.wait(5)
        finished.set()
        return (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2.
                unavailable(candidate))

    server._evaluator_v2 = block_evaluation
    monkeypatch.setattr(authority_preflight, '_REQUEST_DEADLINE_SECONDS', 0.75)
    monkeypatch.setattr(authority_preflight, '_V2_RESPONSE_RESERVE_SECONDS',
                        0.20)
    wire = _wire(request.canonical_bytes,
                 constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, server.bound_port)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        first = pool.submit(v1_transport._exchange, server.bound_port, ca_file,
                            wire)
        assert entered.wait(2)
        started = time.monotonic()
        others = [
            pool.submit(v1_transport._exchange, server.bound_port, ca_file,
                        wire) for _ in range(7)
        ]
        responses = [first.result(timeout=2)]
        responses.extend(item.result(timeout=2) for item in others)
        elapsed = time.monotonic() - started

    expected = resource_actions.canonical_json_bytes({
        'version': 2,
        'code': 'cohort_unavailable',
    })
    assert [v1_transport._assert_response(value, 503) for value in responses
           ] == [expected] * 8
    assert elapsed < 0.85
    assert invocation_count == 1

    # The first validator completes only after its request is closed.  Its
    # typed result is not queued or published into any later request.
    release.set()
    assert finished.wait(2)
    # ``finished`` is set inside the evaluator, just before its wrapper
    # releases the zero-queue slot.  Wait for that release explicitly so this
    # recovery assertion cannot race the evaluator's ``finally`` block.
    slot_deadline = time.monotonic() + 2
    while not server._v2_evaluation_slot.acquire(blocking=False):
        assert time.monotonic() < slot_deadline
        time.sleep(0.001)
    server._v2_evaluation_slot.release()
    response = v1_transport._exchange(server.bound_port, ca_file, wire)
    body = v1_transport._assert_response(response, 200)
    parsed = preflight_v2.provider_authority_preflight_response_from_value_v2(
        json.loads(body))
    parsed.validate_request(request)
    assert invocation_count == 2


def test_stop_bounds_never_returning_v2_evaluator_and_server_is_one_shot(
        dual_server, monkeypatch: pytest.MonkeyPatch) -> None:
    server, ca_file, calls = dual_server
    del calls
    request = v2_fixtures._launch_request()
    entered = threading.Event()
    release = threading.Event()

    def never_return_until_released(candidate):
        entered.set()
        release.wait()
        return (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2.
                unavailable(candidate))

    server._evaluator_v2 = never_return_until_released
    monkeypatch.setattr(authority_preflight, '_REQUEST_DEADLINE_SECONDS', 0.5)
    monkeypatch.setattr(authority_preflight, '_V2_RESPONSE_RESERVE_SECONDS',
                        0.1)
    wire = _wire(request.canonical_bytes,
                 constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, server.bound_port)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        request_future = pool.submit(v1_transport._exchange, server.bound_port,
                                     ca_file, wire)
        assert entered.wait(2)
        started = time.monotonic()
        try:
            server.stop()
            assert time.monotonic() - started < 1.0
            body = v1_transport._assert_response(
                request_future.result(timeout=2), 503)
            assert body == resource_actions.canonical_json_bytes({
                'version': 2,
                'code': 'cohort_unavailable',
            })
            with pytest.raises(RuntimeError, match='one-shot'):
                server.start()
        finally:
            release.set()


def test_v2_slow_body_and_evaluator_share_one_absolute_deadline(
        dual_server, monkeypatch: pytest.MonkeyPatch) -> None:
    server, ca_file, calls = dual_server
    del calls
    request = v2_fixtures._launch_request()
    release = threading.Event()
    entered = threading.Event()

    def block_evaluation(_candidate):
        entered.set()
        release.wait(5)
        return None

    server._evaluator_v2 = block_evaluation
    monkeypatch.setattr(authority_preflight, '_REQUEST_DEADLINE_SECONDS', 1.0)
    monkeypatch.setattr(authority_preflight, '_V2_RESPONSE_RESERVE_SECONDS',
                        0.20)
    wire = _wire(request.canonical_bytes,
                 constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, server.bound_port)
    header, body = wire.split(b'\r\n\r\n', 1)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.set_alpn_protocols(['http/1.1'])
    context.load_verify_locations(cafile=str(ca_file))

    started = time.monotonic()
    with socket.create_connection(('127.0.0.1', server.bound_port),
                                  timeout=5) as raw:
        with context.wrap_socket(raw, server_hostname=_SERVICE_DNS) as secured:
            secured.sendall(header + b'\r\n\r\n')
            time.sleep(0.55)
            secured.sendall(body)
            response = bytearray()
            while True:
                chunk = secured.recv(8192)
                if not chunk:
                    break
                response.extend(chunk)
    elapsed = time.monotonic() - started
    try:
        response_body = v1_transport._assert_response(bytes(response), 503)
        assert response_body == resource_actions.canonical_json_bytes({
            'version': 2,
            'code': 'cohort_unavailable',
        })
        assert entered.is_set()
        assert elapsed < 1.10
    finally:
        release.set()


class _RecordingSession:
    """Minimal client session double that records the exact request route."""

    def __init__(self, response) -> None:
        self.response = response
        self.urls: list[str] = []

    def post(self, url: str, **unused_kwargs):
        del unused_kwargs
        self.urls.append(url)
        return self.response

    def close(self) -> None:
        pass


def _patch_client(monkeypatch: pytest.MonkeyPatch, session) -> None:
    monkeypatch.setattr(client, '_fresh_session', lambda *_: session)
    monkeypatch.setattr(client, '_validate_peer', lambda *_: None)
    monkeypatch.setattr(client.auth_tokens,
                        'get_isolated_resource_action_preflight_auth_tokens',
                        lambda **_: (_TOKEN,))


def test_v2_client_uses_only_v2_path_and_parser(
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = v2_fixtures._launch_request()
    expected = (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2.
                unavailable(request))
    session = _RecordingSession(
        v1_transport._FakeResponse(200, expected.canonical_bytes))
    _patch_client(monkeypatch, session)

    actual = client.request_provider_authority_preflight_v2(
        request, service_dns=_SERVICE_DNS, port=46583, ca_file='unused')
    assert actual.canonical_bytes == expected.canonical_bytes
    assert session.urls == [
        f'https://{_SERVICE_DNS}:46583'
        f'{constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2}'
    ]


def test_v2_client_rejects_v1_request_and_v1_response_without_coercion(
        monkeypatch: pytest.MonkeyPatch) -> None:
    v1_request = v1_transport.preflight_fixtures._launch_request()
    with pytest.raises(TypeError, match='V2 request'):
        client.request_provider_authority_preflight_v2(  # type: ignore[arg-type]
            v1_request,
            service_dns=_SERVICE_DNS,
            port=46583,
            ca_file='unused')

    v2_request = v2_fixtures._launch_request()
    v1_response = (resource_actions.ProviderLaunchAuthorityPreflightResponseV1.
                   unavailable(v1_request))
    session = _RecordingSession(
        v1_transport._FakeResponse(200, v1_response.canonical_bytes))
    _patch_client(monkeypatch, session)
    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v2(v2_request,
                                                       service_dns=_SERVICE_DNS,
                                                       port=46583,
                                                       ca_file='unused')
    assert len(session.urls) == 1


def test_v2_client_rejects_response_content_length_above_65536_without_reading(
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = v2_fixtures._launch_request()
    session = _RecordingSession(v1_transport._FakeResponse(200, b'x' * 65_537))
    _patch_client(monkeypatch, session)
    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v2(request,
                                                       service_dns=_SERVICE_DNS,
                                                       port=46583,
                                                       ca_file='unused')
    assert len(session.urls) == 1
