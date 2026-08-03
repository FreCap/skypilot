"""Strict private HTTPS authority-preflight edge tests."""

# pylint: disable=protected-access,redefined-outer-name

import datetime
import json
import pathlib
import socket
import ssl
import threading
import time
import types
from unittest import mock

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.x509.oid import NameOID
import pytest
import test_serve_resource_action_provider_preflight as preflight_fixtures
from urllib3._collections import HTTPHeaderDict

from sky.serve import constants
from sky.serve import resource_action_preflight_client as client
from sky.serve import resource_action_provider_preflight as evaluator_lib
from sky.serve import resource_actions as actions
from sky.server import authority_preflight

_SERVICE_DNS = 'skypilot-authority-preflight.skypilot-system.svc'
_TOKEN = 't' * 48


def _certificate_material(service_dns: str) -> tuple[bytes, bytes, bytes]:
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, 'preflight-test-ca')])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(
        ca_name).public_key(ca_key.public_key()).serial_number(
            x509.random_serial_number()).not_valid_before(
                now - datetime.timedelta(minutes=1)).not_valid_after(
                    now + datetime.timedelta(hours=1)).add_extension(
                        x509.BasicConstraints(ca=True, path_length=0),
                        critical=True).sign(ca_key, hashes.SHA256()))
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, service_dns)])
    leaf = (x509.CertificateBuilder().subject_name(leaf_name).issuer_name(
        ca_name).public_key(leaf_key.public_key()).serial_number(
            x509.random_serial_number()).not_valid_before(now -
                                                          datetime.timedelta(
                                                              minutes=1)).
            not_valid_after(now + datetime.timedelta(hours=1)).add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True).add_extension(
                    x509.SubjectAlternativeName([x509.DNSName(service_dns)]),
                    critical=False).add_extension(
                        x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH
                                              ]),
                        critical=True).sign(ca_key, hashes.SHA256()))
    cert_pem = leaf.public_bytes(serialization.Encoding.PEM)
    key_pem = leaf_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption())
    ca_pem = ca.public_bytes(serialization.Encoding.PEM)
    return cert_pem, key_pem, ca_pem


def _tls_tree(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    tls_directory = tmp_path / 'tls'
    tls_directory.mkdir()
    generation = tls_directory / '..2026_08_02_00_00_00.000000001'
    generation.mkdir(parents=True)
    (tls_directory / '..data').symlink_to(generation.name)
    cert_pem, key_pem, ca_pem = _certificate_material(_SERVICE_DNS)
    (generation / 'tls.crt').write_bytes(cert_pem)
    (generation / 'tls.key').write_bytes(key_pem)
    (generation / 'ca.crt').write_bytes(ca_pem)
    ca_file = tmp_path / 'ca.crt'
    ca_file.write_bytes(ca_pem)
    return tls_directory, ca_file


def _slow_response_server(tmp_path: pathlib.Path, body: bytes,
                          delay_seconds: float):
    cert_pem, key_pem, ca_pem = _certificate_material('localhost')
    cert_file = tmp_path / 'slow.crt'
    key_file = tmp_path / 'slow.key'
    ca_file = tmp_path / 'slow-ca.crt'
    cert_file.write_bytes(cert_pem)
    key_file.write_bytes(key_pem)
    ca_file.write_bytes(ca_pem)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.set_alpn_protocols(['http/1.1'])
    context.load_cert_chain(str(cert_file), str(key_file))

    def _serve() -> None:
        try:
            connection, _ = listener.accept()
            with connection, context.wrap_socket(connection,
                                                 server_side=True) as secured:
                request_bytes = bytearray()
                while b'\r\n\r\n' not in request_bytes:
                    request_bytes.extend(secured.recv(8192))
                header_bytes, initial_body = bytes(request_bytes).split(
                    b'\r\n\r\n', 1)
                length_line = next(
                    line for line in header_bytes.split(b'\r\n')
                    if line.lower().startswith(b'content-length:'))
                request_length = int(length_line.split(b':', 1)[1])
                received = len(initial_body)
                while received < request_length:
                    received += len(secured.recv(8192))
                response_headers = (
                    b'HTTP/1.1 503 Service Unavailable\r\n'
                    b'Content-Type: application/json\r\n' +
                    f'Content-Length: {len(body)}\r\n'.encode('ascii') +
                    b'Cache-Control: no-store\r\n'
                    b'X-Content-Type-Options: nosniff\r\n'
                    b'Connection: close\r\n\r\n')
                secured.sendall(response_headers)
                for value in body:
                    secured.sendall(bytes((value,)))
                    time.sleep(delay_seconds)
        except (BrokenPipeError, ConnectionError, OSError, ssl.SSLError):
            pass
        finally:
            listener.close()

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    return port, ca_file, thread


def test_tls_generation_symlink_cannot_escape_projected_directory(
        tmp_path: pathlib.Path) -> None:
    tls_directory = tmp_path / 'tls'
    tls_directory.mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    (tls_directory / '..data').symlink_to('../outside')
    with pytest.raises(ValueError, match='symlink target'):
        authority_preflight._load_tls_generation(str(tls_directory),
                                                 _SERVICE_DNS)


def test_tls_material_rejects_wrong_san_key_ca_and_duplicate_roots() -> None:
    cert_pem, key_pem, ca_pem = _certificate_material(_SERVICE_DNS)
    _, other_key, other_ca = _certificate_material('other.example.svc')
    with pytest.raises(ValueError, match='SAN'):
        authority_preflight._validate_certificate_material(
            cert_pem, key_pem, ca_pem, 'different.example.svc')
    with pytest.raises(ValueError, match='do not match'):
        authority_preflight._validate_certificate_material(
            cert_pem, other_key, ca_pem, _SERVICE_DNS)
    with pytest.raises(ValueError, match='terminate'):
        authority_preflight._validate_certificate_material(
            cert_pem, key_pem, other_ca, _SERVICE_DNS)
    with pytest.raises(ValueError, match='duplicates'):
        authority_preflight._validate_certificate_material(
            cert_pem, key_pem, ca_pem + ca_pem, _SERVICE_DNS)


def _wire_request(request: actions.ProviderAuthorityPreflightRequestV1,
                  port: int,
                  *,
                  token: str = _TOKEN,
                  omit_content_length: bool = False,
                  extra_header: bytes = b'',
                  send_body: bool = True) -> bytes:
    body = request.canonical_bytes
    headers = [
        f'POST {constants.RESOURCE_ACTION_PREFLIGHT_PATH} HTTP/1.1',
        f'Host: {_SERVICE_DNS}:{port}',
        f'Authorization: Bearer {token}',
        'Content-Type: application/json',
        'Accept: application/json',
        'Accept-Encoding: identity',
    ]
    if not omit_content_length:
        headers.append(f'Content-Length: {len(body)}')
    headers.append('Connection: close')
    encoded = ('\r\n'.join(headers) + '\r\n').encode()
    if extra_header:
        encoded += extra_header
    encoded += b'\r\n'
    if send_body:
        encoded += body
    return encoded


def _exchange(port: int, ca_file: pathlib.Path, wire_request: bytes) -> bytes:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.set_alpn_protocols(['http/1.1'])
    context.load_verify_locations(cafile=str(ca_file))
    with socket.create_connection(('127.0.0.1', port), timeout=5) as raw:
        with context.wrap_socket(raw, server_hostname=_SERVICE_DNS) as secured:
            assert secured.selected_alpn_protocol() == 'http/1.1'
            secured.sendall(wire_request)
            response = bytearray()
            while True:
                chunk = secured.recv(8192)
                if not chunk:
                    return bytes(response)
                response.extend(chunk)


def _assert_response(response: bytes, status: int) -> bytes:
    header_block, body = response.split(b'\r\n\r\n', 1)
    lines = header_block.decode('ascii').split('\r\n')
    assert lines[0].startswith(f'HTTP/1.1 {status} ')
    headers = dict(line.split(': ', 1) for line in lines[1:])
    assert headers['Content-Type'] == 'application/json'
    assert headers['Cache-Control'] == 'no-store'
    assert headers['X-Content-Type-Options'] == 'nosniff'
    assert headers['Connection'] == 'close'
    assert int(headers['Content-Length']) == len(body)
    return body


@pytest.fixture
def launch_request() -> actions.ProviderAuthorityPreflightRequestV1:
    return preflight_fixtures._launch_request()


@pytest.fixture
def running_server(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
                   launch_request):
    tls_directory, ca_file = _tls_tree(tmp_path)
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
    manifest = launch_request.expected_cohort_manifest
    accepted = [manifest]
    evaluator = evaluator_lib.InitialProviderPreflightEvaluator(
        lambda: accepted[0] if accepted else None)
    invalidations: list[None] = []

    def _invalidate() -> None:
        invalidations.append(None)
        accepted.clear()

    server = authority_preflight.AuthorityPreflightServer(
        '127.0.0.1',
        0,
        _SERVICE_DNS,
        evaluator,
        on_transport_invalid=_invalidate,
        tls_directory=str(tls_directory))
    server.start()
    assert server.is_transport_ready()
    try:
        yield server, ca_file, token_file, accepted
    finally:
        server.stop()


def test_https_server_returns_nonce_bound_typed_response(
        running_server, launch_request) -> None:
    server, ca_file, _, _ = running_server
    response = _exchange(server.bound_port, ca_file,
                         _wire_request(launch_request, server.bound_port))
    body = _assert_response(response, 200)
    parsed = actions.provider_authority_preflight_response_from_value_v1(
        json.loads(body))
    assert parsed.canonical_bytes == body
    parsed.validate_request(launch_request)


@pytest.mark.parametrize('wire_builder,status,code', [
    (lambda request, port: _wire_request(
        request, port, token='wrong' * 8, send_body=False), 401,
     'unauthorized'),
    (lambda request, port: _wire_request(
        request, port, omit_content_length=True), 411, 'length_required'),
    (lambda request, port: _wire_request(
        request, port, extra_header=b'Content-Length: 1\r\n'), 400,
     'bad_request'),
    (lambda request, port:
     _wire_request(request,
                   port,
                   omit_content_length=True,
                   extra_header=b'Content-Length: ' + b'9' * 5_000 + b'\r\n',
                   send_body=False), 413, 'body_too_large'),
    (lambda request, port: _wire_request(
        request, port, extra_header=b'X-Unknown: value\r\n'), 400,
     'bad_request'),
])
def test_https_server_rejects_noncanonical_edge_requests(
        running_server, launch_request, wire_builder, status: int,
        code: str) -> None:
    server, ca_file, _, _ = running_server
    response = _exchange(server.bound_port, ca_file,
                         wire_builder(launch_request, server.bound_port))
    body = _assert_response(response, status)
    assert body == actions.canonical_json_bytes({'version': 1, 'code': code})


def test_invalid_token_rotation_clears_acceptance_until_fresh_adoption(
        running_server, launch_request) -> None:
    server, ca_file, token_file, accepted = running_server
    token_file.write_text('too-short\n', encoding='ascii')
    assert not server.is_transport_ready()
    assert accepted == []

    token_file.write_text(_TOKEN + '\n', encoding='ascii')
    assert server.is_transport_ready()
    response = _exchange(server.bound_port, ca_file,
                         _wire_request(launch_request, server.bound_port))
    body = _assert_response(response, 503)
    assert body == actions.canonical_json_bytes({
        'version': 1,
        'code': 'cohort_unavailable',
    })

    accepted.append(launch_request.expected_cohort_manifest)
    response = _exchange(server.bound_port, ca_file,
                         _wire_request(launch_request, server.bound_port))
    body = _assert_response(response, 200)
    parsed = actions.provider_authority_preflight_response_from_value_v1(
        json.loads(body))
    parsed.validate_request(launch_request)


class _FakeResponse:
    """Minimal urllib3 response double with a deadline-aware body stream."""

    def __init__(self, status: int, body: bytes, *, extra_header: bool = False):
        self.status_code = status
        headers = HTTPHeaderDict()
        headers.add('Content-Type', 'application/json')
        headers.add('Content-Length', str(len(body)))
        headers.add('Cache-Control', 'no-store')
        headers.add('X-Content-Type-Options', 'nosniff')
        headers.add('Connection', 'close')
        if extra_header:
            headers.add('X-Unknown', 'invalid')
        raw = mock.Mock()
        raw.headers = headers
        raw.connection = types.SimpleNamespace(sock=mock.Mock())
        chunks = [body]
        raw.read1.side_effect = lambda *_args, **_kwargs: chunks.pop(0)
        self.raw = raw

    def close(self) -> None:
        pass


class _FakeSession:
    """Minimal urllib3 session double recording request timeouts."""

    def __init__(self, responses: list[_FakeResponse]):
        self._responses = responses
        self.calls = 0
        self.timeouts = []

    def post(self, *unused_args, **unused_kwargs):
        del unused_args
        self.timeouts.append(unused_kwargs['timeout'])
        response = self._responses[self.calls]
        self.calls += 1
        return response

    def close(self) -> None:
        pass


def test_client_does_not_retry_malformed_503(
        launch_request, monkeypatch: pytest.MonkeyPatch) -> None:
    body = actions.canonical_json_bytes({
        'version': 1,
        'code': 'cohort_unavailable',
    })
    session = _FakeSession([_FakeResponse(503, body, extra_header=True)])
    monkeypatch.setattr(client, '_fresh_session', lambda *_: session)
    monkeypatch.setattr(client, '_validate_peer', lambda *_: None)
    monkeypatch.setattr(client.auth_tokens,
                        'get_isolated_resource_action_preflight_auth_tokens',
                        lambda **_: (_TOKEN,))
    monkeypatch.setattr(client.time, 'sleep', lambda _: None)
    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v1(launch_request,
                                                       service_dns=_SERVICE_DNS,
                                                       port=46583,
                                                       ca_file='unused')
    assert session.calls == 1


def test_client_retries_one_exact_503_then_fails_closed(
        launch_request, monkeypatch: pytest.MonkeyPatch) -> None:
    body = actions.canonical_json_bytes({
        'version': 1,
        'code': 'cohort_unavailable',
    })
    session = _FakeSession([_FakeResponse(503, body), _FakeResponse(503, body)])
    monkeypatch.setattr(client, '_fresh_session', lambda *_: session)
    monkeypatch.setattr(client, '_validate_peer', lambda *_: None)
    monkeypatch.setattr(client.auth_tokens,
                        'get_isolated_resource_action_preflight_auth_tokens',
                        lambda **_: (_TOKEN,))
    sleeps: list[float] = []
    monkeypatch.setattr(client.time, 'sleep', sleeps.append)
    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v1(launch_request,
                                                       service_dns=_SERVICE_DNS,
                                                       port=46583,
                                                       ca_file='unused')
    assert session.calls == 2
    assert sleeps == [0.1]


def test_client_retry_shares_one_five_second_deadline(
        launch_request, monkeypatch: pytest.MonkeyPatch) -> None:
    body = actions.canonical_json_bytes({
        'version': 1,
        'code': 'cohort_unavailable',
    })
    session = _FakeSession([_FakeResponse(503, body), _FakeResponse(503, body)])
    monkeypatch.setattr(client, '_fresh_session', lambda *_: session)
    monkeypatch.setattr(client, '_validate_peer', lambda *_: None)
    monkeypatch.setattr(client.auth_tokens,
                        'get_isolated_resource_action_preflight_auth_tokens',
                        lambda **_: (_TOKEN,))
    monotonic_values = iter(
        (100.0, 100.0, 100.1, 100.2, 102.0, 102.1, 102.2, 102.3))
    monkeypatch.setattr(client.time, 'monotonic',
                        lambda: next(monotonic_values))
    sleeps: list[float] = []
    monkeypatch.setattr(client.time, 'sleep', sleeps.append)

    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v1(launch_request,
                                                       service_dns=_SERVICE_DNS,
                                                       port=46583,
                                                       ca_file='unused')

    assert session.calls == 2
    assert [timeout.total for timeout in session.timeouts
           ] == pytest.approx([5.0, 2.9])
    assert sleeps == [0.1]


def test_client_skips_retry_when_delay_would_cross_total_deadline(
        launch_request, monkeypatch: pytest.MonkeyPatch) -> None:
    body = actions.canonical_json_bytes({
        'version': 1,
        'code': 'cohort_unavailable',
    })
    session = _FakeSession([_FakeResponse(503, body)])
    monkeypatch.setattr(client, '_fresh_session', lambda *_: session)
    monkeypatch.setattr(client, '_validate_peer', lambda *_: None)
    monkeypatch.setattr(client.auth_tokens,
                        'get_isolated_resource_action_preflight_auth_tokens',
                        lambda **_: (_TOKEN,))
    monotonic_values = iter((100.0, 100.0, 100.1, 100.2, 104.95))
    monkeypatch.setattr(client.time, 'monotonic',
                        lambda: next(monotonic_values))
    sleep = mock.Mock()
    monkeypatch.setattr(client.time, 'sleep', sleep)

    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v1(launch_request,
                                                       service_dns=_SERVICE_DNS,
                                                       port=46583,
                                                       ca_file='unused')

    assert session.calls == 1
    sleep.assert_not_called()


def test_client_terminates_real_slow_drip_within_one_total_deadline(
        tmp_path: pathlib.Path, launch_request,
        monkeypatch: pytest.MonkeyPatch) -> None:
    body = actions.canonical_json_bytes({
        'version': 1,
        'code': 'cohort_unavailable',
    })
    port, ca_file, thread = _slow_response_server(tmp_path, body, 0.12)
    token_file = tmp_path / 'slow.tokens'
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
    monkeypatch.setattr(client, '_TOTAL_TIMEOUT_SECONDS', 0.35)
    monkeypatch.setattr(client, '_CONNECT_TIMEOUT_SECONDS', 0.2)
    started = time.monotonic()

    with pytest.raises(client.ProviderAuthorityPreflightTransportError):
        client.request_provider_authority_preflight_v1(launch_request,
                                                       service_dns='localhost',
                                                       port=port,
                                                       ca_file=str(ca_file))

    elapsed = time.monotonic() - started
    thread.join(timeout=2)
    assert elapsed < 0.8
