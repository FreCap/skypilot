"""Trust-isolated HTTPS client for private authority preflight."""

from __future__ import annotations

import datetime
import json
import ssl
import time
import typing
from typing import Any

from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID
import urllib3.exceptions
import urllib3.util

from sky.adaptors import common as adaptors_common
from sky.serve import auth_tokens
from sky.serve import constants
from sky.serve import resource_action_preflight_v2
from sky.serve import resource_actions
from sky.server import authority_preflight
from sky.server.requests import resource_actions as kernel_actions

if typing.TYPE_CHECKING:
    import requests
else:
    requests = adaptors_common.LazyImport('requests')

_MAX_RESPONSE_BYTES = 65_536
_RESPONSE_CHUNK_BYTES = 8_192
_RETRY_DELAY_SECONDS = 0.1
_CONNECT_TIMEOUT_SECONDS = 1.0
_TOTAL_TIMEOUT_SECONDS = 5.0
_BASE_RESPONSE_HEADERS = frozenset({
    'Content-Type', 'Content-Length', 'Cache-Control', 'X-Content-Type-Options',
    'Connection'
})
_ERROR_CODES = {
    400: 'bad_request',
    401: 'unauthorized',
    404: 'not_found',
    405: 'method_not_allowed',
    408: 'timeout',
    411: 'length_required',
    413: 'body_too_large',
    415: 'unsupported_media_type',
    431: 'headers_too_large',
    503: 'cohort_unavailable',
}


class ProviderAuthorityPreflightTransportError(RuntimeError):
    """The private transport could not produce matching typed evidence."""

    def __init__(self, action_kind: kernel_actions.ActionKind | str) -> None:
        kind = kernel_actions.ActionKind(action_kind)
        self.reason: (resource_actions.ProviderLaunchNotRepresentableReasonV1 |
                      resource_actions.ProviderDownNotRepresentableReasonV1)
        if kind is kernel_actions.ActionKind.LAUNCH:
            self.reason = (
                resource_actions.ProviderLaunchNotRepresentableReasonV1.
                PREFLIGHT_UNAVAILABLE_OR_INVALID)
        else:
            self.reason = (resource_actions.ProviderDownNotRepresentableReasonV1
                           .PREFLIGHT_UNAVAILABLE_OR_INVALID)
        super().__init__('Provider authority preflight is unavailable or '
                         'invalid.')


class _StrictTLSAdapter(requests.adapters.HTTPAdapter):
    """Install one purpose-only SSLContext and exact hostname assertion."""

    def __init__(self, context: ssl.SSLContext, service_dns: str) -> None:
        self._context = context
        self._service_dns = service_dns
        super().__init__(max_retries=0)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self._context
        kwargs['assert_hostname'] = self._service_dns
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        raise RuntimeError('authority preflight proxies are forbidden')

    def cert_verify(self, conn, url, verify, cert):
        # The freshly constructed context already contains the sole purpose CA.
        # The base implementation would attach system/default CA paths.
        del conn, url, verify, cert


def _request_timeout(remaining_seconds: float) -> urllib3.util.Timeout:
    return urllib3.util.Timeout(connect=min(_CONNECT_TIMEOUT_SECONDS,
                                            remaining_seconds),
                                total=remaining_seconds)


def _exception_tree_contains(
        error: BaseException, exception_types: tuple[type[BaseException],
                                                     ...]) -> bool:
    pending = [error]
    visited: set[int] = set()
    while pending:
        candidate = pending.pop()
        if id(candidate) in visited:
            continue
        visited.add(id(candidate))
        if isinstance(candidate, exception_types):
            return True
        if candidate.__cause__ is not None:
            pending.append(candidate.__cause__)
        if candidate.__context__ is not None:
            pending.append(candidate.__context__)
        pending.extend(argument for argument in candidate.args
                       if isinstance(argument, BaseException))
    return False


def _is_retryable_transport_error(error: BaseException) -> bool:
    if isinstance(error, requests.exceptions.SSLError):
        return False
    timeout_types: tuple[type[BaseException],
                         ...] = (requests.exceptions.Timeout, TimeoutError,
                                 urllib3.exceptions.TimeoutError)
    if _exception_tree_contains(error, timeout_types):
        return True
    return _exception_tree_contains(error, (ConnectionResetError,))


def _purpose_ssl_context(ca_file: str) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    context.set_alpn_protocols(['http/1.1'])
    context.options |= getattr(ssl, 'OP_NO_COMPRESSION', 0)
    context.load_verify_locations(cafile=ca_file)
    return context


def _fresh_session(ca_file: str, service_dns: str) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.clear()
    session.cookies.clear()
    session.auth = None
    session.mount('https://',
                  _StrictTLSAdapter(_purpose_ssl_context(ca_file), service_dns))
    return session


def _certificate_time(certificate: x509.Certificate,
                      attribute: str) -> datetime.datetime:
    value = getattr(certificate, f'{attribute}_utc', None)
    if value is not None:
        return value
    return getattr(certificate, attribute).replace(tzinfo=datetime.timezone.utc)


def _peer_socket(response: requests.Response) -> ssl.SSLSocket:
    connection = getattr(response.raw, 'connection', None)
    sock = getattr(connection, 'sock', None)
    if sock is None:
        connection = getattr(response.raw, '_connection', None)
        sock = getattr(connection, 'sock', None)
    if (sock is None or
            any(not callable(getattr(sock, method, None))
                for method in ('settimeout', 'selected_alpn_protocol',
                               'getpeercert'))):
        raise ValueError('authority preflight peer socket is unavailable')
    return typing.cast(ssl.SSLSocket, sock)


def _validate_peer(response: requests.Response, service_dns: str) -> None:
    sock = _peer_socket(response)
    if sock.selected_alpn_protocol() != 'http/1.1':
        raise ValueError('authority preflight ALPN is not HTTP/1.1')
    der = sock.getpeercert(binary_form=True)
    if type(der) is not bytes or not der:
        raise ValueError('authority preflight peer certificate is unavailable')
    certificate = x509.load_der_x509_certificate(der)
    now = datetime.datetime.now(datetime.timezone.utc)
    if (now < _certificate_time(certificate, 'not_valid_before') or
            now > _certificate_time(certificate, 'not_valid_after')):
        raise ValueError('authority preflight peer certificate is stale')
    basic = certificate.extensions.get_extension_for_class(
        x509.BasicConstraints).value
    eku = certificate.extensions.get_extension_for_class(
        x509.ExtendedKeyUsage).value
    san = certificate.extensions.get_extension_for_class(
        x509.SubjectAlternativeName).value
    names = list(san)
    if basic.ca or set(eku) != {ExtendedKeyUsageOID.SERVER_AUTH}:
        raise ValueError('authority preflight leaf constraints are invalid')
    if (len(names) != 1 or not isinstance(names[0], x509.DNSName) or
            names[0].value != service_dns):
        raise ValueError('authority preflight SAN is not the exact Service DNS')


def _raw_response_headers(response: requests.Response) -> Any:
    headers = getattr(response.raw, 'headers', None)
    if headers is None or not hasattr(headers, 'getlist'):
        raise ValueError('authority preflight raw response headers unavailable')
    return headers


def _validate_response_headers(response: requests.Response) -> int:
    raw_headers = _raw_response_headers(response)
    expected = set(_BASE_RESPONSE_HEADERS)
    if response.status_code == 401:
        expected.add('WWW-Authenticate')
    elif response.status_code == 405:
        expected.add('Allow')
    actual = set(raw_headers.keys())
    if actual != expected:
        raise ValueError('authority preflight response headers are not closed')
    for name in expected:
        values = raw_headers.getlist(name)
        if len(values) != 1:
            raise ValueError('authority preflight response header is duplicate')
    if (raw_headers.getlist('Content-Type')[0] != 'application/json' or
            raw_headers.getlist('Cache-Control')[0] != 'no-store' or
            raw_headers.getlist('X-Content-Type-Options')[0] != 'nosniff' or
            raw_headers.getlist('Connection')[0].lower() != 'close'):
        raise ValueError('authority preflight response headers are invalid')
    if (response.status_code == 401 and
            raw_headers.getlist('WWW-Authenticate')[0] != 'Bearer'):
        raise ValueError('authority preflight challenge is invalid')
    if (response.status_code == 405 and
            raw_headers.getlist('Allow')[0] != 'POST'):
        raise ValueError('authority preflight method allowlist is invalid')
    length_text = raw_headers.getlist('Content-Length')[0]
    if (not length_text.isascii() or not length_text.isdecimal() or
            str(int(length_text)) != length_text):
        raise ValueError('authority preflight response length is noncanonical')
    length = int(length_text)
    if length < 1 or length > _MAX_RESPONSE_BYTES:
        raise ValueError('authority preflight response length is out of bounds')
    return length


def _read_exact_body(response: requests.Response, expected_length: int,
                     deadline: float) -> bytes:
    raw = response.raw
    if not hasattr(raw, 'read1'):
        raise ValueError('authority preflight raw body reader is unavailable')
    sock = _peer_socket(response)
    body = bytearray()
    while len(body) < expected_length:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise TimeoutError('authority preflight body deadline expired')
        # urllib3's total timeout is not a wall-clock download deadline. read1
        # returns after at most one underlying buffered read, letting us reduce
        # the TLS socket timeout before every slow-drip byte/chunk.
        sock.settimeout(remaining_seconds)
        chunk = raw.read1(min(_RESPONSE_CHUNK_BYTES,
                              expected_length - len(body)),
                          decode_content=False)
        if not chunk:
            raise ValueError('authority preflight response body is short')
        if type(chunk) is not bytes or len(chunk) > expected_length - len(body):
            raise ValueError('authority preflight response body is excessive')
        body.extend(chunk)
    if time.monotonic() >= deadline:
        raise TimeoutError('authority preflight body deadline expired')
    return bytes(body)


def _decode_json(body: bytes) -> Any:

    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result

    def _forbid_float(_: str) -> Any:
        raise ValueError('floating-point JSON is forbidden')

    value = json.loads(body.decode('utf-8'),
                       object_pairs_hook=_object,
                       parse_float=_forbid_float,
                       parse_constant=_forbid_float)
    if resource_actions.canonical_json_bytes(value) != body:
        raise ValueError('authority preflight response is noncanonical')
    return value


def _validate_error_response(response: requests.Response, body: bytes,
                             protocol_version: int) -> None:
    code = _ERROR_CODES.get(response.status_code)
    if code is None:
        raise ValueError('authority preflight status is outside the protocol')
    expected = resource_actions.canonical_json_bytes({
        'version': protocol_version,
        'code': code,
    })
    if body != expected:
        raise ValueError('authority preflight error body is invalid')


def _request_provider_authority_preflight(
    request: Any,
    *,
    path: str,
    protocol_version: int,
    response_parser: typing.Callable[[Any], Any],
    service_dns: str,
    port: int,
    ca_file: str,
) -> Any:
    """Execute the common trust edge after version-specific type selection."""

    body = request.canonical_bytes
    url = f'https://{service_dns}:{port}{path}'
    deadline = time.monotonic() + _TOTAL_TIMEOUT_SECONDS

    for attempt in range(2):
        response = None
        session = None
        retry = False
        try:
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                raise ProviderAuthorityPreflightTransportError(
                    request.action_kind)
            tokens = (
                auth_tokens.get_isolated_resource_action_preflight_auth_tokens(
                    required=True))
            headers = {
                'Host': f'{service_dns}:{port}',
                'Authorization': f'Bearer {tokens[0]}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Accept-Encoding': 'identity',
                'Content-Length': str(len(body)),
                'Connection': 'close',
            }
            session = _fresh_session(ca_file, service_dns)
            response = session.post(
                url,
                data=body,
                headers=headers,
                allow_redirects=False,
                stream=True,
                # requests accepts urllib3's richer total timeout object at
                # runtime, while its public type stub exposes only scalar or
                # connect/read tuple forms.
                timeout=typing.cast(Any, _request_timeout(remaining_seconds)))
            _validate_peer(response, service_dns)
            if response.status_code in (502, 504):
                if attempt == 0:
                    retry = True
                else:
                    raise ProviderAuthorityPreflightTransportError(
                        request.action_kind)
            else:
                expected_length = _validate_response_headers(response)
                response_body = _read_exact_body(response, expected_length,
                                                 deadline)
                if response.status_code == 503:
                    _validate_error_response(response, response_body,
                                             protocol_version)
                    if attempt == 0:
                        retry = True
                    else:
                        raise ProviderAuthorityPreflightTransportError(
                            request.action_kind)
                elif response.status_code != 200:
                    _validate_error_response(response, response_body,
                                             protocol_version)
                    raise ProviderAuthorityPreflightTransportError(
                        request.action_kind)
                else:
                    value = _decode_json(response_body)
                    parsed = response_parser(value)
                    if parsed.canonical_bytes != response_body:
                        raise ValueError(
                            'authority response bytes changed on decode')
                    parsed.validate_request(request)
                    if time.monotonic() >= deadline:
                        raise ValueError(
                            'authority response exceeded its total deadline')
                    return parsed
        except ProviderAuthorityPreflightTransportError:
            raise
        except Exception as error:  # pylint: disable=broad-except
            if attempt == 0 and _is_retryable_transport_error(error):
                retry = True
            else:
                raise ProviderAuthorityPreflightTransportError(
                    request.action_kind) from None
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:  # pylint: disable=broad-except
                    pass
            if session is not None:
                try:
                    session.close()
                except Exception:  # pylint: disable=broad-except
                    pass
        if retry:
            if deadline - time.monotonic() <= _RETRY_DELAY_SECONDS:
                raise ProviderAuthorityPreflightTransportError(
                    request.action_kind)
            time.sleep(_RETRY_DELAY_SECONDS)
            continue
    raise AssertionError('authority preflight retry loop did not terminate')


def request_provider_authority_preflight_v1(
    request: resource_actions.ProviderAuthorityPreflightRequestV1,
    *,
    service_dns: str | None = None,
    port: int = constants.RESOURCE_ACTION_PREFLIGHT_PORT,
    ca_file: str = f'{constants.RESOURCE_ACTION_PREFLIGHT_TLS_DIRECTORY}/ca.crt',
) -> resource_actions.ProviderAuthorityPreflightResponseV1:
    """Perform the exact bounded call, retrying only the closed transient set."""

    if type(request
           ) is not resource_actions.ProviderAuthorityPreflightRequestV1:
        raise TypeError('authority preflight request has an invalid type.')
    if service_dns is None:
        service_dns = authority_preflight.authority_preflight_service_dns()
    if type(service_dns) is not str or not service_dns:
        raise ValueError('authority preflight Service DNS is invalid.')
    if type(port) is not int or isinstance(port,
                                           bool) or not 1 <= port <= 65_535:
        raise ValueError('authority preflight port is invalid.')
    return typing.cast(
        resource_actions.ProviderAuthorityPreflightResponseV1,
        _request_provider_authority_preflight(
            request,
            path=constants.RESOURCE_ACTION_PREFLIGHT_PATH_V1,
            protocol_version=1,
            response_parser=resource_actions.
            provider_authority_preflight_response_from_value_v1,
            service_dns=service_dns,
            port=port,
            ca_file=ca_file))


def request_provider_authority_preflight_v2(
    request: resource_action_preflight_v2.ProviderAuthorityPreflightRequestV2,
    *,
    service_dns: str | None = None,
    port: int = constants.RESOURCE_ACTION_PREFLIGHT_PORT,
    ca_file: str = f'{constants.RESOURCE_ACTION_PREFLIGHT_TLS_DIRECTORY}/ca.crt',
) -> resource_action_preflight_v2.ProviderAuthorityPreflightResponseV2:
    """Perform one typed V2 call without accepting a V1 wire value."""

    if type(request) is not (
            resource_action_preflight_v2.ProviderAuthorityPreflightRequestV2):
        raise TypeError('authority preflight V2 request has an invalid type.')
    if service_dns is None:
        service_dns = authority_preflight.authority_preflight_service_dns()
    if type(service_dns) is not str or not service_dns:
        raise ValueError('authority preflight Service DNS is invalid.')
    if type(port) is not int or isinstance(port,
                                           bool) or not 1 <= port <= 65_535:
        raise ValueError('authority preflight port is invalid.')
    return typing.cast(
        resource_action_preflight_v2.ProviderAuthorityPreflightResponseV2,
        _request_provider_authority_preflight(
            request,
            path=constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2,
            protocol_version=2,
            response_parser=(
                resource_action_preflight_v2.
                provider_authority_preflight_response_from_value_v2),
            service_dns=service_dns,
            port=port,
            ca_file=ca_file))


# Concise public spelling for the controller integration.
preflight = request_provider_authority_preflight_v1

__all__ = [
    'ProviderAuthorityPreflightTransportError', 'preflight',
    'request_provider_authority_preflight_v1',
    'request_provider_authority_preflight_v2'
]
