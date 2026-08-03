"""Strict private HTTPS edge for Kubernetes authority preflight."""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
import concurrent.futures
import datetime
import hmac
import json
import os
import re
import socket
import ssl
import stat
import struct
import threading
import time
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric import ed448
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID

from sky.serve import auth_tokens
from sky.serve import constants
from sky.serve import resource_actions

_MAX_REQUEST_LINE_BYTES = 2_048
_MAX_HEADER_COUNT = 32
_MAX_HEADER_LINE_BYTES = 8_192
_MAX_AGGREGATE_HEADER_BYTES = 32_768
_MAX_BODY_BYTES = 65_536
_MAX_TLS_FILE_BYTES = 65_536
_WORKER_COUNT = 8
_LISTEN_BACKLOG = 16
_REQUEST_DEADLINE_SECONDS = 5.0
_TLS_POLL_SECONDS = 0.5
_CERTIFICATE_BLOCK_RE = re.compile(
    br'-----BEGIN CERTIFICATE-----\r?\n.*?\r?\n-----END CERTIFICATE-----\r?\n?',
    re.DOTALL)
_DNS_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_ERRORS = {
    400: ('Bad Request', 'bad_request'),
    401: ('Unauthorized', 'unauthorized'),
    404: ('Not Found', 'not_found'),
    405: ('Method Not Allowed', 'method_not_allowed'),
    408: ('Request Timeout', 'timeout'),
    411: ('Length Required', 'length_required'),
    413: ('Content Too Large', 'body_too_large'),
    415: ('Unsupported Media Type', 'unsupported_media_type'),
    431: ('Request Header Fields Too Large', 'headers_too_large'),
    503: ('Service Unavailable', 'cohort_unavailable'),
}
_EXPECTED_HEADER_NAMES = frozenset({
    'Host', 'Authorization', 'Content-Type', 'Accept', 'Accept-Encoding',
    'Content-Length', 'Connection'
})
_FORBIDDEN_HEADER_NAMES = frozenset({
    'content-encoding', 'transfer-encoding', 'expect', 'proxy-authorization',
    'cookie'
})


class _HTTPFailure(Exception):

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(str(status))


def authority_preflight_service_dns(
        environ: Mapping[str, str] | None = None) -> str:
    """Return the exact in-release authority Service DNS name."""

    source = os.environ if environ is None else environ
    release_name = source.get('SKYPILOT_RELEASE_NAME')
    namespace = source.get('SKYPILOT_POD_NAMESPACE')
    for name, value in (('SKYPILOT_RELEASE_NAME', release_name),
                        ('SKYPILOT_POD_NAMESPACE', namespace)):
        if (type(value) is not str or _DNS_LABEL_RE.fullmatch(value) is None):
            raise ValueError(f'{name} must be one canonical DNS label.')
    assert release_name is not None
    assert namespace is not None
    service_name = (
        f'{release_name}-{constants.RESOURCE_ACTION_PREFLIGHT_SERVICE_SUFFIX}')
    if _DNS_LABEL_RE.fullmatch(service_name) is None:
        raise ValueError('authority preflight Service name is not a DNS label.')
    result = f'{service_name}.{namespace}.svc'
    if len(result.encode('ascii')) > 253:
        raise ValueError('authority preflight Service DNS name is too long.')
    return result


def _certificate_time(certificate: x509.Certificate,
                      attribute: str) -> datetime.datetime:
    utc_attribute = f'{attribute}_utc'
    value = getattr(certificate, utc_attribute, None)
    if value is not None:
        return value
    legacy = getattr(certificate, attribute)
    return legacy.replace(tzinfo=datetime.timezone.utc)


def _certificate_bundle(contents: bytes, *,
                        name: str) -> tuple[x509.Certificate, ...]:
    blocks = tuple(
        match.group(0) for match in _CERTIFICATE_BLOCK_RE.finditer(contents))
    remainder = _CERTIFICATE_BLOCK_RE.sub(b'', contents)
    if not blocks or remainder.strip():
        raise ValueError(f'{name} is not a strict PEM certificate bundle.')
    try:
        return tuple(x509.load_pem_x509_certificate(block) for block in blocks)
    except ValueError as e:
        raise ValueError(f'{name} contains an invalid certificate.') from e


def _verify_certificate_signature(certificate: x509.Certificate,
                                  issuer: x509.Certificate) -> None:
    public_key = issuer.public_key()
    signature = certificate.signature
    data = certificate.tbs_certificate_bytes
    algorithm = certificate.signature_hash_algorithm
    if isinstance(public_key, rsa.RSAPublicKey):
        parameters = getattr(certificate, 'signature_algorithm_parameters',
                             None)
        if not isinstance(parameters, padding.AsymmetricPadding):
            parameters = padding.PKCS1v15()
        public_key.verify(signature, data, parameters, algorithm)
    elif isinstance(public_key, ec.EllipticCurvePublicKey):
        public_key.verify(signature, data, ec.ECDSA(algorithm))
    elif isinstance(public_key, dsa.DSAPublicKey):
        public_key.verify(signature, data, algorithm)
    elif isinstance(public_key,
                    (ed25519.Ed25519PublicKey, ed448.Ed448PublicKey)):
        public_key.verify(signature, data)
    else:
        raise ValueError('TLS certificate uses an unsupported public key.')


def _require_ca_certificate(certificate: x509.Certificate, *,
                            name: str) -> None:
    try:
        basic = certificate.extensions.get_extension_for_class(
            x509.BasicConstraints).value
    except x509.ExtensionNotFound as e:
        raise ValueError(f'{name} is missing BasicConstraints.') from e
    if not basic.ca:
        raise ValueError(f'{name} is not a CA certificate.')
    try:
        usage = certificate.extensions.get_extension_for_class(
            x509.KeyUsage).value
    except x509.ExtensionNotFound:
        return
    if not usage.key_cert_sign:
        raise ValueError(f'{name} cannot sign certificates.')


def _validate_certificate_material(cert_pem: bytes, key_pem: bytes,
                                   ca_pem: bytes, service_dns: str) -> None:
    chain = _certificate_bundle(cert_pem, name='TLS certificate chain')
    roots = _certificate_bundle(ca_pem, name='TLS CA bundle')
    leaf = chain[0]
    try:
        private_key = serialization.load_pem_private_key(key_pem, password=None)
    except (TypeError, ValueError) as e:
        raise ValueError('TLS private key is invalid or encrypted.') from e
    public_format = serialization.PublicFormat.SubjectPublicKeyInfo
    encoding = serialization.Encoding.DER
    if private_key.public_key().public_bytes(
            encoding, public_format) != (leaf.public_key().public_bytes(
                encoding, public_format)):
        raise ValueError('TLS leaf and private key do not match.')

    now = datetime.datetime.now(datetime.timezone.utc)
    for certificate in (*chain, *roots):
        if (now < _certificate_time(certificate, 'not_valid_before') or
                now > _certificate_time(certificate, 'not_valid_after')):
            raise ValueError('TLS certificate is outside its validity window.')
    try:
        basic = leaf.extensions.get_extension_for_class(
            x509.BasicConstraints).value
        eku = leaf.extensions.get_extension_for_class(
            x509.ExtendedKeyUsage).value
        san = leaf.extensions.get_extension_for_class(
            x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound as e:
        raise ValueError('TLS leaf is missing a required extension.') from e
    if basic.ca:
        raise ValueError('TLS serving leaf must have CA=false.')
    if set(eku) != {ExtendedKeyUsageOID.SERVER_AUTH}:
        raise ValueError('TLS serving leaf requires only serverAuth EKU.')
    general_names = list(san)
    if (len(general_names) != 1 or
            not isinstance(general_names[0], x509.DNSName) or
            general_names[0].value != service_dns):
        raise ValueError(
            'TLS leaf SAN must be the singleton exact Service DNS.')
    for index, issuer in enumerate(chain[1:]):
        _require_ca_certificate(issuer, name=f'TLS chain issuer {index + 1}')
    for root in roots:
        _require_ca_certificate(root, name='TLS CA bundle member')
    chain_fingerprints = tuple(
        certificate.fingerprint(hashes.SHA256()) for certificate in chain)
    root_fingerprints = tuple(
        certificate.fingerprint(hashes.SHA256()) for certificate in roots)
    if (len(set(chain_fingerprints)) != len(chain_fingerprints) or
            len(set(root_fingerprints)) != len(root_fingerprints)):
        raise ValueError('TLS certificate chain or CA bundle has duplicates.')

    current = leaf
    for issuer in chain[1:]:
        if current.issuer != issuer.subject:
            raise ValueError('TLS certificate chain is not ordered.')
        _verify_certificate_signature(current, issuer)
        current = issuer
    trusted = False
    current_fingerprint = current.fingerprint(hashes.SHA256())
    for root in roots:
        if current_fingerprint == root.fingerprint(hashes.SHA256()):
            trusted = True
        elif current.issuer == root.subject:
            try:
                _verify_certificate_signature(current, root)
            except Exception:  # pylint: disable=broad-except
                continue
            trusted = True
    if not trusted:
        raise ValueError('TLS chain does not terminate in the purpose CA.')


def _read_generation_file(directory_descriptor: int,
                          name: str) -> tuple[int, bytes, os.stat_result]:
    descriptor = os.open(name,
                         os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW |
                         os.O_NONBLOCK,
                         dir_fd=directory_descriptor)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_size < 1 or
                before.st_size > _MAX_TLS_FILE_BYTES):
            raise ValueError('TLS generation member is not a bounded regular '
                             'file.')
        contents = bytearray()
        while len(contents) < before.st_size:
            chunk = os.read(descriptor, before.st_size - len(contents))
            if not chunk:
                raise ValueError('TLS generation member changed while read.')
            contents.extend(chunk)
        if os.read(descriptor, 1):
            raise ValueError('TLS generation member grew while read.')
        after = os.fstat(descriptor)
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
             before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size,
                                     after.st_mtime_ns, after.st_ctime_ns)):
            raise ValueError('TLS generation member changed while read.')
        # Keep the descriptor open so SSLContext loads the same generation.
        return descriptor, bytes(contents), after
    except Exception:
        os.close(descriptor)
        raise


def _load_tls_generation(
        tls_directory: str,
        service_dns: str) -> tuple[ssl.SSLContext, tuple[Any, ...]]:
    required_flags = ('O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, flag) for flag in required_flags):
        raise ValueError('Descriptor-safe TLS rotation is unsupported.')
    directory_flags = (os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC |
                       os.O_NOFOLLOW)
    root_descriptor = os.open(tls_directory, directory_flags)
    generation_descriptor = -1
    descriptors: list[int] = []
    try:
        # Kubernetes's one constrained symlink hop selects an immutable
        # atomic-writer generation. Resolve only one child basename and reopen
        # it with O_NOFOLLOW so a relative escape cannot become the trust root.
        generation_name: str
        data_stat = os.stat('..data',
                            dir_fd=root_descriptor,
                            follow_symlinks=False)
        if stat.S_ISLNK(data_stat.st_mode):
            generation_name = os.readlink('..data', dir_fd=root_descriptor)
            if (not generation_name.startswith('..') or
                    generation_name in ('..', '..data') or
                    '/' in generation_name or '\\' in generation_name):
                raise ValueError(
                    'TLS projected generation symlink target is invalid.')
        elif stat.S_ISDIR(data_stat.st_mode):
            # Local immutable fixtures use a directory; projected Secrets use
            # the constrained symlink form above.
            generation_name = '..data'
        else:
            raise ValueError('TLS projected generation is not a directory.')
        generation_descriptor = os.open(generation_name,
                                        directory_flags,
                                        dir_fd=root_descriptor)
        generation_stat = os.fstat(generation_descriptor)
        cert_fd, cert_pem, cert_stat = _read_generation_file(
            generation_descriptor, 'tls.crt')
        descriptors.append(cert_fd)
        key_fd, key_pem, key_stat = _read_generation_file(
            generation_descriptor, 'tls.key')
        descriptors.append(key_fd)
        ca_fd, ca_pem, ca_stat = _read_generation_file(generation_descriptor,
                                                       'ca.crt')
        descriptors.append(ca_fd)
        _validate_certificate_material(cert_pem, key_pem, ca_pem, service_dns)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_NONE
        context.set_alpn_protocols(['http/1.1'])
        context.options |= getattr(ssl, 'OP_NO_COMPRESSION', 0)
        context.load_cert_chain(f'/proc/self/fd/{cert_fd}',
                                f'/proc/self/fd/{key_fd}')
        signature: tuple[Any, ...] = (
            generation_stat.st_dev, generation_stat.st_ino,
            *(value for item in (cert_stat, key_stat, ca_stat)
              for value in (item.st_dev, item.st_ino, item.st_size,
                            item.st_mtime_ns, item.st_ctime_ns)))
        return context, signature
    finally:
        for descriptor in descriptors:
            os.close(descriptor)
        if generation_descriptor >= 0:
            os.close(generation_descriptor)
        os.close(root_descriptor)


class _BufferedSocket:
    """Deadline-bound buffered reader for one preflight TLS connection."""

    def __init__(self, connection: ssl.SSLSocket, deadline: float) -> None:
        self._connection = connection
        self._deadline = deadline
        self._buffer = bytearray()

    def _receive(self) -> None:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise _HTTPFailure(408)
        self._connection.settimeout(remaining)
        try:
            chunk = self._connection.recv(8_192)
        except TimeoutError as e:
            raise _HTTPFailure(408) from e
        if not chunk:
            raise _HTTPFailure(408)
        self._buffer.extend(chunk)

    def readline(self, maximum_bytes: int) -> bytes:
        while True:
            newline = self._buffer.find(b'\n')
            if newline >= 0:
                if newline + 1 > maximum_bytes:
                    raise _HTTPFailure(431)
                result = bytes(self._buffer[:newline + 1])
                del self._buffer[:newline + 1]
                return result
            if len(self._buffer) >= maximum_bytes:
                raise _HTTPFailure(431)
            self._receive()

    def readexactly(self, count: int) -> bytes:
        while len(self._buffer) < count:
            self._receive()
        result = bytes(self._buffer[:count])
        del self._buffer[:count]
        return result

    @property
    def has_buffered_bytes(self) -> bool:
        return bool(self._buffer)


def _parse_canonical_request(
        body: bytes) -> resource_actions.ProviderAuthorityPreflightRequestV1:

    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result

    def _forbid_float(_: str) -> Any:
        raise ValueError('floating-point JSON is forbidden')

    try:
        value = json.loads(body.decode('utf-8'),
                           object_pairs_hook=_object,
                           parse_float=_forbid_float,
                           parse_constant=_forbid_float)
        request = (resource_actions.ProviderAuthorityPreflightRequestV1.
                   from_value(value))
        if request.canonical_bytes != body:
            raise ValueError('request body is not canonical')
        return request
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError,
            ValueError) as e:
        raise _HTTPFailure(400) from e


class AuthorityPreflightServer:
    """Fixed-pool, one-request-per-connection private TLS server."""

    def __init__(
        self,
        host: str,
        port: int,
        service_dns: str,
        evaluator: Callable[
            [resource_actions.ProviderAuthorityPreflightRequestV1],
            resource_actions.ProviderAuthorityPreflightResponseV1 | None],
        *,
        on_transport_invalid: Callable[[], None],
        tls_directory: str = constants.RESOURCE_ACTION_PREFLIGHT_TLS_DIRECTORY,
    ) -> None:
        if type(host) is not str or not host:
            raise ValueError('authority preflight host must be nonempty text.')
        if type(port) is not int or isinstance(port,
                                               bool) or not 0 <= port <= 65_535:
            raise ValueError('authority preflight port is invalid.')
        if type(service_dns) is not str or not service_dns:
            raise ValueError('authority preflight Service DNS is invalid.')
        if not callable(evaluator) or not callable(on_transport_invalid):
            raise TypeError('authority preflight callbacks must be callable.')
        self._host = host
        self._requested_port = port
        self._service_dns = service_dns
        self._evaluator = evaluator
        self._on_transport_invalid = on_transport_invalid
        self._tls_directory = tls_directory
        self._listener: socket.socket | None = None
        self._bound_port = port
        self._stop_event = threading.Event()
        self._accept_thread: threading.Thread | None = None
        self._watch_thread: threading.Thread | None = None
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._slots = threading.BoundedSemaphore(_WORKER_COUNT)
        self._future_lock = threading.Lock()
        self._futures: set[concurrent.futures.Future[None]] = set()
        self._connection_lock = threading.Lock()
        self._connections: set[socket.socket] = set()
        self._context_lock = threading.Lock()
        self._tls_context: ssl.SSLContext | None = None
        self._tls_signature: tuple[Any, ...] | None = None
        self._invalid_notified = False

    @property
    def bound_port(self) -> int:
        return self._bound_port

    def start(self) -> None:
        if self._listener is not None:
            raise RuntimeError('authority preflight server is already started.')
        self._stop_event.clear()
        self._refresh_tls_context()
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self._host, self._requested_port))
        listener.listen(_LISTEN_BACKLOG)
        listener.settimeout(0.25)
        self._listener = listener
        self._bound_port = int(listener.getsockname()[1])
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=_WORKER_COUNT, thread_name_prefix='authority-preflight')
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name='authority-preflight-accept',
            daemon=True)
        self._watch_thread = threading.Thread(target=self._watch_tls,
                                              name='authority-preflight-tls',
                                              daemon=True)
        self._accept_thread.start()
        self._watch_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        # Bootstrap/acceptance clears as soon as shutdown stops admission, but
        # the immutable SSLContext remains available to already admitted work
        # during its one bounded request deadline.
        self._notify_invalid()
        for thread in (self._accept_thread, self._watch_thread):
            if thread is not None:
                thread.join(timeout=_REQUEST_DEADLINE_SECONDS)
        self._accept_thread = None
        self._watch_thread = None
        with self._future_lock:
            futures = tuple(self._futures)
        if futures:
            concurrent.futures.wait(futures, timeout=_REQUEST_DEADLINE_SECONDS)
        # A faulty evaluator cannot extend shutdown beyond the protocol
        # deadline. Closing its admitted socket also prevents a late response.
        with self._connection_lock:
            connections = tuple(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass
        self._clear_transport()
        executor = self._executor
        self._executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def is_transport_ready(self) -> bool:
        with self._context_lock:
            ready = self._tls_context is not None
        if not ready or self._listener is None or self._stop_event.is_set():
            return False
        try:
            auth_tokens.get_isolated_resource_action_preflight_auth_tokens(
                required=True)
        except (TypeError, ValueError):
            self._notify_invalid()
            return False
        return True

    def _notify_invalid(self) -> None:
        notify = False
        with self._context_lock:
            if not self._invalid_notified:
                self._invalid_notified = True
                notify = True
        if notify:
            try:
                self._on_transport_invalid()
            except Exception:  # pylint: disable=broad-except
                pass

    def _clear_transport(self) -> None:
        with self._context_lock:
            self._tls_context = None
            self._tls_signature = None
        self._notify_invalid()

    def _refresh_tls_context(self) -> None:
        try:
            context, signature = _load_tls_generation(self._tls_directory,
                                                      self._service_dns)
        except Exception:  # pylint: disable=broad-except
            self._clear_transport()
            return
        with self._context_lock:
            self._tls_context = context
            self._tls_signature = signature
            self._invalid_notified = False

    def _watch_tls(self) -> None:
        while not self._stop_event.wait(_TLS_POLL_SECONDS):
            self._refresh_tls_context()

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            listener = self._listener
            executor = self._executor
            if listener is None or executor is None:
                return
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop_event.is_set():
                    return
                continue
            if not self._slots.acquire(blocking=False):
                try:
                    connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                          struct.pack('ii', 1, 0))
                except OSError:
                    pass
                connection.close()
                continue
            try:
                future = executor.submit(self._serve_connection, connection)
                with self._future_lock:
                    self._futures.add(future)
                future.add_done_callback(self._future_completed)
            except RuntimeError:
                connection.close()
                self._slots.release()

    def _future_completed(self,
                          future: concurrent.futures.Future[None]) -> None:
        with self._future_lock:
            self._futures.discard(future)

    def _serve_connection(self, connection: socket.socket) -> None:
        tls_connection: ssl.SSLSocket | None = None
        deadline = time.monotonic() + _REQUEST_DEADLINE_SECONDS
        with self._connection_lock:
            self._connections.add(connection)
        try:
            with self._context_lock:
                context = self._tls_context
            if context is None:
                connection.close()
                return
            connection.settimeout(max(0.001, deadline - time.monotonic()))
            tls_connection = context.wrap_socket(connection,
                                                 server_side=True,
                                                 do_handshake_on_connect=True)
            with self._connection_lock:
                self._connections.add(tls_connection)
            if tls_connection.selected_alpn_protocol() != 'http/1.1':
                return
            self._handle_http(tls_connection, deadline)
        except (OSError, ssl.SSLError):
            pass
        except Exception:  # pylint: disable=broad-except
            if tls_connection is not None:
                self._send_error(tls_connection, 503, deadline)
        finally:
            candidate = tls_connection if tls_connection is not None else connection
            try:
                candidate.close()
            except OSError:
                pass
            with self._connection_lock:
                self._connections.discard(connection)
                if tls_connection is not None:
                    self._connections.discard(tls_connection)
            self._slots.release()

    def _handle_http(self, connection: ssl.SSLSocket, deadline: float) -> None:
        reader = _BufferedSocket(connection, deadline)
        try:
            try:
                request_line = reader.readline(_MAX_REQUEST_LINE_BYTES)
            except _HTTPFailure as failure:
                if failure.status == 431:
                    raise _HTTPFailure(400) from failure
                raise
            if not request_line.endswith(b'\r\n'):
                raise _HTTPFailure(400)
            try:
                method, target, version = request_line[:-2].decode(
                    'ascii').split(' ')
            except (UnicodeDecodeError, ValueError) as e:
                raise _HTTPFailure(400) from e
            if version != 'HTTP/1.1':
                raise _HTTPFailure(400)
            if method != 'POST':
                raise _HTTPFailure(405)
            if target != constants.RESOURCE_ACTION_PREFLIGHT_PATH:
                raise _HTTPFailure(404)

            headers: dict[str, str] = {}
            lower_names: set[str] = set()
            aggregate = 0
            count = 0
            while True:
                line = reader.readline(_MAX_HEADER_LINE_BYTES)
                aggregate += len(line)
                if aggregate > _MAX_AGGREGATE_HEADER_BYTES:
                    raise _HTTPFailure(431)
                if line == b'\r\n':
                    break
                if not line.endswith(b'\r\n'):
                    raise _HTTPFailure(400)
                count += 1
                if count > _MAX_HEADER_COUNT:
                    raise _HTTPFailure(431)
                try:
                    name_bytes, value_bytes = line[:-2].split(b':', 1)
                    name = name_bytes.decode('ascii')
                    value = value_bytes.decode('ascii')
                except (UnicodeDecodeError, ValueError) as e:
                    raise _HTTPFailure(400) from e
                if (not value.startswith(' ') or value.startswith('  ') or
                        value.endswith(' ')):
                    raise _HTTPFailure(400)
                value = value[1:]
                lower_name = name.lower()
                if (not name or lower_name in lower_names or
                        lower_name in _FORBIDDEN_HEADER_NAMES):
                    raise _HTTPFailure(400)
                lower_names.add(lower_name)
                headers[name] = value
            if 'Content-Length' not in headers:
                raise _HTTPFailure(411)
            if set(headers) != _EXPECTED_HEADER_NAMES:
                raise _HTTPFailure(400)
            if headers['Content-Type'] != 'application/json':
                raise _HTTPFailure(415)
            if (headers['Accept'] != 'application/json' or
                    headers['Accept-Encoding'] != 'identity' or
                    headers['Connection'] != 'close' or headers['Host']
                    != f'{self._service_dns}:{self._bound_port}'):
                raise _HTTPFailure(400)
            content_length_text = headers['Content-Length']
            if (not content_length_text.isascii() or
                    not content_length_text.isdecimal() or
                    content_length_text.startswith('0')):
                raise _HTTPFailure(400)
            maximum_length_text = str(_MAX_BODY_BYTES)
            if (len(content_length_text) > len(maximum_length_text) or
                (len(content_length_text) == len(maximum_length_text) and
                 content_length_text > maximum_length_text)):
                raise _HTTPFailure(413)
            content_length = int(content_length_text)

            authorization = headers['Authorization']
            candidate_token = (authorization[len('Bearer '):]
                               if authorization.startswith('Bearer ') else '')
            try:
                expected_tokens = (
                    auth_tokens.
                    get_isolated_resource_action_preflight_auth_tokens(
                        required=True))
            except (TypeError, ValueError):
                self._notify_invalid()
                raise _HTTPFailure(503) from None
            matched = 0
            for expected_token in expected_tokens:
                matched |= int(
                    hmac.compare_digest(candidate_token, expected_token))
            if not matched:
                raise _HTTPFailure(401)

            body = reader.readexactly(content_length)
            if reader.has_buffered_bytes:
                raise _HTTPFailure(400)
            if connection.pending() > 0:
                raise _HTTPFailure(400)
            request = _parse_canonical_request(body)
            if time.monotonic() >= deadline:
                raise _HTTPFailure(408)
            response = self._evaluator(request)
            if time.monotonic() >= deadline:
                raise _HTTPFailure(408)
            if response is None:
                raise _HTTPFailure(503)
            if type(response) not in (
                    resource_actions.ProviderLaunchAuthorityPreflightResponseV1,
                    resource_actions.ProviderDownAuthorityPreflightResponseV1):
                raise _HTTPFailure(503)
            response.validate_request(request)
            self._send_response(connection, 200, response.canonical_bytes,
                                deadline)
        except _HTTPFailure as failure:
            self._send_error(connection, failure.status, deadline)

    @staticmethod
    def _send_error(connection: ssl.SSLSocket, status: int,
                    deadline: float) -> None:
        reason, code = _ERRORS[status]
        body = resource_actions.canonical_json_bytes({
            'version': 1,
            'code': code,
        })
        AuthorityPreflightServer._send_response(connection,
                                                status,
                                                body,
                                                deadline,
                                                reason=reason)

    @staticmethod
    def _send_response(connection: ssl.SSLSocket,
                       status: int,
                       body: bytes,
                       deadline: float,
                       *,
                       reason: str = 'OK') -> None:
        if len(body) > _MAX_BODY_BYTES:
            status = 503
            reason, code = _ERRORS[status]
            body = resource_actions.canonical_json_bytes({
                'version': 1,
                'code': code,
            })
        headers = [
            f'HTTP/1.1 {status} {reason}',
            'Content-Type: application/json',
            f'Content-Length: {len(body)}',
            'Cache-Control: no-store',
            'X-Content-Type-Options: nosniff',
            'Connection: close',
        ]
        if status == 401:
            headers.append('WWW-Authenticate: Bearer')
        elif status == 405:
            headers.append('Allow: POST')
        encoded = ('\r\n'.join(headers) + '\r\n\r\n').encode('ascii') + body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        connection.settimeout(remaining)
        try:
            connection.sendall(encoded)
        except (OSError, ssl.SSLError):
            pass


__all__ = ['AuthorityPreflightServer', 'authority_preflight_service_dns']
