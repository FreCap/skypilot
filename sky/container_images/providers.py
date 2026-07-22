"""Credential-confined OCI source reader used by image copy workers."""

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
import dataclasses
import functools
import hashlib
import ipaddress
import json
from typing import Any
import urllib.parse

from sky.adaptors import common as adaptor_common
from sky.container_images import models
from sky.container_images import oci

_REGISTRY_ACCEPT = ', '.join([
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
])
_MAX_TOKEN_RESPONSE_BYTES = 64 * 1024
_DEFAULT_MAX_MANIFEST_BYTES = 4 * 1024 * 1024

requests = adaptor_common.LazyImport(
    'requests',
    import_error_message='Container image workers require requests.')


def _require_public_network_address(value: str) -> None:
    """Rejects every address that is not globally routable public space."""
    try:
        address = ipaddress.ip_address(value.split('%', 1)[0])
    except ValueError:
        return
    if not address.is_global:
        raise ValueError('OCI source network destination is not public.')


def validate_public_https_destination(url: str, subject: str) -> str:
    """Returns one normalized HTTPS authority after local literal rejection."""
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or
            parsed.password or parsed.fragment):
        raise ValueError(f'{subject} must be a credential-free HTTPS URL.')
    hostname = parsed.hostname.rstrip('.').lower()
    if hostname == 'localhost' or hostname.endswith('.localhost'):
        raise ValueError('OCI source network destination is not public.')
    _require_public_network_address(hostname)
    try:
        return models.normalize_registry_authority(parsed.netloc, subject)
    except ValueError:
        raise ValueError(f'{subject} has an invalid HTTPS authority.') from None


def _read_bounded_response(
    response: Any,
    *,
    max_bytes: int,
    subject: str,
    provider_fence: Callable[[], None] | None = None,
) -> bytes:
    """Reads and closes a response without allowing an eager unbounded body."""
    try:
        content_length = response.headers.get('Content-Length')
        if content_length is not None:
            declared_length: int
            try:
                declared_length = int(content_length)
            except ValueError:
                raise ValueError(
                    f'{subject} has an invalid Content-Length.') from None
            if declared_length < 0:
                raise ValueError(f'{subject} has an invalid Content-Length.')
            if declared_length > max_bytes:
                raise ValueError(f'{subject} exceeds the size limit.')
        payload = bytearray()
        if provider_fence is not None:
            provider_fence()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if provider_fence is not None:
                provider_fence()
            payload.extend(chunk)
            if len(payload) > max_bytes:
                raise ValueError(f'{subject} exceeds the size limit.')
        return bytes(payload)
    finally:
        response.close()


@functools.lru_cache(maxsize=1)
def _guarded_https_adapter_type() -> type[Any]:
    """Builds a Requests adapter that checks the peer before TLS or HTTP."""
    urllib3 = requests.packages.urllib3
    urllib3_pool = urllib3.connectionpool

    class GuardedHttpsConnection(
            urllib3.connection.HTTPSConnection  # type: ignore[name-defined]
    ):
        """HTTPS connection that rejects a non-public connected peer."""

        def _new_conn(self) -> Any:
            _require_public_network_address(str(self.host))
            connection = super()._new_conn()
            try:
                _require_public_network_address(str(
                    connection.getpeername()[0]))
            except Exception:
                connection.close()
                raise
            return connection

    class GuardedHttpsPool(
            urllib3_pool.HTTPSConnectionPool  # type: ignore[name-defined]
    ):
        """HTTPS pool that creates only guarded connections."""

        ConnectionCls = GuardedHttpsConnection

    class GuardedHttpsAdapter(
            requests.adapters.HTTPAdapter  # type: ignore[name-defined]
    ):
        """Requests adapter that installs the guarded pool and blocks proxies."""

        def init_poolmanager(self,
                             connections: int,
                             maxsize: int,
                             block: bool = False,
                             **pool_kwargs: Any) -> None:
            super().init_poolmanager(connections,
                                     maxsize,
                                     block=block,
                                     **pool_kwargs)
            pool_classes = dict(self.poolmanager.pool_classes_by_scheme)
            pool_classes['https'] = GuardedHttpsPool
            self.poolmanager.pool_classes_by_scheme = pool_classes

        def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
            del proxy, proxy_kwargs
            raise ValueError('OCI source readers do not permit HTTP proxies.')

    return GuardedHttpsAdapter


def guarded_https_session() -> Any:
    """Returns a no-proxy HTTPS session with connected-peer validation."""
    session = requests.Session()
    # Environment proxy configuration must not bypass the peer-address guard
    # or become another credential-bearing trust boundary.
    session.trust_env = False
    session.mount('https://', _guarded_https_adapter_type()())
    return session


@dataclasses.dataclass(frozen=True)
class SourceCredentials:
    """Ephemeral source credentials that refuse serialization and display."""

    username: str | None = dataclasses.field(default=None, repr=False)
    password: str | None = dataclasses.field(default=None, repr=False)
    bearer_token: str | None = dataclasses.field(default=None, repr=False)

    def __post_init__(self) -> None:
        basic = self.username is not None or self.password is not None
        if basic and (self.username is None or self.password is None):
            raise ValueError('Source basic credentials require both values.')
        if basic and self.bearer_token is not None:
            raise ValueError('Source credentials must select one auth method.')

    def __getstate__(self) -> Any:
        raise TypeError('Source registry credentials must not be serialized.')


class RegistryV2Source:
    """Digest-only OCI Distribution source with bounded bearer negotiation."""

    def __init__(
        self,
        reference: str,
        credential_resolver: Callable[[], SourceCredentials | None],
        *,
        timeout_seconds: int = 60,
        provider_fence: Callable[[], None] | None = None,
    ) -> None:
        reference = models.validate_oci_reference(reference,
                                                  'OCI source reference')
        repository, digest = models.split_digest(reference)
        if digest is None:
            raise ValueError('OCI publication source must be digest-pinned.')
        first, separator, path = repository.partition('/')
        if not separator or not path or not ('.' in first or ':' in first or
                                             first == 'localhost' or
                                             first.startswith('[')):
            authority = 'registry-1.docker.io'
            path = (repository
                    if '/' in repository else f'library/{repository}')
        else:
            authority = models.normalize_registry_authority(first, 'OCI source')
        self.reference = reference
        self.authority = authority
        self.repository = path
        self.digest = digest
        self._credential_resolver = credential_resolver
        self._timeout_seconds = timeout_seconds
        self._provider_fence = provider_fence
        self._session = guarded_https_session()
        validate_public_https_destination(f'https://{self.authority}',
                                          'OCI source registry')

    def _fence(self) -> None:
        if self._provider_fence is not None:
            self._provider_fence()

    def _fence_response(self, response: Any) -> None:
        try:
            self._fence()
        except BaseException:
            response.close()
            raise

    def _url(self, suffix: str) -> str:
        return f'https://{self.authority}/v2/{self.repository}/{suffix}'

    def _credentials(self) -> SourceCredentials | None:
        self._fence()
        credentials = self._credential_resolver()
        self._fence()
        if credentials is not None and not isinstance(credentials,
                                                      SourceCredentials):
            raise TypeError('Source credential resolver returned invalid data.')
        return credentials

    def _request(self,
                 method: str,
                 url: str,
                 *,
                 headers: dict[str, str] | None = None,
                 stream: bool = False,
                 allow_blob_redirect: bool = False) -> Any:
        credentials = self._credentials()
        request_headers = dict(headers or {})
        auth = None
        if credentials is not None:
            if credentials.bearer_token is not None:
                request_headers['Authorization'] = (
                    f'Bearer {credentials.bearer_token}')
            elif credentials.username is not None:
                auth = (credentials.username, credentials.password)
        self._fence()
        response = self._session.request(method,
                                         url,
                                         headers=request_headers,
                                         auth=auth,
                                         timeout=self._timeout_seconds,
                                         stream=stream,
                                         allow_redirects=False)
        self._fence_response(response)
        if response.status_code == 401:
            challenge = response.headers.get('WWW-Authenticate', '')
            if not challenge.lower().startswith('bearer '):
                try:
                    response.raise_for_status()
                finally:
                    response.close()
            challenge_fields = requests.utils.parse_dict_header(challenge[7:])
            response.close()
            realm = challenge_fields.get('realm')
            service = challenge_fields.get('service')
            scope = challenge_fields.get('scope')
            if not isinstance(realm, str) or not isinstance(service, str):
                raise ValueError('Registry bearer challenge is incomplete.')
            realm_authority = validate_public_https_destination(
                realm, 'Registry bearer challenge')
            if auth is not None and realm_authority != self.authority:
                raise ValueError(
                    'Registry basic credentials cannot cross authorities.')
            params = {'service': service}
            if scope is not None:
                params['scope'] = scope
            self._fence()
            token_response = self._session.get(realm,
                                               params=params,
                                               auth=auth,
                                               timeout=self._timeout_seconds,
                                               stream=True,
                                               allow_redirects=False)
            self._fence_response(token_response)
            if token_response.status_code in (301, 302, 303, 307, 308):
                token_response.close()
                raise ValueError('Registry token redirects are not allowed.')
            try:
                token_response.raise_for_status()
            except Exception:
                token_response.close()
                raise
            try:
                token_payload = json.loads(
                    _read_bounded_response(token_response,
                                           max_bytes=_MAX_TOKEN_RESPONSE_BYTES,
                                           subject='Registry token response',
                                           provider_fence=self._fence))
            except json.JSONDecodeError:
                raise ValueError(
                    'Registry token response is not valid JSON.') from None
            if not isinstance(token_payload, dict):
                raise ValueError(
                    'Registry token response must be a JSON object.')
            token = token_payload.get('token',
                                      token_payload.get('access_token'))
            if not isinstance(token, str) or not token:
                raise ValueError('Registry token response contained no token.')
            request_headers['Authorization'] = f'Bearer {token}'
            self._fence()
            response = self._session.request(method,
                                             url,
                                             headers=request_headers,
                                             timeout=self._timeout_seconds,
                                             stream=stream,
                                             allow_redirects=False)
            self._fence_response(response)
        # Blob redirects may point to a signed HTTPS object URL. Never forward
        # source Authorization outside the registry authority.
        if response.status_code in (301, 302, 303, 307, 308):
            if not allow_blob_redirect:
                response.close()
                raise ValueError('Registry manifest redirects are not allowed.')
            location = response.headers.get('Location')
            if not isinstance(location, str):
                response.close()
                raise ValueError('Registry redirect has no destination.')
            redirected = urllib.parse.urljoin(url, location)
            response.close()
            validate_public_https_destination(redirected,
                                              'Registry blob redirect')
            self._fence()
            response = self._session.request(method,
                                             redirected,
                                             timeout=self._timeout_seconds,
                                             stream=stream,
                                             allow_redirects=False)
            self._fence_response(response)
            if response.status_code in (301, 302, 303, 307, 308):
                response.close()
                raise ValueError('Registry redirect chains are not allowed.')
        try:
            response.raise_for_status()
        except Exception:
            response.close()
            raise
        self._fence_response(response)
        return response

    def read_manifest(self,
                      digest: str,
                      *,
                      max_bytes: int = _DEFAULT_MAX_MANIFEST_BYTES) -> bytes:
        digest = models.validate_sha256_digest(digest, 'OCI manifest digest')
        response = self._request('GET',
                                 self._url(f'manifests/{digest}'),
                                 headers={'Accept': _REGISTRY_ACCEPT},
                                 stream=True)
        payload = _read_bounded_response(response,
                                         max_bytes=max_bytes,
                                         subject='OCI source manifest',
                                         provider_fence=self._fence)
        if f'sha256:{hashlib.sha256(payload).hexdigest()}' != digest:
            raise ValueError('Source registry manifest digest mismatch.')
        return payload

    def read_root(self,
                  *,
                  max_bytes: int = _DEFAULT_MAX_MANIFEST_BYTES) -> bytes:
        return self.read_manifest(self.digest, max_bytes=max_bytes)

    def read_blob(self, descriptor: oci.OciDescriptor) -> Iterable[bytes]:
        response = self._request('GET',
                                 self._url(f'blobs/{descriptor.digest}'),
                                 stream=True,
                                 allow_blob_redirect=True)

        def chunks() -> Iterator[bytes]:
            try:
                self._fence()
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    self._fence()
                    yield chunk
            finally:
                response.close()

        return chunks()

    def read_blob_bytes(self, digest: str, *, max_bytes: int) -> bytes:
        digest = models.validate_sha256_digest(digest, 'OCI blob digest')
        response = self._request('GET',
                                 self._url(f'blobs/{digest}'),
                                 stream=True,
                                 allow_blob_redirect=True)
        payload = bytearray()
        try:
            self._fence()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                self._fence()
                payload.extend(chunk)
                if len(payload) > max_bytes:
                    raise ValueError(
                        'OCI source blob exceeds inspection limit.')
        finally:
            response.close()
        return bytes(payload)
