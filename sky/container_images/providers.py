"""Credential-confined OCI source reader used by image copy workers."""

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
import dataclasses
import hashlib
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

requests = adaptor_common.LazyImport(
    'requests',
    import_error_message='Container image workers require requests.')


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
        self._session = requests.Session()

    def _url(self, suffix: str) -> str:
        return f'https://{self.authority}/v2/{self.repository}/{suffix}'

    def _credentials(self) -> SourceCredentials | None:
        credentials = self._credential_resolver()
        if credentials is not None and not isinstance(credentials,
                                                      SourceCredentials):
            raise TypeError('Source credential resolver returned invalid data.')
        return credentials

    def _request(self,
                 method: str,
                 url: str,
                 *,
                 headers: dict[str, str] | None = None,
                 stream: bool = False) -> Any:
        credentials = self._credentials()
        request_headers = dict(headers or {})
        auth = None
        if credentials is not None:
            if credentials.bearer_token is not None:
                request_headers['Authorization'] = (
                    f'Bearer {credentials.bearer_token}')
            elif credentials.username is not None:
                auth = (credentials.username, credentials.password)
        response = self._session.request(method,
                                         url,
                                         headers=request_headers,
                                         auth=auth,
                                         timeout=self._timeout_seconds,
                                         stream=stream,
                                         allow_redirects=False)
        if response.status_code == 401:
            challenge = response.headers.get('WWW-Authenticate', '')
            if not challenge.lower().startswith('bearer '):
                response.raise_for_status()
            challenge_fields = requests.utils.parse_dict_header(challenge[7:])
            realm = challenge_fields.get('realm')
            service = challenge_fields.get('service')
            scope = challenge_fields.get('scope')
            if not isinstance(realm, str) or not isinstance(service, str):
                raise ValueError('Registry bearer challenge is incomplete.')
            parsed = urllib.parse.urlsplit(realm)
            if parsed.scheme != 'https' or not parsed.netloc:
                raise ValueError('Registry bearer challenge must use HTTPS.')
            params = {'service': service}
            if scope is not None:
                params['scope'] = scope
            token_response = self._session.get(realm,
                                               params=params,
                                               auth=auth,
                                               timeout=self._timeout_seconds)
            token_response.raise_for_status()
            token_payload = token_response.json()
            token = token_payload.get('token',
                                      token_payload.get('access_token'))
            if not isinstance(token, str) or not token:
                raise ValueError('Registry token response contained no token.')
            request_headers['Authorization'] = f'Bearer {token}'
            response = self._session.request(method,
                                             url,
                                             headers=request_headers,
                                             timeout=self._timeout_seconds,
                                             stream=stream,
                                             allow_redirects=False)
        # Blob redirects may point to a signed HTTPS object URL. Never forward
        # source Authorization outside the registry authority.
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location')
            if not isinstance(location, str):
                raise ValueError('Registry redirect has no destination.')
            redirected = urllib.parse.urljoin(url, location)
            parsed = urllib.parse.urlsplit(redirected)
            if parsed.scheme != 'https' or not parsed.netloc:
                raise ValueError('Registry blob redirect must use HTTPS.')
            response = self._session.request(method,
                                             redirected,
                                             timeout=self._timeout_seconds,
                                             stream=stream,
                                             allow_redirects=False)
        response.raise_for_status()
        return response

    def read_manifest(self, digest: str) -> bytes:
        digest = models.validate_sha256_digest(digest, 'OCI manifest digest')
        response = self._request('GET',
                                 self._url(f'manifests/{digest}'),
                                 headers={'Accept': _REGISTRY_ACCEPT})
        payload = bytes(response.content)
        if f'sha256:{hashlib.sha256(payload).hexdigest()}' != digest:
            raise ValueError('Source registry manifest digest mismatch.')
        return payload

    def read_root(self) -> bytes:
        return self.read_manifest(self.digest)

    def read_blob(self, descriptor: oci.OciDescriptor) -> Iterable[bytes]:
        response = self._request('GET',
                                 self._url(f'blobs/{descriptor.digest}'),
                                 stream=True)

        def chunks() -> Iterator[bytes]:
            try:
                yield from response.iter_content(chunk_size=1024 * 1024)
            finally:
                response.close()

        return chunks()

    def read_blob_bytes(self, digest: str, *, max_bytes: int) -> bytes:
        digest = models.validate_sha256_digest(digest, 'OCI blob digest')
        response = self._request('GET',
                                 self._url(f'blobs/{digest}'),
                                 stream=True)
        payload = bytearray()
        try:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                payload.extend(chunk)
                if len(payload) > max_bytes:
                    raise ValueError(
                        'OCI source blob exceeds inspection limit.')
        finally:
            response.close()
        return bytes(payload)
