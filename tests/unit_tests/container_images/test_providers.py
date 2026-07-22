"""Network and credential boundaries for OCI source readers."""
# pylint: disable=protected-access

import json
import socket
import threading
from unittest import mock

import pytest

from sky.container_images import providers

_DIGEST = 'sha256:' + 'a' * 64


@pytest.mark.parametrize('authority', [
    'localhost:8443',
    '127.0.0.1:8443',
    '10.0.0.1:8443',
    '169.254.169.254:443',
    '[::1]:8443',
    '[fe80::1]:443',
])
def test_source_reader_rejects_non_public_literal_authorities(
        authority: str) -> None:
    reference = f'{authority}/example/image@{_DIGEST}'
    with pytest.raises(ValueError, match='network destination is not public'):
        providers.RegistryV2Source(reference, lambda: None)


def test_source_reader_rejects_rebound_private_peer_before_tls(
        monkeypatch: pytest.MonkeyPatch) -> None:
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    listener.settimeout(5)
    port = listener.getsockname()[1]
    received: list[bytes] = []

    def accept() -> None:
        connection, _ = listener.accept()
        connection.settimeout(2)
        received.append(connection.recv(16))
        connection.close()
        listener.close()

    server = threading.Thread(target=accept)
    server.start()
    original = socket.getaddrinfo

    def rebound(host, *args, **kwargs):
        if host == 'source.invalid':
            host = '127.0.0.1'
        return original(host, *args, **kwargs)

    monkeypatch.setattr(socket, 'getaddrinfo', rebound)
    source = providers.RegistryV2Source(
        f'source.invalid:{port}/example/image@{_DIGEST}',
        lambda: None,
        timeout_seconds=2)
    with pytest.raises(ValueError, match='network destination is not public'):
        source.read_root()
    server.join(timeout=5)
    assert not server.is_alive()
    # The peer is checked immediately after TCP connect. No TLS handshake,
    # HTTP path, header, or credential reaches the private endpoint.
    assert received == [b'']


def test_source_reader_disables_proxy_inheritance() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}', lambda: None)

    assert not source._session.trust_env  # pylint: disable=protected-access
    adapter = source._session.get_adapter(  # pylint: disable=protected-access
        'https://registry.example')
    with pytest.raises(ValueError, match='do not permit HTTP proxies'):
        adapter.proxy_manager_for('http://127.0.0.1:8080')


def test_source_reader_fences_before_resolving_credentials() -> None:
    resolver = mock.Mock()
    fence = mock.Mock(side_effect=RuntimeError('source lease lost'))
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}',
        resolver,
        provider_fence=fence)
    source._session = mock.Mock()  # pylint: disable=protected-access

    with pytest.raises(RuntimeError, match='source lease lost'):
        source.read_root()

    resolver.assert_not_called()
    source._session.request.assert_not_called()  # pylint: disable=protected-access


def test_source_reader_closes_response_when_lease_is_lost_after_request(
) -> None:
    response = mock.Mock(status_code=200, headers={})
    fence = mock.Mock(
        side_effect=[None, None, None,
                     RuntimeError('source lease lost')])
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}',
        lambda: None,
        provider_fence=fence)
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = response

    with pytest.raises(RuntimeError, match='source lease lost'):
        source.read_root()

    response.close.assert_called_once_with()


def test_source_reader_fences_each_streamed_blob_chunk_and_closes_on_loss(
) -> None:
    response = mock.Mock(status_code=200, headers={})
    response.iter_content.return_value = [b'first', b'second']
    calls = 0

    def fence() -> None:
        nonlocal calls
        calls += 1
        # Five checks complete request setup, one precedes iteration, and the
        # seventh check fences the first chunk before it leaves the adapter.
        if calls == 7:
            raise RuntimeError('source lease lost while streaming')

    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}',
        lambda: None,
        provider_fence=fence)
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = response

    chunks = iter(
        source.read_blob(
            mock.Mock(digest=_DIGEST,
                      size=11,
                      media_type='application/octet-stream')))
    with pytest.raises(RuntimeError, match='lease lost while streaming'):
        next(chunks)
    response.close.assert_called_once_with()


def test_basic_credentials_cannot_cross_to_bearer_realm() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}',
        lambda: providers.SourceCredentials(username='user',
                                            password='password'))
    response = mock.Mock(
        status_code=401,
        headers={
            'WWW-Authenticate': 'Bearer realm="https://auth.example/token",'
                                'service="registry.example"'
        })
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = response

    with pytest.raises(ValueError,
                       match='credentials cannot cross authorities'):
        source.read_root()
    source._session.get.assert_not_called()
    response.close.assert_called_once_with()


def test_private_redirect_is_rejected_without_a_second_request() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}', lambda: None)
    response = mock.Mock(
        status_code=307,
        headers={'Location': 'https://169.254.169.254/latest/meta-data'})
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = response

    with pytest.raises(ValueError, match='network destination is not public'):
        source.read_blob_bytes(_DIGEST, max_bytes=1024)
    assert source._session.request.call_count == 1
    response.close.assert_called_once_with()


def test_manifest_redirect_is_rejected_without_a_second_request() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}', lambda: None)
    response = mock.Mock(status_code=307,
                         headers={'Location': 'https://cdn.example/manifest'})
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = response

    with pytest.raises(ValueError, match='manifest redirects are not allowed'):
        source.read_root()
    assert source._session.request.call_count == 1
    response.close.assert_called_once_with()


def test_blob_redirect_drops_credentials_and_rejects_a_chain() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}',
        lambda: providers.SourceCredentials(username='user',
                                            password='password'))
    first = mock.Mock(status_code=307,
                      headers={'Location': 'https://cdn.example/blob'})
    second = mock.Mock(status_code=307,
                       headers={'Location': 'https://other.example/blob'})
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.side_effect = [first, second]

    with pytest.raises(ValueError, match='redirect chains are not allowed'):
        source.read_blob_bytes(_DIGEST, max_bytes=1024)
    assert source._session.request.call_count == 2
    initial_call, redirected_call = source._session.request.call_args_list
    assert initial_call.kwargs['auth'] == ('user', 'password')
    assert 'auth' not in redirected_call.kwargs
    assert 'headers' not in redirected_call.kwargs
    first.close.assert_called_once_with()
    second.close.assert_called_once_with()


def test_bearer_token_request_does_not_follow_redirects() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}',
        lambda: providers.SourceCredentials(username='user',
                                            password='password'))
    challenge = mock.Mock(
        status_code=401,
        headers={
            'WWW-Authenticate': 'Bearer realm="https://registry.example/token",'
                                'service="registry.example"'
        })
    token = mock.Mock(status_code=200)
    token.headers = {}
    token.iter_content.return_value = [
        json.dumps({
            'token': 'ephemeral'
        }).encode()
    ]
    manifest = mock.Mock(status_code=200, headers={})
    manifest.iter_content.return_value = [b'not-the-requested-digest']
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.side_effect = [challenge, manifest]
    source._session.get.return_value = token

    with pytest.raises(ValueError, match='manifest digest mismatch'):
        source.read_root()
    source._session.get.assert_called_once_with(
        'https://registry.example/token',
        params={'service': 'registry.example'},
        auth=('user', 'password'),
        timeout=60,
        stream=True,
        allow_redirects=False)
    challenge.close.assert_called_once_with()
    token.close.assert_called_once_with()
    manifest.close.assert_called_once_with()


def test_bearer_token_redirect_is_rejected() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}', lambda: None)
    challenge = mock.Mock(
        status_code=401,
        headers={
            'WWW-Authenticate': 'Bearer realm="https://registry.example/token",'
                                'service="registry.example"'
        })
    token = mock.Mock(status_code=302,
                      headers={'Location': 'https://public.example/token'})
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = challenge
    source._session.get.return_value = token

    with pytest.raises(ValueError, match='token redirects are not allowed'):
        source.read_root()
    assert source._session.request.call_count == 1
    token.close.assert_called_once_with()


def test_manifest_body_is_bounded_while_streaming() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}', lambda: None)
    response = mock.Mock(status_code=200, headers={})
    response.iter_content.return_value = [b'a' * 5, b'b' * 5]
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = response

    with pytest.raises(ValueError, match='manifest exceeds the size limit'):
        source.read_root(max_bytes=9)
    response.close.assert_called_once_with()


def test_token_body_is_bounded_while_streaming() -> None:
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{_DIGEST}', lambda: None)
    challenge = mock.Mock(
        status_code=401,
        headers={
            'WWW-Authenticate': 'Bearer realm="https://registry.example/token",'
                                'service="registry.example"'
        })
    token = mock.Mock(status_code=200, headers={})
    token.iter_content.return_value = [b'x' * (64 * 1024 + 1)]
    source._session = mock.Mock()  # pylint: disable=protected-access
    source._session.request.return_value = challenge
    source._session.get.return_value = token

    with pytest.raises(ValueError,
                       match='token response exceeds the size limit'):
        source.read_root()
    token.close.assert_called_once_with()
