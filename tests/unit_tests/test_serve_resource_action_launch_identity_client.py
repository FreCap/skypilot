"""Focused tests for the bounded launch-identity client transport."""

import dataclasses
import hashlib
from unittest import mock

import pytest
import requests
import urllib3.util

from sky.serve import resource_action_client
from sky.serve import resource_actions as actions
from sky.server import rest
from sky.server.requests import resource_actions as kernel_actions

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CAPABILITY = '12' * 32


def _request() -> actions.ProviderLaunchIdentityCanonicalizationRequestV1:
    identity = actions.ProviderResourceIdentityV1.from_value({
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': 3,
    })
    canonical_input = actions.ProviderLaunchIdentityCanonicalizationInputV1(
        version=1,
        contract='api_server_effective_launch_identity_v1',
        service_name='svc',
        resource_identity=identity,
        prepared_original_user='prepared@example.com',
        prepared_user_hash='prepared-hash')
    context = actions.ProviderLaunchIdentityCanonicalizationContextV1(
        version=1,
        decision_id=identity.action_identity(
            kernel_actions.ActionKind.LAUNCH).action_id,
        cohort_id='authority-v1',
        action_type=kernel_actions.ActionKind.LAUNCH,
        controller_owner_fence='123:10.0.0.1',
        lifecycle_epoch=4,
        preparation_reference_revision=1,
        reference_state=actions.WorkerCohortReferenceState.PREPARING,
        preparation_capability_sha256=hashlib.sha256(
            bytes.fromhex(_CAPABILITY)).hexdigest(),
        input=canonical_input,
        input_sha256=canonical_input.sha256)
    return actions.ProviderLaunchIdentityCanonicalizationRequestV1(
        version=1,
        context=context,
        context_sha256=context.sha256,
        preparation_capability=_CAPABILITY)


def _response_value(
    request: actions.ProviderLaunchIdentityCanonicalizationRequestV1,
) -> actions.ProviderLaunchIdentityCanonicalizationResponseV1:
    proof = actions.ProviderLaunchIdentityCanonicalizationProofV1(
        version=1,
        boundary='api_server_post_auth_no_enqueue',
        context=request.context,
        context_sha256=request.context_sha256,
        effective_original_user='effective@example.com',
        effective_user_hash='effective-hash')
    return actions.ProviderLaunchIdentityCanonicalizationResponseV1(
        version=1,
        decision_id=request.context.decision_id,
        context_sha256=request.context_sha256,
        proof=proof,
        proof_sha256=proof.sha256)


class _Response:
    """Minimal streaming response that forbids eager content access."""

    def __init__(self,
                 status_code: int,
                 body: bytes = b'',
                 *,
                 headers: dict[str, str] | None = None,
                 chunks: list[bytes] | None = None,
                 close_error: bool = False) -> None:
        self.status_code = status_code
        self.headers = ({
            'Content-Type': 'application/json',
            'Content-Length': str(len(body)),
        } if headers is None else headers)
        self._chunks = [body] if chunks is None else chunks
        self.closed = False
        self.iterated = 0
        self._close_error = close_error

    @property
    def content(self):
        raise AssertionError('the response must be streamed')

    def iter_content(self, *, chunk_size: int, decode_unicode: bool):
        assert chunk_size == 8_192
        assert not decode_unicode
        for chunk in self._chunks:
            self.iterated += 1
            yield chunk

    def close(self) -> None:
        self.closed = True
        if self._close_error:
            raise RuntimeError('close failed')


def _success_response(
    request: actions.ProviderLaunchIdentityCanonicalizationRequestV1,
) -> _Response:
    return _Response(200, _response_value(request).canonical_bytes)


def _assert_terminal(error: pytest.ExceptionInfo[Exception]) -> None:
    assert isinstance(
        error.value, resource_action_client.LaunchIdentityCanonicalizationError)
    assert error.value.reason is (
        actions.ProviderLaunchNotRepresentableReasonV1.UNFROZEN_IDENTITY)


def test_launch_identity_client_uses_exact_authenticated_raw_transport(
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request()
    raw_response = _success_response(request)
    transport = mock.Mock(return_value=raw_response)
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)

    assert (resource_action_client.canonicalize_launch_identity(request) ==
            _response_value(request))

    transport.assert_called_once()
    call = transport.call_args
    assert call.args == (
        'POST', '/internal/resource-actions/v1/launch-identity/canonicalize')
    assert call.kwargs['retry'] is False
    assert call.kwargs['allow_non_get_without_retry'] is True
    assert call.kwargs['data'] == request.canonical_bytes
    assert call.kwargs['headers'] == {'Content-Type': 'application/json'}
    assert call.kwargs['allow_redirects'] is False
    assert call.kwargs['stream'] is True
    assert call.kwargs['raise_for_server_unavailable'] is False
    timeout = call.kwargs['timeout']
    assert isinstance(timeout, urllib3.util.Timeout)
    assert timeout.connect_timeout == 1.0
    assert timeout.total == 5.0
    assert raw_response.closed


@pytest.mark.parametrize('first_error', [
    ConnectionResetError('reset'),
    requests.exceptions.Timeout('timeout'),
    requests.exceptions.ConnectionError(ConnectionResetError('wrapped reset')),
])
def test_launch_identity_client_retries_one_transport_failure_with_same_bytes(
        monkeypatch: pytest.MonkeyPatch, first_error: Exception) -> None:
    request = _request()
    transport = mock.Mock(side_effect=[first_error, _success_response(request)])
    sleep = mock.Mock()
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)
    monkeypatch.setattr(resource_action_client.time, 'sleep', sleep)

    assert (resource_action_client.canonicalize_launch_identity(request) ==
            _response_value(request))
    assert transport.call_count == 2
    assert (transport.call_args_list[0].kwargs['data']
            is transport.call_args_list[1].kwargs['data'])
    first_timeout = transport.call_args_list[0].kwargs['timeout']
    second_timeout = transport.call_args_list[1].kwargs['timeout']
    assert first_timeout is not second_timeout
    assert first_timeout.connect_timeout == second_timeout.connect_timeout == 1.0
    assert first_timeout.total == second_timeout.total == 5.0
    sleep.assert_called_once_with(0.1)


def test_launch_identity_client_retries_only_exact_503_once(
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request()
    unavailable = _Response(503)
    transport = mock.Mock(side_effect=[unavailable, _success_response(request)])
    sleep = mock.Mock()
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)
    monkeypatch.setattr(resource_action_client.time, 'sleep', sleep)

    resource_action_client.canonicalize_launch_identity(request)

    assert transport.call_count == 2
    assert (transport.call_args_list[0].kwargs['data']
            is transport.call_args_list[1].kwargs['data'])
    sleep.assert_called_once_with(0.1)
    assert unavailable.closed
    assert unavailable.iterated == 0


@pytest.mark.parametrize('failure', [
    _Response(201),
    _Response(302),
    _Response(401),
    _Response(502),
    _Response(504),
    requests.exceptions.ConnectionError('generic connection failure'),
])
def test_launch_identity_client_other_failures_are_terminal_without_retry(
        monkeypatch: pytest.MonkeyPatch, failure: object) -> None:
    transport = mock.Mock(
        side_effect=[failure] if isinstance(failure, Exception) else None,
        return_value=None if isinstance(failure, Exception) else failure)
    sleep = mock.Mock()
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)
    monkeypatch.setattr(resource_action_client.time, 'sleep', sleep)

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(_request())

    _assert_terminal(error)
    assert transport.call_count == 1
    sleep.assert_not_called()


def test_launch_identity_client_second_retryable_failure_is_terminal(
        monkeypatch: pytest.MonkeyPatch) -> None:
    transport = mock.Mock(side_effect=[
        requests.exceptions.Timeout('first'),
        requests.exceptions.Timeout('second'),
        AssertionError('must not attempt a third request'),
    ])
    sleep = mock.Mock()
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)
    monkeypatch.setattr(resource_action_client.time, 'sleep', sleep)

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(_request())

    _assert_terminal(error)
    assert transport.call_count == 2
    sleep.assert_called_once_with(0.1)


@pytest.mark.parametrize('response', [
    _Response(200, b'{}'),
    _Response(200, b'{}\n'),
    _Response(200, b'\xff'),
    _Response(200, b'', headers={'Content-Type': 'text/json'}),
    _Response(200,
              b'',
              headers={
                  'Content-Type': 'application/json',
                  'Content-Encoding': 'gzip',
              }),
])
def test_launch_identity_client_malformed_response_is_terminal(
        monkeypatch: pytest.MonkeyPatch, response: _Response) -> None:
    transport = mock.Mock(return_value=response)
    sleep = mock.Mock()
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)
    monkeypatch.setattr(resource_action_client.time, 'sleep', sleep)

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(_request())

    _assert_terminal(error)
    assert transport.call_count == 1
    sleep.assert_not_called()
    assert response.closed


def test_launch_identity_client_rejects_unequal_valid_response(
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request()
    changed_input = dataclasses.replace(
        request.context.input, prepared_original_user='different@example.com')
    changed_context = dataclasses.replace(request.context,
                                          input=changed_input,
                                          input_sha256=changed_input.sha256)
    changed_request = dataclasses.replace(request,
                                          context=changed_context,
                                          context_sha256=changed_context.sha256)
    transport = mock.Mock(return_value=_success_response(changed_request))
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(request)

    _assert_terminal(error)
    transport.assert_called_once()


def test_launch_identity_client_caps_stream_before_buffer_growth(
        monkeypatch: pytest.MonkeyPatch) -> None:
    chunks = [b'x' * 8_192] * 10
    response = _Response(200,
                         headers={'Content-Type': 'application/json'},
                         chunks=chunks)
    transport = mock.Mock(return_value=response)
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request', transport)

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(_request())

    _assert_terminal(error)
    assert response.iterated == 9
    assert response.closed


def test_launch_identity_client_rejects_oversized_content_length_without_read(
        monkeypatch: pytest.MonkeyPatch) -> None:
    response = _Response(200,
                         headers={
                             'Content-Type': 'application/json',
                             'Content-Length': '65537',
                         },
                         chunks=[b'must not be read'])
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request',
                        mock.Mock(return_value=response))

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(_request())

    _assert_terminal(error)
    assert response.iterated == 0
    assert response.closed


def test_launch_identity_client_close_failure_cannot_override_terminal_error(
        monkeypatch: pytest.MonkeyPatch) -> None:
    response = _Response(401, close_error=True)
    monkeypatch.setattr(resource_action_client.server_common,
                        'make_authenticated_request',
                        mock.Mock(return_value=response))

    with pytest.raises(resource_action_client.
                       LaunchIdentityCanonicalizationError) as error:
        resource_action_client.canonicalize_launch_identity(_request())

    _assert_terminal(error)
    assert response.closed


@mock.patch('sky.server.rest.handle_server_unavailable')
@mock.patch('sky.server.rest._session')
def test_raw_rest_transport_can_return_exact_503(
        session: mock.Mock, handle_server_unavailable: mock.Mock) -> None:
    response = mock.Mock(status_code=503, headers={})
    session.request.return_value = response

    result = rest.request_without_retry('POST',
                                        'http://test.com/read-only-post',
                                        stream=True,
                                        raise_for_server_unavailable=False)

    assert result is response
    handle_server_unavailable.assert_not_called()
    session.request.assert_called_once_with(
        'POST',
        'http://test.com/read-only-post',
        stream=True,
        timeout=rest.DEFAULT_REQUEST_TIMEOUT)
