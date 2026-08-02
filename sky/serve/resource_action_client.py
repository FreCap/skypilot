"""Bounded API-server transport for SkyServe resource-action preparation."""

from __future__ import annotations

import json
import time
import typing

import urllib3.exceptions
import urllib3.util

from sky.adaptors import common as adaptors_common
from sky.serve import resource_actions
from sky.server import common as server_common

if typing.TYPE_CHECKING:
    import requests
else:
    requests = adaptors_common.LazyImport('requests')

_LAUNCH_IDENTITY_PATH = (
    '/internal/resource-actions/v1/launch-identity/canonicalize')
_MAX_RESPONSE_BYTES = 65_536
_RETRY_DELAY_SECONDS = 0.1
_RESPONSE_CHUNK_BYTES = 8_192


class LaunchIdentityCanonicalizationError(RuntimeError):
    """Terminal failure to freeze API-server effective launch identity."""

    reason = (resource_actions.ProviderLaunchNotRepresentableReasonV1.
              UNFROZEN_IDENTITY)


def _exception_tree_contains(
        error: BaseException, exception_types: tuple[type[BaseException],
                                                     ...]) -> bool:
    """Return whether an exception or an explicitly wrapped cause matches."""

    pending = [error]
    visited: set[int] = set()
    while pending:
        candidate = pending.pop()
        identity = id(candidate)
        if identity in visited:
            continue
        visited.add(identity)
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
    timeout_types: tuple[type[BaseException],
                         ...] = (requests.exceptions.Timeout, TimeoutError,
                                 urllib3.exceptions.TimeoutError)
    if _exception_tree_contains(error, timeout_types):
        return True
    return _exception_tree_contains(error, (ConnectionResetError,))


def _request_timeout() -> urllib3.util.Timeout:
    """Return a fresh one-second-connect, five-second-total timeout."""

    return urllib3.util.Timeout(connect=1.0, total=5.0)


def _close_response(response: requests.Response) -> None:
    """Best-effort close without overriding the boundary's typed outcome."""

    try:
        response.close()
    except Exception:  # pylint: disable=broad-except
        pass


def _read_bounded_response(response: requests.Response) -> bytes:
    """Stream a canonicalization response without exceeding its hard cap."""

    content_length = response.headers.get('Content-Length')
    if content_length is not None:
        try:
            parsed_content_length = int(content_length)
        except ValueError:
            raise LaunchIdentityCanonicalizationError(
                'Launch identity response has an invalid length.') from None
        if (parsed_content_length < 0 or
                parsed_content_length > _MAX_RESPONSE_BYTES):
            raise LaunchIdentityCanonicalizationError(
                'Launch identity response exceeds its byte bound.')

    body = bytearray()
    for chunk in response.iter_content(chunk_size=_RESPONSE_CHUNK_BYTES,
                                       decode_unicode=False):
        if not chunk:
            continue
        if not isinstance(chunk, bytes):
            raise LaunchIdentityCanonicalizationError(
                'Launch identity response is not a byte stream.')
        if len(chunk) > _MAX_RESPONSE_BYTES - len(body):
            raise LaunchIdentityCanonicalizationError(
                'Launch identity response exceeds its byte bound.')
        body.extend(chunk)
    return bytes(body)


def _parse_exact_response(
    body: bytes,
    request: resource_actions.ProviderLaunchIdentityCanonicalizationRequestV1,
) -> resource_actions.ProviderLaunchIdentityCanonicalizationResponseV1:
    try:
        decoded = json.loads(body.decode('utf-8'))
        response = (
            resource_actions.ProviderLaunchIdentityCanonicalizationResponseV1.
            from_value(decoded))
        if response.canonical_bytes != body:
            raise ValueError('response is not canonical JSON')
        if response.decision_id != request.context.decision_id:
            raise ValueError('response decision does not match request')
        if response.context_sha256 != request.context_sha256:
            raise ValueError('response context hash does not match request')
        if response.proof.context != request.context:
            raise ValueError('response context does not match request')
        if response.proof.context.input != request.context.input:
            raise ValueError('response input does not match request')
        if response.proof.context_sha256 != request.context_sha256:
            raise ValueError(
                'response proof context hash does not match request')
        if response.proof_sha256 != response.proof.sha256:
            raise ValueError('response proof hash does not match proof')
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError,
            ValueError):
        raise LaunchIdentityCanonicalizationError(
            'Launch identity response is invalid or unequal.') from None
    return response


def canonicalize_launch_identity(
    request: resource_actions.ProviderLaunchIdentityCanonicalizationRequestV1,
) -> resource_actions.ProviderLaunchIdentityCanonicalizationResponseV1:
    """Freeze effective launch identity through the no-enqueue API boundary."""

    if type(
            request
    ) is not resource_actions.ProviderLaunchIdentityCanonicalizationRequestV1:
        raise TypeError('launch identity request has an invalid type.')
    body = request.canonical_bytes
    headers = {'Content-Type': 'application/json'}

    for attempt in range(2):
        response = None
        retry_attempt = False
        response_body = None
        try:
            response = server_common.make_authenticated_request(
                'POST',
                _LAUNCH_IDENTITY_PATH,
                retry=False,
                allow_non_get_without_retry=True,
                data=body,
                headers=headers,
                allow_redirects=False,
                stream=True,
                timeout=_request_timeout(),
                raise_for_server_unavailable=False)
            if response.status_code == 503:
                if attempt == 0:
                    retry_attempt = True
                else:
                    raise LaunchIdentityCanonicalizationError(
                        'Launch identity endpoint is unavailable.')
            elif response.status_code != 200:
                raise LaunchIdentityCanonicalizationError(
                    'Launch identity endpoint rejected the request.')
            else:
                if response.headers.get('Content-Type') != 'application/json':
                    raise LaunchIdentityCanonicalizationError(
                        'Launch identity response has an invalid content type.')
                if response.headers.get('Content-Encoding') is not None:
                    raise LaunchIdentityCanonicalizationError(
                        'Launch identity response encoding is unsupported.')
                response_body = _read_bounded_response(response)
        except LaunchIdentityCanonicalizationError:
            raise
        except Exception as error:  # pylint: disable=broad-except
            if attempt == 0 and _is_retryable_transport_error(error):
                retry_attempt = True
            else:
                raise LaunchIdentityCanonicalizationError(
                    'Launch identity transport failed.') from None
        finally:
            if response is not None:
                _close_response(response)
        if retry_attempt:
            time.sleep(_RETRY_DELAY_SECONDS)
            continue
        assert response_body is not None
        return _parse_exact_response(response_body, request)

    raise AssertionError('launch identity retry loop did not terminate')
