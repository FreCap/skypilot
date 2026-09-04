"""Implementation of the synchronous SDK request-result lifecycle."""

from collections.abc import Callable
from collections.abc import Mapping
import logging
import typing
from typing import Optional, TypeVar

import colorama

from sky import exceptions
from sky.client import common as client_common
from sky.client import interactive_utils
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import rest
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.utils import common_utils
from sky.utils import context_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import io

    import requests

T = TypeVar('T')

_REQUEST_RESULT_MAX_ATTEMPTS = 3
_TRANSIENT_RESULT_HTTP_STATUSES = frozenset((502, 503, 504))


def _response_api_version(headers: Mapping[str, str]) -> int | None:
    """Return the API version asserted by this exact response, if valid."""
    value = headers.get(server_constants.API_VERSION_HEADER)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_request_result_retry_required(status_code: int, headers: Mapping[str,
                                                                        str],
                                     request_id: str) -> bool:
    """Whether one response authoritatively permits replaying an operation."""
    if status_code != 503:
        return False
    response_api_version = _response_api_version(headers)
    if (response_api_version is None or response_api_version
            < server_constants.MIN_REQUEST_RESULT_RETRY_MARKER_API_VERSION):
        return False
    marker = headers.get(server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER)
    return marker == request_id


def request_result_protocol_error(http_status_code: int,
                                  request_status: requests_lib.RequestStatus,
                                  has_error: bool) -> str | None:
    """Validate the terminal result's HTTP/status/error consistency."""
    if has_error:
        if http_status_code != 500:
            return (f'HTTP {http_status_code} carried an application error; '
                    'expected HTTP 500')
        if request_status != requests_lib.RequestStatus.FAILED:
            return (f'HTTP 500 carried an application error for request '
                    f'status {request_status!r}; expected FAILED')
        return None
    if request_status == requests_lib.RequestStatus.CANCELLED:
        if http_status_code != 200:
            return (f'CANCELLED result used HTTP {http_status_code}; '
                    'expected HTTP 200')
        return None
    if request_status != requests_lib.RequestStatus.SUCCEEDED:
        return f'request status {request_status!r} has no application error'
    if http_status_code != 200:
        return (f'SUCCEEDED result used HTTP {http_status_code}; '
                'expected HTTP 200')
    return None


def _response_description(response: 'requests.Response') -> str:
    try:
        text = response.text
    except Exception:  # pylint: disable=broad-except
        text = '<unreadable response body>'
    return f'{response.status_code} {text}'


def _is_transient_result_error(error: Exception) -> bool:
    return (isinstance(error, exceptions.RequestInterruptedError) or
            rest.is_transient_error(error))


def _get_request_response(
        request_id: server_common.RequestId[T]) -> 'requests.Response':
    """Read one durable result with bounded, cancellation-aware retries."""
    backoff = common_utils.Backoff(initial_backoff=1, max_backoff_factor=5)
    last_error: Exception | None = None
    for attempt in range(_REQUEST_RESULT_MAX_ATTEMPTS):
        response = None
        try:
            response = server_common.make_authenticated_request(
                'GET',
                f'/api/get?request_id={request_id}',
                retry=False,
                raise_for_server_unavailable=False,
                timeout=(
                    client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                    client_common.API_SERVER_REQUEST_RESULT_READ_TIMEOUT_SECONDS
                ))
        except Exception as error:  # pylint: disable=broad-except
            last_error = error
            if (not _is_transient_result_error(error) or
                    attempt == _REQUEST_RESULT_MAX_ATTEMPTS - 1):
                break
        else:
            if is_request_result_retry_required(response.status_code,
                                                response.headers,
                                                str(request_id)):
                response.close()
                raise exceptions.RequestResultShouldRetryError(str(request_id))
            if response.status_code not in _TRANSIENT_RESULT_HTTP_STATUSES:
                return response
            last_error = RuntimeError(_response_description(response))
            response.close()
            if attempt == _REQUEST_RESULT_MAX_ATTEMPTS - 1:
                break
        context_utils.sleep_with_cancellation(backoff.current_backoff())

    assert last_error is not None
    raise exceptions.RequestResultUnavailableError(
        str(request_id),
        common_utils.format_exception(last_error)) from last_error


def stream_response(
    request_id: server_common.RequestId[T] | None,
    response: 'requests.Response',
    output_stream: Optional['io.TextIOBase'] = None,
    resumable: bool = False,
    get_result: bool = True,
    relay_rich_status: bool = False,
    *,
    get_request_result: Callable[[server_common.RequestId[T]], T],
    logger: logging.Logger,
) -> T | None:
    """Streams a response and optionally retrieves its final result."""
    # Always fetch the retry context (if any) so we can report progress to
    # the retry decorator across all stream types. `resumable` only controls
    # whether already-printed lines are skipped on retry.
    retry_context = rest.get_retry_context()
    try:
        line_count = 0

        for line in rich_utils.decode_rich_status(
                response, relay_rich_status=relay_rich_status):
            # Report forward progress to the retry decorator for every
            # message received from the wire, including None control
            # messages (e.g. heartbeats). Receiving any message
            # indicates the underlying connection is healthy, so the
            # consecutive-failure counter should reset. Without this,
            # resumable streams that spend a full retry window only
            # replaying already-printed lines (or receiving only
            # heartbeats) never advance `progress_count` and can
            # exhaust their retry budget even though the stream is
            # actively making progress over the network.
            if retry_context is not None:
                retry_context.progress_count += 1

            if line is not None:
                line_count += 1

                line = interactive_utils.handle_interactive_auth(line)
                if line is None:
                    # Line was consumed by interactive auth handler
                    continue

                if (resumable and retry_context is not None and
                        line_count <= retry_context.line_processed):
                    # Already printed on a previous attempt; skip.
                    continue

                print(line, flush=True, end='', file=output_stream)

                if retry_context is not None and resumable:
                    # Reaching here implies line_count > line_processed
                    # (otherwise the resumable skip above would have
                    # `continue`'d). Advance the high-water mark.
                    retry_context.line_processed = line_count
        if request_id is not None and get_result:
            return get_request_result(request_id)
        else:
            return None
    except Exception:  # pylint: disable=broad-except
        logger.debug(f'To stream request logs: sky api logs {request_id}')
        raise


def _get(
    request_id: server_common.RequestId[T],
    *,
    raise_exception: Callable[[BaseException], None],
    logger: logging.Logger,
    require_exact_request_id: bool,
) -> T:
    """Observe and decode one request result under an explicit error policy."""
    response = _get_request_response(request_id)
    request_id_str = str(request_id)
    request_task = None
    decode_error: Exception | None = None
    try:
        try:
            response_payload = response.json()
            if not isinstance(response_payload, dict):
                raise TypeError('request result body is not an object')
            encoded_request = None
            if response.status_code == 200:
                encoded_request = response_payload
            elif response.status_code == 500:
                encoded_request = response_payload.get('detail')
            if not isinstance(encoded_request, dict):
                raise TypeError('request result payload is not an object')
            request_task = requests_lib.Request.decode(
                payloads.RequestPayload(**encoded_request))
        except Exception as error:  # pylint: disable=broad-except
            decode_error = error
    finally:
        response.close()

    if request_task is None:
        if decode_error is None:
            message = _response_description(response)
        else:
            message = ('malformed request result: '
                       f'{common_utils.format_exception(decode_error)}')
        raise exceptions.RequestResultUnavailableError(
            request_id_str, message) from decode_error
    if (require_exact_request_id and
            str(request_task.request_id) != request_id_str):
        raise exceptions.RequestResultUnavailableError(
            request_id_str,
            f'result belongs to request {request_task.request_id!r}')
    try:
        request_error = request_task.get_error()
    except Exception as decode_error:  # pylint: disable=broad-except
        raise exceptions.RequestResultUnavailableError(
            request_id_str, 'malformed request error: '
            f'{common_utils.format_exception(decode_error)}') from decode_error
    if request_error is not None:
        error_obj = (request_error.get('object') if isinstance(
            request_error, dict) else None)
        if not isinstance(error_obj, BaseException):
            raise exceptions.RequestResultUnavailableError(
                request_id_str, 'malformed request error object')
    else:
        error_obj = None
    protocol_error = request_result_protocol_error(response.status_code,
                                                   request_task.status,
                                                   error_obj is not None)
    if protocol_error is not None:
        raise exceptions.RequestResultUnavailableError(request_id_str,
                                                       protocol_error)
    if error_obj is not None:
        logger.debug(f'Got request with error: {request_task.name}')
        raise_exception(error_obj)
        raise exceptions.RequestResultUnavailableError(
            request_id_str, 'request error projection unexpectedly returned')
    if request_task.status == requests_lib.RequestStatus.CANCELLED:
        if require_exact_request_id:
            raise exceptions.RequestResultUnavailableError(
                request_id_str,
                'request was cancelled without authoritative replay marker')
        cancelled = exceptions.RequestCancelled(
            f'{colorama.Fore.YELLOW}Current {request_task.name!r} request '
            f'({request_task.request_id}) is cancelled by another process.'
            f'{colorama.Style.RESET_ALL}')
        raise_exception(cancelled)
        raise exceptions.RequestResultUnavailableError(
            request_id_str,
            'request cancellation projection unexpectedly returned')
    try:
        return request_task.get_return_value()
    except Exception as decode_error:  # pylint: disable=broad-except
        raise exceptions.RequestResultUnavailableError(
            request_id_str, 'malformed request return value: '
            f'{common_utils.format_exception(decode_error)}') from decode_error


def get(
    request_id: server_common.RequestId[T],
    *,
    raise_exception: Callable[[BaseException], None],
    logger: logging.Logger,
) -> T:
    """Waits for and decodes the result of a request."""
    return _get(request_id,
                raise_exception=raise_exception,
                logger=logger,
                require_exact_request_id=False)


def get_for_reconciliation(request_id: server_common.RequestId[T], *,
                           logger: logging.Logger) -> T:
    """Get an exact result while preserving decoded operation provenance."""

    def raise_application_error(error: BaseException) -> None:
        raise exceptions.RequestResultApplicationError(str(request_id), error)

    return _get(request_id,
                raise_exception=raise_application_error,
                logger=logger,
                require_exact_request_id=True)


def stream_and_get(
    request_id: server_common.RequestId[T] | None = None,
    log_path: str | None = None,
    tail: int | None = None,
    follow: bool = True,
    output_stream: Optional['io.TextIOBase'] = None,
    relay_rich_status: bool = False,
    *,
    get_request_result: Callable[[server_common.RequestId[T]], T],
    stream_response_fn: Callable[..., T | None],
) -> T | None:
    """Streams request logs and returns the decoded result."""
    params = {
        'request_id': request_id,
        'log_path': log_path,
        'tail': str(tail) if tail is not None else None,
        'follow': follow,
        'format': 'console',
    }
    response = server_common.make_authenticated_request(
        'GET',
        '/api/stream',
        params=params,
        retry=False,
        timeout=(client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                 None),
        stream=True)
    if response.status_code in [404, 400]:
        detail = response.json().get('detail')
        with ux_utils.print_exception_no_traceback():
            raise exceptions.ClientError(f'Failed to stream logs: {detail}')
    if response.status_code != 200 and request_id is not None:
        # A failed stream may still correspond to a request whose result is
        # available through /api/get when the caller supplied its ID. Without
        # one, get_stream_request_id() preserves the HTTP error.
        return get_request_result(request_id)
    stream_request_id: server_common.RequestId[
        T] | None = server_common.get_stream_request_id(response)
    if request_id is not None and stream_request_id is not None:
        if request_id != stream_request_id:
            raise RuntimeError(
                f'Stream request ID mismatch: requested {request_id!r}, '
                f'server returned {stream_request_id!r}.')
    if request_id is None:
        request_id = stream_request_id
    return stream_response_fn(request_id,
                              response,
                              output_stream,
                              resumable=True,
                              get_result=follow,
                              relay_rich_status=relay_rich_status)
