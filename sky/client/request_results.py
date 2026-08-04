"""Implementation of the synchronous SDK request-result lifecycle."""

from collections.abc import Callable
import logging
import typing
from typing import Optional, TypeVar

import colorama

from sky import exceptions
from sky.client import common as client_common
from sky.client import interactive_utils
from sky.server import common as server_common
from sky.server import rest
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import io

    import requests

T = TypeVar('T')


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


def get(
    request_id: server_common.RequestId[T],
    *,
    raise_exception: Callable[[BaseException], None],
    logger: logging.Logger,
) -> T:
    """Waits for and decodes the result of a request."""
    response = server_common.make_authenticated_request(
        'GET',
        f'/api/get?request_id={request_id}',
        retry=False,
        timeout=(client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                 None))
    request_task = None
    if response.status_code == 200:
        request_task = requests_lib.Request.decode(
            payloads.RequestPayload(**response.json()))
    elif response.status_code == 500:
        try:
            request_task = requests_lib.Request.decode(
                payloads.RequestPayload(**response.json().get('detail')))
            logger.debug(f'Got request with error: {request_task.name}')
        except Exception:  # pylint: disable=broad-except
            request_task = None
    if request_task is None:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'Failed to get request {request_id}: '
                               f'{response.status_code} {response.text}')
    error = request_task.get_error()
    if error is not None:
        error_obj = error['object']
        raise_exception(error_obj)
    if request_task.status == requests_lib.RequestStatus.CANCELLED:
        with ux_utils.print_exception_no_traceback():
            raise exceptions.RequestCancelled(
                f'{colorama.Fore.YELLOW}Current {request_task.name!r} request '
                f'({request_task.request_id}) is cancelled by another process.'
                f'{colorama.Style.RESET_ALL}')
    return request_task.get_return_value()


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
