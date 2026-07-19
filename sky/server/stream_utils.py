"""Utilities for streaming logs from response."""

import asyncio
import collections
from collections.abc import AsyncGenerator
import fcntl
import os
import pathlib

import aiofiles
import aiofiles.os
import fastapi

from sky import global_user_state
from sky import sky_logging
from sky.server.requests import requests as requests_lib
from sky.utils import common_utils
from sky.utils import context as sky_context
from sky.utils import message_utils
from sky.utils import rich_utils
from sky.utils import status_lib

logger = sky_logging.init_logger(__name__)

# When streaming log lines, buffer the lines in memory and flush them in chunks
# to improve log tailing throughput. Buffer size is the max size bytes of each
# chunk and the timeout threshold for flushing the buffer to ensure
# responsiveness.
_BUFFER_SIZE = 8 * 1024  # 8KB
_BUFFER_TIMEOUT = 0.02  # 20ms
_HEARTBEAT_INTERVAL = 30
_READ_CHUNK_SIZE = 256 * 1024  # 256KB chunks for file reading

# If a SHORT request has been stuck in pending for
# _SHORT_REQUEST_SPINNER_TIMEOUT seconds, we show the waiting spinner
_SHORT_REQUEST_SPINNER_TIMEOUT = 2

LONG_REQUEST_POLL_INTERVAL = 1
DEFAULT_POLL_INTERVAL = 0.1


def ensure_request_log_storage_available() -> None:
    """Reject new remote-log work before it consumes the disk reserve."""
    usage = requests_lib.get_request_log_storage_usage()
    if usage.free_bytes >= usage.hard_free_bytes:
        return
    raise fastapi.HTTPException(
        status_code=fastapi.status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=('API server request-log storage is under pressure; retry '
                'after terminal streaming logs are cleaned up.'),
        headers={'Retry-After': '60'})


async def _yield_log_file_with_payloads_skipped(
        log_file) -> AsyncGenerator[str, None]:
    async for line in log_file:
        if not line:
            return
        is_payload, line_str = message_utils.decode_payload(
            line.decode('utf-8'), raise_for_mismatch=False)
        if is_payload:
            continue

        yield line_str


async def _rewind_if_log_truncated(
        log_file: aiofiles.threadpool.binary.AsyncBufferedReader,
        log_path: pathlib.Path) -> bool:
    """Rewind a follower whose offset is beyond an in-place truncation."""
    position = await log_file.tell()
    try:
        current_size = (await aiofiles.os.stat(log_path)).st_size
    except FileNotFoundError:
        return False
    if current_size >= position:
        return False
    # Seeking directly to 0 can reuse BufferedReader bytes cached before the
    # external truncate. Seeking to the current EOF first invalidates that
    # read-ahead buffer, then the rewind sees the replacement generation.
    await log_file.seek(0, os.SEEK_END)
    await log_file.seek(0)
    return True


def _request_log_marker(
    log_file: aiofiles.threadpool.binary.AsyncBufferedReader,
) -> sky_context.RequestLogTruncationMarker | None:
    """Read a bounded request log's marker without buffered read-ahead."""
    # BufferedReader can retain bytes from before an in-place truncate, even
    # after seek(0). pread() bypasses that user-space buffer and leaves the
    # follower's cursor unchanged. This fixed-size local read is intentionally
    # synchronous: it is one small syscall, not filesystem traversal.
    prefix = os.pread(log_file.fileno(),
                      sky_context.REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES, 0)
    return sky_context.parse_request_log_truncation_marker(prefix)


async def _rewind_if_log_generation_changed(
    log_file: aiofiles.threadpool.binary.AsyncBufferedReader,
    previous_marker: sky_context.RequestLogTruncationMarker | None,
) -> tuple[sky_context.RequestLogTruncationMarker | None, bool, bool]:
    """Map a follower across rollover, including truncate-and-regrow races.

    Returns the current marker, whether the generation changed, and whether
    the follower lost an unread prefix because it fell behind the retained
    window.
    """
    marker = _request_log_marker(log_file)
    if marker is None or marker == previous_marker:
        return previous_marker, False, False
    if (previous_marker is not None and
            marker.generation == previous_marker.generation):
        return previous_marker, False, False

    position = await log_file.tell()
    if previous_marker is None:
        logical_position = position
    else:
        logical_position = previous_marker.logical_start + max(
            0, position - previous_marker.byte_length)
    current_size = os.fstat(log_file.fileno()).st_size
    lost_prefix = logical_position < marker.logical_start
    if lost_prefix:
        target_position = 0
    else:
        target_position = marker.byte_length + (logical_position -
                                                marker.logical_start)
        target_position = min(target_position, current_size)

    # Visit EOF first to invalidate BufferedReader data cached before the
    # external truncate, then seek to the equivalent logical stream offset.
    await log_file.seek(0, os.SEEK_END)
    await log_file.seek(target_position)
    return marker, True, lost_prefix


async def _acquire_shared_file_lock(fd: int) -> None:
    """Acquire a cancellation-safe shared flock without blocking the loop."""
    # A blocking flock offloaded with asyncio.to_thread() can leak a lock if
    # the tail task is cancelled while the worker thread is waiting: the
    # thread may later acquire the lock after the coroutine's finally block is
    # gone. Poll non-blockingly so cancellation can never strand a lock.
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            await asyncio.sleep(0.01)


async def _read_request_log_chunk(
    log_file: aiofiles.threadpool.binary.AsyncBufferedReader,
    previous_marker: sky_context.RequestLogTruncationMarker | None,
    log_path: pathlib.Path,
) -> tuple[bytes, sky_context.RequestLogTruncationMarker | None, bool]:
    """Read one chunk atomically with respect to bounded-log rollover."""
    fd = log_file.fileno()
    await _acquire_shared_file_lock(fd)
    try:
        marker, _, lost_prefix = (await _rewind_if_log_generation_changed(
            log_file, previous_marker))
        if marker is None and await _rewind_if_log_truncated(
                log_file, log_path):
            # Legacy/unbounded writers have no generation marker. Keep their
            # physical truncate handling inside the same shared-lock window as
            # the following read so a cooperative bounded writer cannot race
            # between the size check and cursor reset.
            lost_prefix = True
        chunk = await log_file.read(_READ_CHUNK_SIZE)
        return chunk, marker, lost_prefix
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)


async def wait_for_request_to_start(
    request_id: str,
    plain_logs: bool = False,
    follow: bool = True,
    polling_interval: float = DEFAULT_POLL_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield waiting-status feedback until a request reaches RUNNING.

    Used as a prelude to log streaming: polls the request's status and
    yields either rich spinner payloads (for CLI clients) or plain text
    waiting messages (for plain-log clients) while the request is still
    PENDING, so the HTTP stream stays warm and the user sees progress.

    The behavior mirrors the wait loop in ``log_streamer`` and is the
    source of truth for it — ``log_streamer`` delegates to this helper
    when given a ``request_id``.

    Args:
        request_id: The request ID to wait for.
        plain_logs: If True, yield plain-text waiting messages; otherwise
            yield rich spinner payloads (SHORT requests stay silent until
            stuck longer than ``_SHORT_REQUEST_SPINNER_TIMEOUT``).
        follow: If False, return after one status check even if the
            request is still PENDING.
        polling_interval: Initial DB poll interval. Backs off up to 10x.

    Raises:
        fastapi.HTTPException: 404 if the request does not exist.
    """
    start_time = asyncio.get_event_loop().time()
    status_msg = rich_utils.EncodedStatusMessage(
        f'[dim]Checking request: {request_id}[/dim]')
    request_task = await requests_lib.get_request_async(
        request_id,
        fields=['request_id', 'name', 'schedule_type', 'status', 'status_msg'])
    if request_task is None:
        raise fastapi.HTTPException(status_code=404,
                                    detail=f'Request {request_id} not found')

    # By default, do not show the waiting spinner for SHORT requests.
    # If the request has been stuck in pending for
    # _SHORT_REQUEST_SPINNER_TIMEOUT seconds, we show the waiting spinner
    show_request_waiting_spinner = (not plain_logs and
                                    request_task.schedule_type
                                    == requests_lib.ScheduleType.LONG)

    if show_request_waiting_spinner:
        yield status_msg.init()
        yield status_msg.start()
    last_waiting_msg = ''
    waiting_msg = (f'Waiting for {request_task.name!r} request to be '
                   f'scheduled: {request_id}')
    req_status = request_task.status
    req_msg = request_task.status_msg
    del request_task
    # Slowly back off the database polling up to every 1 second, to avoid
    # overloading the CPU and DB.
    backoff = common_utils.Backoff(initial_backoff=polling_interval,
                                   max_backoff_factor=10,
                                   multiplier=1.2)
    while req_status < requests_lib.RequestStatus.RUNNING:
        current_time = asyncio.get_event_loop().time()
        # Show the waiting spinner for a SHORT request if it has been stuck
        # in pending for _SHORT_REQUEST_SPINNER_TIMEOUT seconds
        if not show_request_waiting_spinner and (
                current_time - start_time > _SHORT_REQUEST_SPINNER_TIMEOUT):
            show_request_waiting_spinner = True
            yield status_msg.init()
            yield status_msg.start()
        if req_msg is not None:
            waiting_msg = req_msg
        if show_request_waiting_spinner:
            yield status_msg.update(f'[dim]{waiting_msg}[/dim]')
        elif plain_logs and waiting_msg != last_waiting_msg:
            # Only log when waiting message changes.
            last_waiting_msg = waiting_msg
            # Padding forces browser rendering of the streamed chunk.
            yield waiting_msg + ' ' * 4096 + '\n'
        # Sleep shortly to avoid storming the DB and CPU and allow other
        # coroutines to run.
        # TODO(aylei): we should use a better mechanism to avoid busy
        # polling the DB, which can be a bottleneck for high-concurrency
        # requests.
        await asyncio.sleep(backoff.current_backoff())
        status_with_msg = await requests_lib.get_request_status_async(
            request_id, include_msg=True)
        if status_with_msg is None:
            # Request record vanished (e.g. deleted while polling).
            break
        req_status = status_with_msg.status
        req_msg = status_with_msg.status_msg
        if not follow:
            break
    if show_request_waiting_spinner:
        yield status_msg.stop()


async def log_streamer(
    request_id: str | None,
    log_path: pathlib.Path | None = None,
    plain_logs: bool = False,
    tail: int | None = None,
    follow: bool = True,
    cluster_name: str | None = None,
    polling_interval: float = DEFAULT_POLL_INTERVAL
) -> AsyncGenerator[str, None]:
    """Streams the logs of a request.

    Args:
        request_id: The request ID to check whether the log tailing process
            should be stopped.
        log_path: The path to the log file or directory containing the log
        files. If it is a directory, all *.log files in the directory will be
        streamed.
        plain_logs: Whether to show plain logs.
        tail: The number of lines to tail. If None, tail the whole file.
        follow: Whether to follow the log file.
        cluster_name: The cluster name to check status for provision logs.
            If provided and cluster status is UP, streaming will terminate.
    """

    if request_id is not None:
        async for chunk in wait_for_request_to_start(
                request_id,
                plain_logs=plain_logs,
                follow=follow,
                polling_interval=polling_interval):
            yield chunk

    # worker node provision logs
    if log_path is not None and log_path.is_dir():
        # Get all *.log files in the log_path dir
        log_files = sorted(log_path.glob('*.log'))

        for log_file_path in log_files:
            # Add header before each file (similar to tail -f behavior)
            header = f'\n==> {log_file_path} <==\n\n'
            yield header

            async with aiofiles.open(log_file_path, 'rb') as f:
                async for chunk in _tail_log_file(f,
                                                  request_id,
                                                  plain_logs,
                                                  tail,
                                                  follow,
                                                  cluster_name,
                                                  polling_interval,
                                                  log_path=log_file_path):
                    yield chunk

    # api server request logs (if request_id is provided) or
    # head node provision logs (if cluster_name is provided)
    else:
        assert log_path is not None, (request_id, cluster_name)
        async with aiofiles.open(log_path, 'rb') as f:
            async for chunk in _tail_log_file(f,
                                              request_id,
                                              plain_logs,
                                              tail,
                                              follow,
                                              cluster_name,
                                              polling_interval,
                                              log_path=log_path):
                yield chunk


async def _tail_log_file(
    f: aiofiles.threadpool.binary.AsyncBufferedReader,
    request_id: str | None = None,
    plain_logs: bool = False,
    tail: int | None = None,
    follow: bool = True,
    cluster_name: str | None = None,
    polling_interval: float = DEFAULT_POLL_INTERVAL,
    log_path: pathlib.Path | None = None,
) -> AsyncGenerator[str, None]:
    """Tail the opened log file, buffer the lines and flush in chunks."""

    log_marker = _request_log_marker(f) if log_path is not None else None
    if tail is not None:
        # Find last n lines of the log file. Do not read the whole file into
        # memory.
        # TODO(zhwu): this will include the control lines for rich status,
        # which may not lead to exact tail lines when showing on the client
        # side.
        lines: collections.deque[str] = collections.deque(maxlen=tail)
        if log_path is not None:
            fd = f.fileno()
            await _acquire_shared_file_lock(fd)
            try:
                log_marker = _request_log_marker(f)
                async for line_str in _yield_log_file_with_payloads_skipped(f):
                    lines.append(line_str)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        else:
            async for line_str in _yield_log_file_with_payloads_skipped(f):
                lines.append(line_str)
        for line_str in lines:
            yield line_str

    last_heartbeat_time = asyncio.get_event_loop().time()
    last_status_check_time = asyncio.get_event_loop().time()

    # Buffer the lines in memory and flush them in chunks to improve log
    # tailing throughput.
    buffer: list[str] = []
    buffer_bytes = 0
    last_flush_time = asyncio.get_event_loop().time()

    # Read file in chunks instead of line-by-line for better performance
    incomplete_line = b''  # Buffer for incomplete lines across chunks

    def flush_buffer() -> str | None:
        nonlocal buffer, buffer_bytes, last_flush_time
        if buffer:
            chunk = ''.join(buffer)
            buffer.clear()
            buffer_bytes = 0
            last_flush_time = asyncio.get_event_loop().time()
            return chunk
        return None

    while True:
        # Sleep 0 to yield control to allow other coroutines to run,
        # while keeps the loop tight to make log stream responsive.
        await asyncio.sleep(0)
        current_time = asyncio.get_event_loop().time()
        # Flush the buffer when it is not empty and the buffer is full or the
        # flush timeout is reached.
        if buffer and (buffer_bytes >= _BUFFER_SIZE or
                       (current_time - last_flush_time) >= _BUFFER_TIMEOUT):
            chunk = flush_buffer()
            if chunk is not None:
                yield chunk

        if log_path is not None:
            file_chunk, log_marker, lost_prefix = (await
                                                   _read_request_log_chunk(
                                                       f, log_marker, log_path))
            if lost_prefix:
                # Preserve complete output before the gap, but never join a
                # partial discarded line to the retained generation.
                chunk = flush_buffer()
                if chunk is not None:
                    yield chunk
                incomplete_line = b''
        else:
            # Read file in chunks for better I/O performance.
            file_chunk = await f.read(_READ_CHUNK_SIZE)
        if not file_chunk:
            # Process any remaining incomplete line
            if incomplete_line:
                line_str = incomplete_line.decode('utf-8')
                if plain_logs:
                    is_payload, line_str = message_utils.decode_payload(
                        line_str, raise_for_mismatch=False)
                    if not is_payload:
                        buffer.append(line_str)
                        buffer_bytes += len(line_str.encode('utf-8'))
                else:
                    buffer.append(line_str)
                    buffer_bytes += len(line_str.encode('utf-8'))
                incomplete_line = b''

            # Avoid checking the status too frequently to avoid overloading the
            # DB.
            should_check_status = (current_time -
                                   last_status_check_time) >= polling_interval
            if not follow:
                # We will only hit this path once, but we should make sure to
                # check the status so that we display the final request status
                # if the request is complete.
                should_check_status = True
            if request_id is not None and should_check_status:
                last_status_check_time = current_time
                req_status = await requests_lib.get_request_status_async(
                    request_id)
                if req_status.status > requests_lib.RequestStatus.RUNNING:
                    # Status can become terminal after our EOF read but before
                    # the producer's final write becomes visible. Drain once
                    # more under the same lock/generation protocol before
                    # deciding the stream is complete.
                    if log_path is not None:
                        file_chunk, log_marker, lost_prefix = (
                            await
                            _read_request_log_chunk(f, log_marker, log_path))
                        if lost_prefix:
                            chunk = flush_buffer()
                            if chunk is not None:
                                yield chunk
                            incomplete_line = b''
                    else:
                        file_chunk = await f.read(_READ_CHUNK_SIZE)
                    if not file_chunk:
                        if (req_status.status ==
                                requests_lib.RequestStatus.CANCELLED):
                            request_task = await requests_lib.get_request_async(
                                request_id, fields=['name', 'should_retry'])
                            if request_task.should_retry:
                                buffer.append(
                                    message_utils.encode_payload(
                                        rich_utils.Control.RETRY.encode('')))
                            else:
                                buffer.append(f'{request_task.name!r} request '
                                              f'{request_id} cancelled\n')
                            del request_task
                        break
            if not file_chunk:
                if not follow:
                    # The below checks (cluster status, heartbeat) are not
                    # needed for non-follow logs.
                    break
                # Provision logs pass in cluster_name, check cluster status
                # periodically to see if provisioning is done.
                if cluster_name is not None:
                    if should_check_status:
                        last_status_check_time = current_time
                        cluster_status = await (
                            global_user_state.
                            get_status_from_cluster_name_async(cluster_name))
                        if cluster_status is None:
                            logger.debug(
                                'Stop tailing provision logs for cluster'
                                f' status for cluster {cluster_name} not found')
                            break
                        # if the cluster is not in INIT state (UP or STOPPED),
                        # stop tailing provision logs
                        if cluster_status != status_lib.ClusterStatus.INIT:
                            logger.debug(
                                f'Stop tailing provision logs for cluster'
                                f' {cluster_name} has status {cluster_status} '
                                '(not in INIT state)')
                            break
                        req_filter = requests_lib.RequestTaskFilter(
                            status=[requests_lib.RequestStatus.RUNNING],
                            cluster_names=[cluster_name],
                            include_request_names=['sky.launch'],
                            fields=['cluster_name'])
                        req_tasks = await requests_lib.get_request_tasks_async(
                            req_filter)
                        # If the cluster is INIT with no ongoing launch request,
                        # stop tailing provision logs.
                        if len(req_tasks) == 0:
                            break
                if current_time - last_heartbeat_time >= _HEARTBEAT_INTERVAL:
                    # Currently just used to keep the connection busy, refer to
                    # https://github.com/skypilot-org/skypilot/issues/5750 for
                    # more details.
                    buffer.append(
                        message_utils.encode_payload(
                            rich_utils.Control.HEARTBEAT.encode('')))
                    last_heartbeat_time = current_time

                # Sleep shortly to avoid storming the DB and CPU, this has
                # little impact on the responsivness here since we are waiting
                # for a new line to come in.
                await asyncio.sleep(0.1)
                continue

        # Refresh the heartbeat time, this is a trivial optimization for
        # performance but it helps avoid unnecessary heartbeat strings
        # being printed when the client runs in an old version.
        last_heartbeat_time = asyncio.get_event_loop().time()

        # Combine with any incomplete line from previous chunk
        file_chunk = incomplete_line + file_chunk
        incomplete_line = b''

        # Split chunk into lines, preserving line structure
        lines_bytes = file_chunk.split(b'\n')

        # If chunk doesn't end with newline, the last element is incomplete
        if file_chunk and not file_chunk.endswith(b'\n'):
            incomplete_line = lines_bytes[-1]
            lines_bytes = lines_bytes[:-1]
        else:
            # If ends with \n, split creates an empty last element we should
            # ignore
            if lines_bytes and lines_bytes[-1] == b'':
                lines_bytes = lines_bytes[:-1]

        # Process all complete lines in this chunk
        for line_bytes in lines_bytes:
            # Reconstruct line with newline (since split removed it)
            line_str = line_bytes.decode('utf-8') + '\n'

            if plain_logs:
                is_payload, line_str = message_utils.decode_payload(
                    line_str, raise_for_mismatch=False)
                # TODO(aylei): implement heartbeat mechanism for plain logs,
                # sending invisible characters might be okay.
                if is_payload:
                    continue

            buffer.append(line_str)
            buffer_bytes += len(line_str.encode('utf-8'))

    # Flush remaining lines in the buffer.
    chunk = flush_buffer()
    if chunk is not None:
        yield chunk


def stream_response_for_long_request(
    request_id: str,
    logs_path: pathlib.Path,
    background_tasks: fastapi.BackgroundTasks,
    kill_request_on_disconnect: bool = True,
) -> fastapi.responses.StreamingResponse:
    """Stream the logs of a long request."""
    return stream_response(
        request_id,
        logs_path,
        background_tasks,
        polling_interval=LONG_REQUEST_POLL_INTERVAL,
        kill_request_on_disconnect=kill_request_on_disconnect,
    )


def stream_response(
    request_id: str,
    logs_path: pathlib.Path,
    background_tasks: fastapi.BackgroundTasks,
    polling_interval: float = DEFAULT_POLL_INTERVAL,
    kill_request_on_disconnect: bool = True,
) -> fastapi.responses.StreamingResponse:

    if kill_request_on_disconnect:

        async def on_disconnect():
            logger.info(f'User terminated the connection for request '
                        f'{request_id}')
            await requests_lib.kill_request_async(request_id)

        # The background task will be run after returning a response.
        # https://fastapi.tiangolo.com/tutorial/background-tasks/
        background_tasks.add_task(on_disconnect)

    # Route through LogProvider.
    # pylint: disable=import-outside-toplevel
    from sky.server.requests import log_provider as lp
    return fastapi.responses.StreamingResponse(
        lp.get_log_provider().log_stream(request_id=request_id,
                                         log_path=logs_path,
                                         polling_interval=polling_interval),
        media_type='text/plain',
        headers={
            'Cache-Control': 'no-cache, no-transform',
            'X-Accel-Buffering': 'no',
            'Transfer-Encoding': 'chunked'
        })
