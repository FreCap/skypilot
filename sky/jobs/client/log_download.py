"""Log download transport and local persistence for managed jobs."""
import json
import pathlib
import threading
from typing import TYPE_CHECKING
import zlib

from sky.client import common as client_common
from sky.client import sdk
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server.requests import payloads

if TYPE_CHECKING:
    import requests


def download_logs_streaming(
    name: str | None,
    job_id: int | None,
    refresh: bool,
    controller: bool,
    local_dir: str,
) -> dict[int, str] | None:
    """Download a managed job log through the streaming API."""

    def _close_response(response: 'requests.Response') -> None:
        try:
            response.close()
        except Exception:  # pylint: disable=broad-except
            pass

    body = payloads.JobsLogsBody(
        name=name,
        job_id=job_id,
        follow=False,
        controller=controller,
        refresh=refresh,
        tail=None,
    )
    dispatch = server_common.make_authenticated_request(
        'POST',
        '/jobs/logs',
        json=json.loads(body.model_dump_json()),
        stream=True,
        timeout=(5, None))
    if not dispatch.ok:
        status_code = dispatch.status_code
        _close_response(dispatch)
        raise RuntimeError(f'Failed to dispatch /jobs/logs: HTTP {status_code}')
    request_id = dispatch.headers.get(server_constants.STREAM_REQUEST_HEADER) \
        or dispatch.headers.get('X-SkyPilot-Request-ID')
    if not request_id:
        _close_response(dispatch)
        raise RuntimeError(
            '/jobs/logs response missing X-SkyPilot-Request-ID header')

    stream_url = (f'/api/stream?request_id={request_id}'
                  '&format=plain&compress=gz')
    try:
        stream_resp = server_common.make_authenticated_request('GET',
                                                               stream_url,
                                                               stream=True,
                                                               timeout=(5,
                                                                        None))
    except BaseException:
        _close_response(dispatch)
        raise
    if not stream_resp.ok:
        status_code = stream_resp.status_code
        _close_response(stream_resp)
        _close_response(dispatch)
        raise RuntimeError(
            f'Failed to attach to /api/stream: HTTP {status_code}')

    # Drain the dispatch body in a background thread. Cancelling/closing
    # would tell the API server the client disconnected and the running
    # tail_logs task would be cancelled, leaving /api/stream with only
    # a partial log. Start only after the consumer stream is attached, then
    # let this thread exclusively own and close the dispatch response.
    def _drain() -> None:
        try:
            for _ in dispatch.iter_content(chunk_size=64 * 1024):
                pass
        except Exception:  # pylint: disable=broad-except
            pass
        finally:
            _close_response(dispatch)

    try:
        threading.Thread(target=_drain, daemon=True).start()
    except BaseException:
        _close_response(stream_resp)
        _close_response(dispatch)
        raise

    # Save into a per-job directory matching the legacy download_logs
    # shape (<dir>/controller.log or <dir>/run.log) so existing scripts
    # that grep <path>/controller.log keep working. Decompress on the
    # client when the server gzipped the stream — older API servers
    # without compress=gz support silently ignore the query param and
    # return text/plain, so sniff Content-Type and skip decompression
    # in that case.
    content_type = (stream_resp.headers.get('Content-Type') or '').lower()
    is_gzipped = content_type.startswith('application/gzip')
    decompressor = (zlib.decompressobj(16 +
                                       zlib.MAX_WBITS) if is_gzipped else None)
    log_type = 'controller' if controller else 'job'
    log_filename = 'controller.log' if controller else 'run.log'
    job_label = job_id if job_id is not None else (name or 'latest')
    job_dir = (pathlib.Path(local_dir).expanduser() / 'managed_jobs' /
               f'managed-{log_type}-{job_label}')
    job_dir.mkdir(parents=True, exist_ok=True)
    local_path = job_dir / log_filename

    def _remove_local_artifact() -> None:
        try:
            local_path.unlink()
        except FileNotFoundError:
            pass
        try:
            job_dir.rmdir()
        except OSError:
            pass

    bytes_written = 0
    try:
        with open(local_path, 'wb') as f:
            for chunk in stream_resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                out = decompressor.decompress(chunk) if decompressor else chunk
                if out:
                    f.write(out)
                    bytes_written += len(out)
            if decompressor is not None:
                tail_bytes = decompressor.flush()
                if tail_bytes:
                    f.write(tail_bytes)
                    bytes_written += len(tail_bytes)
    except BaseException:
        _remove_local_artifact()
        raise
    finally:
        _close_response(stream_resp)

    if bytes_written == 0:
        # Server sent nothing (e.g., terminal job, worker cluster gone) —
        # the underlying tail_logs has no source. Remove the empty file
        # + dir and return None so the caller falls back to sync-down.
        _remove_local_artifact()
        return None

    key = int(job_id) if job_id is not None else 0
    return {key: str(job_dir)}


def download_logs(name: str | None, job_id: int | None, refresh: bool,
                  controller: bool, local_dir: str) -> dict[int, str]:
    """Download managed job logs through the legacy sync-down path."""
    body = payloads.JobsDownloadLogsBody(
        name=name,
        job_id=job_id,
        refresh=refresh,
        controller=controller,
        local_dir=local_dir,
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/jobs/download_logs',
        json=json.loads(body.model_dump_json()),
        timeout=(5, None))
    request_id: server_common.RequestId[dict[
        str, str]] = server_common.get_request_id(response)
    job_id_remote_path_dict = sdk.stream_and_get(request_id)
    remote2local_path_dict = client_common.download_logs_from_api_server(
        job_id_remote_path_dict.values())
    return {
        int(job_id): remote2local_path_dict[remote_path]
        for job_id, remote_path in job_id_remote_path_dict.items()
    }
