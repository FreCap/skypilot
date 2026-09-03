"""Async client-side Python SDK for SkyPilot.

All functions will return a future that can be awaited on.

Usage example:

.. code-block:: python

    request_id = await sky.status()
    statuses = await sky.get(request_id)

"""
import asyncio
import dataclasses
import logging
import typing
from typing import Any, Optional, Union

import aiohttp
import colorama

from sky import admin_policy
from sky import catalog
from sky import exceptions
from sky import sky_logging
from sky.client import common as client_common
from sky.client import interactive_utils
from sky.client import request_results
from sky.client import sdk
from sky.events import api_models as event_api_models
from sky.schemas.api import responses
from sky.server import common as server_common
from sky.server import rest
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import common
from sky.utils import common_utils
from sky.utils import env_options
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import io

    import sky
    from sky import backends
    from sky import models
    from sky.provision.kubernetes import utils as kubernetes_utils
    from sky.skylet import autostop_lib
    from sky.skylet import job_lib

logger = sky_logging.init_logger(__name__)
logging.getLogger('httpx').setLevel(logging.CRITICAL)


@dataclasses.dataclass
class StreamConfig:
    """Configuration class for stream_and_get behavior.

    Attributes:
        log_path: The path to the log file to stream.
        tail: The number of lines to show from the end of the logs.
            If None, show all logs.
        follow: Whether to follow the logs.
        output_stream: The output stream to write to. If None, print to the
            console.
    """
    log_path: str | None = None
    tail: int | None = None
    follow: bool = True
    output_stream: Optional['io.TextIOBase'] = None


DEFAULT_STREAM_CONFIG = StreamConfig()

_REQUEST_RESULT_MAX_ATTEMPTS = 3
_TRANSIENT_RESULT_HTTP_STATUSES = frozenset((502, 503, 504))


def _is_transient_result_error(error: Exception) -> bool:
    return isinstance(error,
                      (aiohttp.ClientError, asyncio.TimeoutError,
                       ConnectionError, exceptions.RequestInterruptedError))


async def _get_request_response(session: 'aiohttp.ClientSession',
                                request_id: str) -> 'aiohttp.ClientResponse':
    """Read and buffer one result with bounded same-ID retries."""
    backoff = common_utils.Backoff(initial_backoff=1, max_backoff_factor=5)
    last_error: Exception | None = None
    for attempt in range(_REQUEST_RESULT_MAX_ATTEMPTS):
        response = None
        try:
            response = await server_common.make_authenticated_request_async(
                session,
                'GET',
                f'/api/get?request_id={request_id}',
                retry=False,
                raise_for_server_unavailable=False,
                timeout=aiohttp.ClientTimeout(
                    total=None,
                    connect=client_common.
                    API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                    sock_read=client_common.
                    API_SERVER_REQUEST_RESULT_READ_TIMEOUT_SECONDS))
            # aiohttp returns after headers.  Buffering inside this retry
            # boundary is essential: the result ACK can be lost while reading
            # the body, after a successful status line was received.
            await response.read()
        except asyncio.CancelledError:
            if response is not None:
                response.close()
            raise
        except Exception as error:  # pylint: disable=broad-except
            if response is not None:
                response.close()
            last_error = error
            if (not _is_transient_result_error(error) or
                    attempt == _REQUEST_RESULT_MAX_ATTEMPTS - 1):
                break
        else:
            if request_results.is_request_result_retry_required(
                    response.status, response.headers, request_id):
                response.close()
                raise exceptions.RequestResultShouldRetryError(request_id)
            if response.status not in _TRANSIENT_RESULT_HTTP_STATUSES:
                return response
            last_error = RuntimeError(
                f'HTTP {response.status} while observing request result')
            response.close()
            if attempt == _REQUEST_RESULT_MAX_ATTEMPTS - 1:
                break
        await asyncio.sleep(backoff.current_backoff())

    assert last_error is not None
    raise exceptions.RequestResultUnavailableError(
        request_id, common_utils.format_exception(last_error)) from last_error


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
async def get(request_id: str) -> Any:
    """Async version of get() that waits for and gets the result of a request.

    Args:
        request_id: The request ID of the request to get.

    Returns:
        The ``Request Returns`` of the specified request. See the documentation
        of the specific requests above for more details.

    Raises:
        Exception: It raises the same exceptions as the specific requests,
            see ``Request Raises`` in the documentation of the specific requests
            above.
    """
    async with aiohttp.ClientSession() as session:
        response = await _get_request_response(session, request_id)

        try:
            request_task = None
            decode_error: Exception | None = None
            try:
                response_payload = await response.json()
                if not isinstance(response_payload, dict):
                    raise TypeError('request result body is not an object')
                encoded_request = None
                if response.status == 200:
                    encoded_request = response_payload
                elif response.status == 500:
                    encoded_request = response_payload.get('detail')
                if not isinstance(encoded_request, dict):
                    raise TypeError('request result payload is not an object')
                request_task = requests_lib.Request.decode(
                    payloads.RequestPayload(**encoded_request))
            except Exception as error:  # pylint: disable=broad-except
                decode_error = error
            if request_task is None:
                message = 'malformed request result'
                if decode_error is not None:
                    message += (
                        ': '
                        f'{common_utils.format_exception(decode_error)}')
                raise exceptions.RequestResultUnavailableError(
                    request_id, message) from decode_error
            request_error = None
            try:
                request_error = request_task.get_error()
            except Exception as decode_error:  # pylint: disable=broad-except
                raise exceptions.RequestResultUnavailableError(
                    request_id, 'malformed request error: '
                    f'{common_utils.format_exception(decode_error)}'
                ) from decode_error
            if request_error is not None:
                error_obj = (request_error.get('object') if isinstance(
                    request_error, dict) else None)
                if not isinstance(error_obj, BaseException):
                    raise exceptions.RequestResultUnavailableError(
                        request_id, 'malformed request error object')
            else:
                error_obj = None
            protocol_error = request_results.request_result_protocol_error(
                response.status, request_task.status, error_obj is not None)
            if protocol_error is not None:
                raise exceptions.RequestResultUnavailableError(
                    request_id, protocol_error)
            if error_obj is not None:
                logger.debug(f'Got request with error: {request_task.name}')
                if env_options.Options.SHOW_DEBUG_INFO.get():
                    stacktrace = getattr(error_obj, 'stacktrace',
                                         str(error_obj))
                    logger.error('=== Traceback on SkyPilot API Server ===\n'
                                 f'{stacktrace}')
                with ux_utils.print_exception_no_traceback():
                    raise error_obj
            if request_task.status == requests_lib.RequestStatus.CANCELLED:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.RequestCancelled(
                        f'{colorama.Fore.YELLOW}Current {request_task.name!r} '
                        f'request ({request_task.request_id}) is cancelled by '
                        f'another process. {colorama.Style.RESET_ALL}')
            try:
                return request_task.get_return_value()
            except Exception as decode_error:  # pylint: disable=broad-except
                raise exceptions.RequestResultUnavailableError(
                    request_id, 'malformed request return value: '
                    f'{common_utils.format_exception(decode_error)}'
                ) from decode_error
        finally:
            response.close()


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
async def stream_response_async(request_id: str | None,
                                response: 'aiohttp.ClientResponse',
                                output_stream: Optional['io.TextIOBase'] = None,
                                resumable: bool = False,
                                get_result: bool = True) -> Any:
    """Async version of stream_response that streams the response to the
    console.

    Args:
        request_id: The request ID.
        response: The aiohttp response.
        output_stream: The output stream to write to. If None, print to the
            console.
        resumable: Whether the response is resumable on retry. If True, the
            streaming will start from the previous failure point on retry.

    Returns:
        Result of request_id if given. Will only return if get_result is True.
    """

    # Always fetch the retry context (if any) so we can report progress to
    # the retry decorator across all stream types. `resumable` only controls
    # whether already-printed lines are skipped on retry.
    retry_context = rest.get_retry_context()
    try:
        line_count = 0

        async for line in rich_utils.decode_rich_status_async(response):
            if line is not None:
                line_count += 1

                line = await interactive_utils.handle_interactive_auth_async(
                    line)
                if line is None:
                    # Line was consumed by interactive auth handler
                    continue

                if (resumable and retry_context is not None and
                        line_count <= retry_context.line_processed):
                    # Already printed on a previous attempt; skip.
                    continue

                print(line, flush=True, end='', file=output_stream)

                if retry_context is not None:
                    if resumable:
                        # Reaching here implies line_count > line_processed
                        # (otherwise the resumable skip above would have
                        # `continue`'d). Advance the high-water mark.
                        retry_context.line_processed = line_count
                    # Report forward progress to the retry decorator so it
                    # can reset the consecutive-failure counter even for
                    # non-resumable streams.
                    retry_context.progress_count += 1
        if request_id is not None and get_result:
            return await get(request_id)
    except Exception:  # pylint: disable=broad-except
        logger.debug(f'To stream request logs: sky api logs {request_id}')
        raise


async def _stream_and_get(
    request_id: str | None = None,
    config: StreamConfig = DEFAULT_STREAM_CONFIG,
) -> Any:
    """Streams the logs of a request or a log file and gets the final result.
    """
    return await stream_and_get(
        request_id,
        config.log_path,
        config.tail,
        config.follow,
        config.output_stream,
    )


async def stream_and_get(
    request_id: str | None = None,
    log_path: str | None = None,
    tail: int | None = None,
    follow: bool = True,
    output_stream: Optional['io.TextIOBase'] = None,
) -> Any:
    """Streams the logs of a request or a log file and gets the final result.

    This will block until the request is finished. The request id can be a
    prefix of the full request id.

    Args:
        request_id: The prefix of the request ID of the request to stream.
        config: Configuration for streaming behavior.

    Returns:
        The ``Request Returns`` of the specified request. See the documentation
        of the specific requests above for more details.

    Raises:
        Exception: It raises the same exceptions as the specific requests,
            see ``Request Raises`` in the documentation of the specific requests
            above.
    """
    params = {
        'request_id': request_id,
        'log_path': log_path,
        'tail': str(tail) if tail is not None else None,
        'follow': str(follow).lower(),  # Convert boolean to string for aiohttp
        'format': 'console',
    }
    # Filter out None values
    params = {k: v for k, v in params.items() if v is not None}

    async with aiohttp.ClientSession() as session:
        response = await server_common.make_authenticated_request_async(
            session,
            'GET',
            '/api/stream',
            params=params,
            retry=False,
            timeout=aiohttp.ClientTimeout(
                total=None,
                connect=client_common.
                API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS))

        try:
            if response.status in [404, 400]:
                detail = (await response.json()).get('detail')
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(f'Failed to stream logs: {detail}')
            elif response.status != 200:
                # TODO(syang): handle the case where the requestID is not
                # provided. https://github.com/skypilot-org/skypilot/issues/6549
                if request_id is None:
                    return None
                return await get(request_id)

            return await stream_response_async(request_id, response,
                                               output_stream)
        finally:
            response.close()


@usage_lib.entrypoint
@annotations.client_api
async def check(
    infra_list: tuple[str, ...] | None,
    verbose: bool,
    workspace: str | None = None,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> dict[str, list[str]]:
    """Async version of check() that checks the credentials to enable clouds."""
    request_id = await asyncio.to_thread(sdk.check, infra_list, verbose,
                                         workspace)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def enabled_clouds(
        workspace: str | None = None,
        expand: bool = False,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> list[str]:
    """Async version of enabled_clouds() that gets the enabled clouds."""
    request_id = await asyncio.to_thread(sdk.enabled_clouds, workspace, expand)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def list_accelerators(
    gpus_only: bool = True,
    name_filter: str | None = None,
    region_filter: str | None = None,
    quantity_filter: int | None = None,
    clouds: list[str] | str | None = None,
    all_regions: bool = False,
    require_price: bool = True,
    case_sensitive: bool = True,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> dict[str, list[catalog.common.InstanceTypeInfo]]:
    """Async version of list_accelerators() that lists the names of all
    accelerators offered by Sky."""
    request_id = await asyncio.to_thread(sdk.list_accelerators, gpus_only,
                                         name_filter, region_filter,
                                         quantity_filter, clouds, all_regions,
                                         require_price, case_sensitive)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def list_accelerator_counts(
    gpus_only: bool = True,
    name_filter: str | None = None,
    region_filter: str | None = None,
    quantity_filter: int | None = None,
    clouds: list[str] | str | None = None,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> dict[str, list[int]]:
    """Async version of list_accelerator_counts() that lists all accelerators
      offered by Sky and available counts."""
    request_id = await asyncio.to_thread(sdk.list_accelerator_counts, gpus_only,
                                         name_filter, region_filter,
                                         quantity_filter, clouds)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def optimize(
        dag: 'sky.Dag',
        minimize: common.OptimizeTarget = common.OptimizeTarget.COST,
        admin_policy_request_options: admin_policy.RequestOptions | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> 'sky.Dag':
    """Async version of optimize() that finds the best execution plan for the
      given DAG."""
    request_id = await asyncio.to_thread(sdk.optimize, dag, minimize,
                                         admin_policy_request_options)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def workspaces(
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> dict[str, Any]:
    """Async version of workspaces() that gets the workspaces."""
    request_id = await asyncio.to_thread(sdk.workspaces)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def launch(
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str | None = None,
    retry_until_up: bool = False,
    idle_minutes_to_autostop: int | None = None,
    wait_for: Optional['autostop_lib.AutostopWaitFor'] = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
    optimize_target: common.OptimizeTarget = common.OptimizeTarget.COST,
    no_setup: bool = False,
    clone_disk_from: str | None = None,
    fast: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False,
    _is_launched_by_jobs_controller: bool = False,
    _is_launched_by_sky_serve_controller: bool = False,
    _disable_controller_check: bool = False,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG,
) -> tuple[int | None, Optional['backends.ResourceHandle']]:
    """Async version of launch() that launches a cluster or task."""
    request_id = await asyncio.to_thread(
        sdk.launch, task, cluster_name, retry_until_up,
        idle_minutes_to_autostop, wait_for, dryrun, down, backend,
        optimize_target, no_setup, clone_disk_from, fast, _need_confirmation,
        _is_launched_by_jobs_controller, _is_launched_by_sky_serve_controller,
        _disable_controller_check)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def exec(  # pylint: disable=redefined-builtin
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str | None = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG,
) -> tuple[int | None, Optional['backends.ResourceHandle']]:
    """Async version of exec() that executes a task on an existing cluster."""
    request_id = await asyncio.to_thread(sdk.exec, task, cluster_name, dryrun,
                                         down, backend)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def tail_logs(cluster_name: str,
                    job_id: int | None,
                    follow: bool,
                    tail: int = 0,
                    output_stream: Optional['io.TextIOBase'] = None) -> int:
    """Async version of tail_logs() that tails the logs of a job."""
    return await asyncio.to_thread(sdk.tail_logs, cluster_name, job_id, follow,
                                   tail, output_stream)


@usage_lib.entrypoint
@annotations.client_api
async def download_logs(cluster_name: str,
                        job_ids: list[str] | None) -> dict[str, str]:
    """Async version of download_logs() that downloads the logs of jobs."""
    return await asyncio.to_thread(sdk.download_logs, cluster_name, job_ids)


@usage_lib.entrypoint
@annotations.client_api
async def start(
    cluster_name: str,
    idle_minutes_to_autostop: int | None = None,
    wait_for: Optional['autostop_lib.AutostopWaitFor'] = None,
    retry_until_up: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    force: bool = False,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG,
) -> 'backends.CloudVmRayResourceHandle':
    """Async version of start() that restarts a cluster."""
    request_id = await asyncio.to_thread(sdk.start, cluster_name,
                                         idle_minutes_to_autostop, wait_for,
                                         retry_until_up, down, force)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def down(
        cluster_name: str,
        purge: bool = False,
        graceful: bool = False,
        graceful_timeout: int | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of down() that tears down a cluster."""
    request_id = await asyncio.to_thread(sdk.down, cluster_name, purge,
                                         graceful, graceful_timeout)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def stop(
        cluster_name: str,
        purge: bool = False,
        graceful: bool = False,
        graceful_timeout: int | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of stop() that stops a cluster."""
    request_id = await asyncio.to_thread(sdk.stop, cluster_name, purge,
                                         graceful, graceful_timeout)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def autostop(
    cluster_name: str,
    idle_minutes: int,
    wait_for: Optional['autostop_lib.AutostopWaitFor'] = None,
    down: bool = False,  # pylint: disable=redefined-outer-name
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> None:
    """Async version of autostop() that schedules an autostop/autodown for a
      cluster."""
    request_id = await asyncio.to_thread(sdk.autostop, cluster_name,
                                         idle_minutes, wait_for, down)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def queue(
    cluster_name: str,
    skip_finished: bool = False,
    all_users: bool = False,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> list[responses.ClusterJobRecord]:
    """Async version of queue() that gets the job queue of a cluster."""
    request_id = await asyncio.to_thread(sdk.queue, cluster_name, skip_finished,
                                         all_users)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def job_status(
    cluster_name: str,
    job_ids: list[int] | None = None,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> dict[int | None, Optional['job_lib.JobStatus']]:
    """Async version of job_status() that gets the status of jobs on a
      cluster."""
    request_id = await asyncio.to_thread(sdk.job_status, cluster_name, job_ids)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def cancel(
        cluster_name: str,
        all: bool = False,  # pylint: disable=redefined-builtin
        all_users: bool = False,
        job_ids: list[int] | None = None,
        # pylint: disable=invalid-name
        _try_cancel_if_cluster_is_init: bool = False,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of cancel() that cancels jobs on a cluster."""
    request_id = await asyncio.to_thread(sdk.cancel, cluster_name, all,
                                         all_users, job_ids,
                                         _try_cancel_if_cluster_is_init)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def status(
    cluster_names: list[str] | None = None,
    refresh: common.StatusRefreshMode = common.StatusRefreshMode.NONE,
    all_users: bool = False,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG,
    *,
    workspaces_filter: list[str] | None = None,
    _include_credentials: bool = False,
) -> list[dict[str, Any]]:
    """Async version of status() that gets cluster statuses."""
    if workspaces_filter is None:
        request_id = await asyncio.to_thread(
            sdk.status,
            cluster_names,
            refresh,
            all_users,
            _include_credentials=_include_credentials)
    else:
        request_id = await asyncio.to_thread(
            sdk.status,
            cluster_names,
            refresh,
            all_users,
            workspaces_filter=workspaces_filter,
            _include_credentials=_include_credentials)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def endpoints(
        cluster: str,
        port: int | str | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> dict[int, str]:
    """Async version of endpoints() that gets the endpoint for a given cluster
      and port number."""
    request_id = await asyncio.to_thread(sdk.endpoints, cluster, port)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def cost_report(
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> list[dict[str, Any]]:
    """Async version of cost_report() that gets all cluster cost reports."""
    request_id = await asyncio.to_thread(sdk.cost_report)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def list_events(**kwargs) -> event_api_models.EventsPage:
    """Async version of list_events()."""
    return await asyncio.to_thread(sdk.list_events, **kwargs)


@usage_lib.entrypoint
@annotations.client_api
async def storage_ls(
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> list[dict[str, Any]]:
    """Async version of storage_ls() that gets the storages."""
    request_id = await asyncio.to_thread(sdk.storage_ls)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def storage_delete(
        name: str,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of storage_delete() that deletes a storage."""
    request_id = await asyncio.to_thread(sdk.storage_delete, name)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def local_up(
        gpus: bool,
        name: str | None = None,
        port_start: int | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of local_up() that launches a Kubernetes cluster on
    local machines."""
    request_id = await asyncio.to_thread(sdk.local_up, gpus, name, port_start)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def local_down(
        name: str | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of local_down() that tears down the Kubernetes cluster
    started by local_up."""
    request_id = await asyncio.to_thread(sdk.local_down, name)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def ssh_up(
        infra: str | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of ssh_up() that deploys the SSH Node Pools defined in
      ~/.sky/ssh_targets.yaml."""
    request_id = await asyncio.to_thread(sdk.ssh_up, infra)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def ssh_down(
        infra: str | None = None,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> None:
    """Async version of ssh_down() that tears down a Kubernetes cluster on SSH
    targets."""
    request_id = await asyncio.to_thread(sdk.ssh_down, infra)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def realtime_kubernetes_gpu_availability(
    context: str | None = None,
    name_filter: str | None = None,
    quantity_filter: int | None = None,
    is_ssh: bool | None = None,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> list[tuple[str, list['models.RealtimeGpuAvailability']]]:
    """Async version of realtime_kubernetes_gpu_availability() that gets the
      real-time Kubernetes GPU availability."""
    request_id = await asyncio.to_thread(
        sdk.realtime_kubernetes_gpu_availability, context, name_filter,
        quantity_filter, is_ssh)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def kubernetes_node_info(
    context: str | None = None,
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> 'models.KubernetesNodesInfo':
    """Async version of kubernetes_node_info() that gets the resource
    information for all the nodes in the cluster."""
    request_id = await asyncio.to_thread(sdk.kubernetes_node_info, context)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def status_kubernetes(
    stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG
) -> tuple[list['kubernetes_utils.KubernetesSkyPilotClusterInfoPayload'],
           list['kubernetes_utils.KubernetesSkyPilotClusterInfoPayload'],
           list[dict[str, Any]], str | None]:
    """Async version of status_kubernetes() that gets all SkyPilot clusters
      and jobs in the Kubernetes cluster."""
    request_id = await asyncio.to_thread(sdk.status_kubernetes)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def api_cancel(
        request_ids: str | list[str] | None = None,
        all_users: bool = False,
        silent: bool = False,
        stream_logs: StreamConfig | None = DEFAULT_STREAM_CONFIG) -> list[str]:
    """Async version of api_cancel() that aborts a request or all requests."""
    request_id = await asyncio.to_thread(sdk.api_cancel, request_ids, all_users,
                                         silent)
    if stream_logs is not None:
        return await _stream_and_get(request_id, stream_logs)
    else:
        return await get(request_id)


@usage_lib.entrypoint
@annotations.client_api
async def api_status(
        request_ids: list[str] | None = None,
        all_status: bool = False,
        limit: int | None = None,
        fields: list[str] | None = None,
        cluster_name: str | None = None,
        cluster_names: list[str] | None = None
) -> list[payloads.RequestPayload]:
    """Async version of api_status() that lists all requests."""
    return await asyncio.to_thread(sdk.api_status, request_ids, all_status,
                                   limit, fields, cluster_name, cluster_names)


@usage_lib.entrypoint
@annotations.client_api
async def dashboard(starting_page: str | None = None) -> None:
    """Async version of dashboard() that starts the dashboard for SkyPilot."""
    return await asyncio.to_thread(sdk.dashboard, starting_page)


@usage_lib.entrypoint
@annotations.client_api
async def api_info() -> responses.APIHealthResponse:
    """Async version of api_info() that gets the server's status, commit and
      version."""
    return await asyncio.to_thread(sdk.api_info)


@usage_lib.entrypoint
@annotations.client_api
async def api_stop() -> None:
    """Async version of api_stop() that stops the API server."""
    return await asyncio.to_thread(sdk.api_stop)


@usage_lib.entrypoint
@annotations.client_api
async def api_server_logs(follow: bool = True, tail: int | None = None) -> None:
    """Async version of api_server_logs() that streams the API server logs."""
    return await asyncio.to_thread(sdk.api_server_logs, follow, tail)


@usage_lib.entrypoint
@annotations.client_api
async def api_login(endpoint: str | None = None,
                    relogin: bool = False,
                    service_account_token: str | None = None,
                    no_browser: bool = False,
                    *,
                    get_token: bool | None = None) -> None:
    """Async version of api_login() that logs into a SkyPilot API server.

    Args:
        endpoint: The endpoint of the SkyPilot API server.
        relogin: Whether to force relogin with OAuth2 when enabled.
        service_account_token: Service account token for authentication.
        no_browser: Whether to avoid opening a local browser.
        get_token: Deprecated alias for ``relogin``.
    """
    if get_token is not None:
        if relogin:
            raise ValueError('Cannot specify both relogin and get_token.')
        relogin = get_token
    return await asyncio.to_thread(sdk.api_login,
                                   endpoint,
                                   relogin=relogin,
                                   service_account_token=service_account_token,
                                   no_browser=no_browser)
