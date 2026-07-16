"""Stable API-server proxy for external SkyServe load balancers.

The per-service controller is a child process owned by one API-server pod.  A
load balancer must not use a Kubernetes Service that spreads sync requests over
all API pods: only the owner has the controller socket.  This endpoint gives
the load balancer a stable API-server address while resolving the current owner
from shared Serve state for every sync.
"""

import asyncio
import ipaddress

import aiohttp
import fastapi

from sky import sky_logging
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)

router = fastapi.APIRouter()

CONTROLLER_SYNC_ROUTE_PATH = (
    '/api/internal/serve/{service_name}/controller/load_balancer_sync')
CONTROLLER_HISTORY_SYNC_ROUTE_PATH = (
    '/api/internal/serve/{service_name}/controller/'
    'load_balancer_request_history_sync')
_CONTROLLER_SYNC_ROUTE_PREFIX = '/api/internal/serve/'
_CONTROLLER_SYNC_ROUTE_SUFFIXES = (
    '/controller/load_balancer_sync',
    '/controller/load_balancer_request_history_sync',
)
_CONTROLLER_SYNC_TARGET_PATH = '/controller/load_balancer_sync'
_CONTROLLER_HISTORY_SYNC_TARGET_PATH = (
    '/controller/load_balancer_request_history_sync')

# (durable service incarnation, controller process, normalized IP, port).
# Every member participates in the before/after comparison so same-name
# replacement, PID reuse, and controller migration all fail closed.
_ControllerOwner = tuple[str, int, str, int]


def is_controller_sync_path(path: str) -> bool:
    """Whether ``path`` is exactly an internal LB-controller sync route."""
    if not path.startswith(_CONTROLLER_SYNC_ROUTE_PREFIX):
        return False
    for suffix in _CONTROLLER_SYNC_ROUTE_SUFFIXES:
        if not path.endswith(suffix):
            continue
        service_name = path[len(_CONTROLLER_SYNC_ROUTE_PREFIX):-len(suffix)]
        return bool(service_name) and '/' not in service_name
    return False


def _get_controller_owner(service_name: str) -> _ControllerOwner | None:
    """Read and validate the service controller's authoritative address."""
    record = serve_state.get_service_controller_owner(service_name)
    if record is None:
        return None

    service_hash = record.get('hash')
    service_status = record.get('status')
    controller_ip = record.get('controller_ip')
    controller_port = record.get('controller_port')
    controller_pid = record.get('controller_pid')
    if not isinstance(service_hash, str) or not service_hash:
        return None
    # CONTROLLER_FAILED is deliberately routable: service supervision uses it
    # for an unavailable external LB, and the replacement LB must sync through
    # this proxy before its readiness probe can pass and heal the status. Only
    # teardown states fence new syncs.
    if (not isinstance(service_status, serve_state.ServiceStatus) or
            service_status in (serve_state.ServiceStatus.SHUTTING_DOWN,
                               serve_state.ServiceStatus.FAILED_CLEANUP)):
        return None
    if (not isinstance(controller_pid, int) or
            isinstance(controller_pid, bool) or controller_pid <= 0):
        return None
    if not isinstance(controller_ip, str) or not controller_ip:
        return None
    if (not isinstance(controller_port, int) or
            isinstance(controller_port, bool) or
            not 1 <= controller_port <= 65535):
        return None

    try:
        normalized_ip = str(ipaddress.ip_address(controller_ip))
    except ValueError:
        return None
    return service_hash, controller_pid, normalized_ip, controller_port


def _controller_sync_url(owner: _ControllerOwner, target_path: str) -> str:
    _, _, controller_ip, controller_port = owner
    # RFC 3986 requires brackets around an IPv6 literal in a URL authority.
    host = f'[{controller_ip}]' if ':' in controller_ip else controller_ip
    return f'http://{host}:{controller_port}{target_path}'


def _service_unavailable(detail: str) -> fastapi.responses.JSONResponse:
    return fastapi.responses.JSONResponse(status_code=503,
                                          content={'detail': detail})


async def _read_controller_owner(service_name: str) -> _ControllerOwner | None:
    # Serve state uses synchronous SQLAlchemy.  Keep that I/O off the API
    # server event loop, especially because every running LB syncs regularly.
    return await asyncio.to_thread(_get_controller_owner, service_name)


async def _proxy_controller_sync(service_name: str, request: fastapi.Request,
                                 target_path: str) -> fastapi.Response:
    """Forward one LB sync to the service's current controller owner.

    The request is deliberately never retried: the sync carries a drained
    request batch or a cumulative history snapshot. The owner tuple is checked
    again after the response; a concurrent ownership transfer makes the result
    unusable even if the old owner replied.
    """
    try:
        owner_before = await _read_controller_owner(service_name)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to resolve the SkyServe controller owner for '
                       f'{service_name!r}: {e}')
        return _service_unavailable('Controller owner is unavailable.')
    if owner_before is None:
        return _service_unavailable(
            'Controller owner is missing or incomplete.')

    expected_service_hash = request.headers.get(constants.SERVICE_HASH_HEADER)
    if expected_service_hash != owner_before[0]:
        return fastapi.responses.JSONResponse(
            status_code=409,
            content={'detail': 'Service incarnation mismatch.'})

    # The outer internal-auth middleware has already validated this header.
    # Forward the same credential to the controller's sync-only dependency.
    authorization = request.headers.get('authorization')
    if authorization is None:
        return fastapi.responses.JSONResponse(
            status_code=401,
            content={'detail': 'Controller sync authentication required.'})

    body = await request.body()
    forwarded_headers = {
        'Authorization': authorization,
        'Content-Type': request.headers.get('content-type', 'application/json'),
        constants.CONTROLLER_OWNER_HEADER:
            serve_utils.make_controller_owner_fingerprint(*owner_before),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    _controller_sync_url(owner_before, target_path),
                    data=body,
                    headers=forwarded_headers,
                    timeout=aiohttp.ClientTimeout(
                        total=constants.LB_CONTROLLER_PROXY_TIMEOUT_SECONDS),
                    # Following a redirect would issue another request and
                    # violate the at-most-once contract for this POST.
                    allow_redirects=False,
            ) as controller_response:
                response_body = await controller_response.read()
                response_status = controller_response.status
                response_content_type = controller_response.headers.get(
                    'Content-Type')
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning('Failed to connect to the SkyServe controller for '
                       f'{service_name!r}: {e}')
        return _service_unavailable('Controller connection failed.')

    try:
        owner_after = await _read_controller_owner(service_name)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to verify the SkyServe controller owner for '
                       f'{service_name!r}: {e}')
        return _service_unavailable('Controller ownership could not be '
                                    'verified.')
    if owner_after != owner_before:
        return _service_unavailable('Controller ownership changed during '
                                    'the request.')

    response_headers = {}
    if response_content_type is not None:
        response_headers['Content-Type'] = response_content_type
    return fastapi.Response(content=response_body,
                            status_code=response_status,
                            headers=response_headers)


@router.post(CONTROLLER_SYNC_ROUTE_PATH, include_in_schema=False)
async def proxy_load_balancer_sync(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    return await _proxy_controller_sync(service_name, request,
                                        _CONTROLLER_SYNC_TARGET_PATH)


@router.post(CONTROLLER_HISTORY_SYNC_ROUTE_PATH, include_in_schema=False)
async def proxy_load_balancer_request_history_sync(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    return await _proxy_controller_sync(service_name, request,
                                        _CONTROLLER_HISTORY_SYNC_TARGET_PATH)
