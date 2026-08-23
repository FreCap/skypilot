"""Stable API-server proxy for external SkyServe load balancers.

The per-service controller is a child process owned by one API-server pod.  A
load balancer must not use a Kubernetes Service that spreads sync requests over
all API pods: only the owner has the controller socket.  This endpoint gives
the load balancer a stable API-server address while resolving the current owner
from shared Serve state for every sync.
"""

import asyncio
import ipaddress
import json
import time
from typing import Any

import aiohttp
import fastapi

from sky import sky_logging
from sky.serve import async_request_ledger
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import lb_ha_observability as lb_ha_obs
from sky.serve import route_projection
from sky.serve import serve_state
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)

router = fastapi.APIRouter()

_CONTROLLER_PROXY_ROUTE_PREFIX = '/api/internal/serve/{service_name}'
CONTROLLER_SYNC_ROUTE_PATH = (_CONTROLLER_PROXY_ROUTE_PREFIX +
                              constants.LB_CONTROLLER_SYNC_PATH)
CONTROLLER_ROLE_ROUTE_PATH = (_CONTROLLER_PROXY_ROUTE_PREFIX +
                              constants.LB_CONTROLLER_ROLE_PATH)
CONTROLLER_SYSTEM_RECOVERY_LEASE_ROUTE_PATH = (
    _CONTROLLER_PROXY_ROUTE_PREFIX +
    constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH)
CONTROLLER_HISTORY_SYNC_ROUTE_PATH = (_CONTROLLER_PROXY_ROUTE_PREFIX +
                                      constants.LB_CONTROLLER_HISTORY_SYNC_PATH)
DEMAND_REPORT_ROUTE_PATH = (_CONTROLLER_PROXY_ROUTE_PREFIX +
                            constants.LB_DEMAND_REPORT_PATH)
ASYNC_REQUEST_LEDGER_ROUTE_PATH = (_CONTROLLER_PROXY_ROUTE_PREFIX +
                                   constants.LB_ASYNC_REQUEST_LEDGER_PATH)
_CONTROLLER_SYNC_ROUTE_PREFIX = '/api/internal/serve/'
_CONTROLLER_SYNC_ROUTE_SUFFIXES = (
    constants.LB_CONTROLLER_SYNC_PATH,
    constants.LB_CONTROLLER_ROLE_PATH,
    constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH,
    constants.LB_CONTROLLER_HISTORY_SYNC_PATH,
    constants.LB_DEMAND_REPORT_PATH,
    constants.LB_ASYNC_REQUEST_LEDGER_PATH,
)
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


def _service_unavailable(
        detail: str,
        outcome: lb_ha_obs.LbRoleOutcome | None = None,
        observability: dict | None = None) -> fastapi.responses.JSONResponse:
    content: dict[str, Any] = {'detail': detail}
    if outcome is not None:
        content['outcome'] = outcome.value
    if observability is not None:
        content['proxy_observability'] = observability
    return fastapi.responses.JSONResponse(status_code=503, content=content)


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
    started_at = time.monotonic()
    phases: dict[str, float] = {}
    request_bytes: int | None = None
    is_role_request = target_path == constants.LB_CONTROLLER_ROLE_PATH
    is_recovery_lease_request = (
        target_path == constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH)

    def proxy_observability() -> dict:
        return {
            'total_seconds': max(0.0,
                                 time.monotonic() - started_at),
            'request_bytes': request_bytes,
            'phases_seconds': dict(sorted(phases.items())),
        }

    def unavailable(
            detail: str,
            outcome: lb_ha_obs.LbRoleOutcome) -> fastapi.responses.JSONResponse:
        return _service_unavailable(
            detail, outcome if is_role_request else None,
            proxy_observability() if is_role_request else None)

    owner_read_started_at = time.monotonic()
    try:
        owner_before = await _read_controller_owner(service_name)
    except Exception as e:  # pylint: disable=broad-except
        phases['owner_before'] = time.monotonic() - owner_read_started_at
        logger.warning('Failed to resolve the SkyServe controller owner for '
                       f'{service_name!r}: {e}')
        return unavailable('Controller owner is unavailable.',
                           lb_ha_obs.LbRoleOutcome.PROXY_OWNER_READ_FAILED)
    phases['owner_before'] = time.monotonic() - owner_read_started_at
    if owner_before is None:
        return unavailable('Controller owner is missing or incomplete.',
                           lb_ha_obs.LbRoleOutcome.PROXY_OWNER_MISSING)

    expected_service_hash = request.headers.get(constants.SERVICE_HASH_HEADER)
    if expected_service_hash != owner_before[0]:
        content: dict[str, Any] = {'detail': 'Service incarnation mismatch.'}
        if is_role_request:
            content.update(outcome=lb_ha_obs.LbRoleOutcome.
                           PROXY_INCARNATION_MISMATCH.value,
                           proxy_observability=proxy_observability())
        return fastapi.responses.JSONResponse(status_code=409, content=content)

    # The outer internal-auth middleware has already validated this header.
    # Forward the same credential to the controller's sync-only dependency.
    authorization = request.headers.get('authorization')
    if authorization is None:
        content = {'detail': 'Controller sync authentication required.'}
        if is_role_request:
            content.update(outcome=(
                lb_ha_obs.LbRoleOutcome.PROXY_AUTHENTICATION_REQUIRED.value),
                           proxy_observability=proxy_observability())
        return fastapi.responses.JSONResponse(status_code=401, content=content)

    body = await request.body()
    request_bytes = len(body)
    expected_owner_fingerprint = serve_utils.make_controller_owner_fingerprint(
        *owner_before)
    forwarded_headers = {
        'Authorization': authorization,
        'Content-Type': request.headers.get('content-type', 'application/json'),
        constants.CONTROLLER_OWNER_HEADER: expected_owner_fingerprint,
    }
    forward_started_at = time.monotonic()
    controller_owner_attestation: str | None = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    _controller_sync_url(owner_before, target_path),
                    data=body,
                    headers=forwarded_headers,
                    timeout=aiohttp.ClientTimeout(total=(
                        constants.LB_SYSTEM_RECOVERY_LEASE_PROXY_TIMEOUT_SECONDS
                        if is_recovery_lease_request else constants.
                        LB_CONTROLLER_PROXY_TIMEOUT_SECONDS)),
                    # Following a redirect would issue another request and
                    # violate the at-most-once contract for this POST.
                    allow_redirects=False,
            ) as controller_response:
                response_body = await controller_response.read()
                response_status = controller_response.status
                response_content_type = controller_response.headers.get(
                    'Content-Type')
                controller_owner_attestation = controller_response.headers.get(
                    constants.LB_ROLE_CONTROLLER_OWNER_VERIFIED_HEADER)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        phases['controller_forward'] = time.monotonic() - forward_started_at
        logger.warning('Failed to connect to the SkyServe controller for '
                       f'{service_name!r}: {e}')
        return unavailable(
            'Controller connection failed.',
            lb_ha_obs.LbRoleOutcome.PROXY_CONTROLLER_CONNECTION_FAILED)
    phases['controller_forward'] = time.monotonic() - forward_started_at

    if is_role_request and controller_owner_attestation is not None:
        # The controller emits this only after its complete owner/cutover row
        # has been re-read under the role lock. Matching the exact fingerprint
        # read before routing moves the final owner linearization point into the
        # controller and avoids a redundant proxy SQL query. An explicit bad
        # attestation is never treated as an old-version fallback.
        if controller_owner_attestation != expected_owner_fingerprint:
            return unavailable(
                'Controller ownership attestation changed during the request.',
                lb_ha_obs.LbRoleOutcome.PROXY_OWNER_CHANGED)
    else:
        # Mixed-version role responses carry no attestation and every non-role
        # route retains the historical before/after owner fence.
        owner_verify_started_at = time.monotonic()
        try:
            owner_after = await _read_controller_owner(service_name)
        except Exception as e:  # pylint: disable=broad-except
            phases['owner_after'] = time.monotonic() - owner_verify_started_at
            logger.warning('Failed to verify the SkyServe controller owner for '
                           f'{service_name!r}: {e}')
            return unavailable(
                'Controller ownership could not be verified.',
                lb_ha_obs.LbRoleOutcome.PROXY_OWNER_VERIFICATION_FAILED)
        phases['owner_after'] = time.monotonic() - owner_verify_started_at
        if owner_after != owner_before:
            return unavailable(
                'Controller ownership changed during the request.',
                lb_ha_obs.LbRoleOutcome.PROXY_OWNER_CHANGED)

    response_headers = {}
    if response_content_type is not None:
        response_headers['Content-Type'] = response_content_type
    if is_role_request:
        response_headers[constants.LB_ROLE_PROXY_OBSERVABILITY_HEADER] = (
            json.dumps(proxy_observability(), separators=(',', ':')))
    return fastapi.Response(content=response_body,
                            status_code=response_status,
                            headers=response_headers)


@router.post(CONTROLLER_SYNC_ROUTE_PATH, include_in_schema=False)
async def proxy_load_balancer_sync(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    service_hash = request.headers.get(constants.SERVICE_HASH_HEADER)
    if not service_hash:
        return fastapi.responses.JSONResponse(
            status_code=409,
            content={'detail': 'Service incarnation header is required.'})
    try:
        body = await request.body()
        payload = json.loads(body)
        lb_session_id = (payload.get('lb_session_id') if isinstance(
            payload, dict) else None)
    except json.JSONDecodeError:
        # Legacy controllers own validation until a service is promoted.  A
        # malformed body therefore still follows the exact historical proxy
        # path when the durable mode says LEGACY_PROXY.
        lb_session_id = None
    try:
        decision = await asyncio.to_thread(
            route_projection.RouteProjectionRepository().resolve_sync,
            service_name, service_hash, lb_session_id)
    except route_projection.RouteProjectionConflict as error:
        return fastapi.responses.JSONResponse(status_code=409,
                                              content={'detail': str(error)})
    except route_projection.RouteProjectionUnavailable as error:
        return fastapi.responses.JSONResponse(status_code=503,
                                              content={'detail': str(error)})
    if decision.mode == route_projection.RouteSourceMode.DURABLE_PROJECTED:
        assert decision.response is not None
        response = dict(decision.response)
        # A stored or process-local stale field cannot survive deactivation.
        response.pop('async_request_ledger_protocol_version', None)
        if async_request_ledger.schema_available():
            response['async_request_ledger_protocol_version'] = (
                async_request_ledger.PROTOCOL_VERSION)
        return fastapi.responses.JSONResponse(status_code=200, content=response)
    return await _proxy_controller_sync(service_name, request,
                                        constants.LB_CONTROLLER_SYNC_PATH)


@router.post(DEMAND_REPORT_ROUTE_PATH, include_in_schema=False)
async def record_load_balancer_demand(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    """Persist demand at the stable API server, never at the controller."""
    service_hash = request.headers.get(constants.SERVICE_HASH_HEADER)
    if not service_hash:
        return fastapi.responses.JSONResponse(
            status_code=409,
            content={'detail': 'Service incarnation header is required.'})
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > constants.LB_DEMAND_REPORT_MAX_BYTES:
            return fastapi.responses.JSONResponse(
                status_code=413,
                content={'detail': 'Demand report exceeds the size limit.'})
    try:
        payload = json.loads(body)
        receipt = await asyncio.to_thread(demand_state.ingest_report,
                                          service_name, service_hash, payload)
    except (json.JSONDecodeError, demand_state.DemandReportError) as e:
        status = (409
                  if isinstance(e, demand_state.DemandReportConflict) else 400)
        return fastapi.responses.JSONResponse(status_code=status,
                                              content={'detail': str(e)})
    except demand_state.DemandReportUnavailable as e:
        return fastapi.responses.JSONResponse(status_code=503,
                                              content={'detail': str(e)})
    except Exception as e:  # pylint: disable=broad-except
        logger.exception('Failed to persist demand for %r.', service_name)
        return fastapi.responses.JSONResponse(
            status_code=503,
            content={
                'detail': f'Demand persistence failed: {type(e).__name__}'
            })
    return fastapi.responses.JSONResponse(
        status_code=200,
        content={
            'generation': receipt.generation,
            'duplicate': receipt.duplicate,
            'request_history_accepted': receipt.request_history_accepted,
            'request_classification_history_accepted':
                receipt.request_classification_history_accepted,
            'prediction_time_history_accepted':
                receipt.prediction_time_history_accepted,
        })


@router.post(ASYNC_REQUEST_LEDGER_ROUTE_PATH, include_in_schema=False)
async def record_async_request_ledger(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    """Commit exact dispatch receipts at the stable PostgreSQL boundary."""
    service_hash = request.headers.get(constants.SERVICE_HASH_HEADER)
    if not service_hash:
        return fastapi.responses.JSONResponse(
            status_code=409,
            content={'detail': 'Service incarnation header is required.'})
    media_type = request.headers.get('content-type', '').partition(';')[0]
    if media_type.strip().lower() != 'application/json':
        return fastapi.responses.JSONResponse(
            status_code=415,
            content={'detail': 'Ledger requests must use application/json.'})
    try:
        content_length = request.headers.get('content-length')
        if (content_length is not None and int(content_length)
                > constants.LB_ASYNC_REQUEST_LEDGER_MAX_BYTES):
            return fastapi.responses.JSONResponse(
                status_code=413,
                content={'detail': 'Ledger request exceeds the size limit.'})
    except ValueError:
        pass
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > constants.LB_ASYNC_REQUEST_LEDGER_MAX_BYTES:
            return fastapi.responses.JSONResponse(
                status_code=413,
                content={'detail': 'Ledger request exceeds the size limit.'})
    try:
        payload = json.loads(bytes(body))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError,
            RecursionError):
        return fastapi.responses.JSONResponse(
            status_code=400,
            content={'detail': 'Ledger request must be valid JSON.'})
    if not isinstance(payload, dict):
        return fastapi.responses.JSONResponse(
            status_code=400,
            content={'detail': 'Ledger request must be an object.'})
    operation = payload.get('operation')
    if not async_request_ledger.schema_available():
        return fastapi.responses.JSONResponse(
            status_code=503,
            content={'detail': 'Async request ledger schema is unavailable.'})
    read_only_bind = (operation == 'bind' and
                      payload.get('allow_new_attempt') is False)
    try:
        repository = async_request_ledger.AsyncRequestLedgerRepository()
        if operation == 'bind':
            bind_payload = dict(payload)
            bind_payload.pop('operation')
            if read_only_bind:
                receipt = await asyncio.to_thread(repository.lookup_current,
                                                  service_name, service_hash,
                                                  bind_payload)
            else:
                receipt = await asyncio.to_thread(repository.bind, service_name,
                                                  service_hash, bind_payload)
        elif operation == 'reject_before_dispatch':
            if set(payload) != {
                    'protocol_version', 'operation', 'request_id',
                    'intent_sha256'
            } or payload.get('protocol_version') != (
                    async_request_ledger.PROTOCOL_VERSION):
                raise async_request_ledger.AsyncRequestLedgerError(
                    'Pre-dispatch rejection has an unsupported shape.')
            receipt = await asyncio.to_thread(repository.reject_before_dispatch,
                                              service_name, service_hash,
                                              payload.get('request_id'),
                                              payload.get('intent_sha256'))
        else:
            receipt = await asyncio.to_thread(repository.transition,
                                              service_name, service_hash,
                                              payload)
    except async_request_ledger.AsyncRequestLedgerNotFound as error:
        return fastapi.responses.JSONResponse(status_code=404,
                                              content={'detail': str(error)})
    except async_request_ledger.AsyncRequestLedgerConflict as error:
        return fastapi.responses.JSONResponse(status_code=409,
                                              content={'detail': str(error)})
    except async_request_ledger.AsyncRequestLedgerError as error:
        return fastapi.responses.JSONResponse(status_code=400,
                                              content={'detail': str(error)})
    except async_request_ledger.AsyncRequestLedgerUnavailable as error:
        return fastapi.responses.JSONResponse(status_code=503,
                                              content={'detail': str(error)})
    except Exception as error:  # pylint: disable=broad-except
        logger.exception('Failed to commit an async request receipt for %r.',
                         service_name)
        return fastapi.responses.JSONResponse(
            status_code=503,
            content={
                'detail': 'Ledger persistence failed: '
                          f'{type(error).__name__}'
            })
    return fastapi.responses.JSONResponse(status_code=200,
                                          content=receipt.to_dict())


@router.post(CONTROLLER_ROLE_ROUTE_PATH, include_in_schema=False)
async def proxy_load_balancer_role(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    return await _proxy_controller_sync(service_name, request,
                                        constants.LB_CONTROLLER_ROLE_PATH)


@router.post(CONTROLLER_SYSTEM_RECOVERY_LEASE_ROUTE_PATH,
             include_in_schema=False)
async def proxy_load_balancer_system_recovery_route_lease(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    return await _proxy_controller_sync(
        service_name, request,
        constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH)


@router.post(CONTROLLER_HISTORY_SYNC_ROUTE_PATH, include_in_schema=False)
async def proxy_load_balancer_request_history_sync(
        service_name: str, request: fastapi.Request) -> fastapi.Response:
    return await _proxy_controller_sync(
        service_name, request, constants.LB_CONTROLLER_HISTORY_SYNC_PATH)
