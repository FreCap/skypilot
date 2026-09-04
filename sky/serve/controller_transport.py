"""Fenced HTTP transport for SkyServe controllers."""
import hashlib
import ipaddress
import json
import logging
import os
import time
import typing
from typing import Any

from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.serve import auth_tokens
from sky.serve import constants

if typing.TYPE_CHECKING:
    import requests

    from sky.serve import serve_state
else:
    requests = adaptors_common.LazyImport('requests')
    serve_state = adaptors_common.LazyImport('sky.serve.serve_state')

logger: logging.Logger = sky_logging.init_logger(__name__)

# Retry settings for cross-pod controller HTTP calls. The DB row update of
# `controller_ip` is atomic w.r.t. controller readiness (sky.serve.service
# only flips DB after _wait_for_controller_ready), so the only failure modes
# left are: (1) DB read replica lag right after a recovery; (2) brief network
# blips between pods.
# Intermediate retries log at DEBUG to avoid spamming WARN every refresh tick
# while the controller is intentionally absent (CONTROLLER_INIT /
# SHUTTING_DOWN / FAILED_CLEANUP); the final-attempt failure logs once at
# WARN.
# A single attempt with a tight connect timeout keeps `sky jobs pool status`
# responsive even when one of N pools' controllers is unreachable.
_CONTROLLER_HTTP_RETRY_ATTEMPTS: int = 1
_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS: float = 0.5
# (connect_timeout, read_timeout). Connect timeout matters most: when the
# controller pod is dead/unreachable, kernel ECONNREFUSED is instant on
# loopback but cross-pod TCP can hang for 30s+ if the remote pod silently
# drops SYN (e.g. NetworkPolicy, pod terminating mid-flight). Without an
# explicit timeout, `requests` waits forever and `sky jobs pool status`
# appears to hang. Read timeout is generous because /autoscaler/info on a
# busy controller can take a moment.
_CONTROLLER_HTTP_TIMEOUT_SECONDS: tuple[float, float] = (1.0, 10.0)

get_controller_admin_auth_tokens = auth_tokens.get_controller_admin_auth_tokens


class ControllerOwnerError(RuntimeError):
    """The intended service incarnation has no safe controller target."""


_ControllerOwner = tuple[str, int, str | None, int]


def make_controller_owner_fingerprint(service_hash: object,
                                      controller_pid: object,
                                      controller_ip: object,
                                      controller_port: object) -> str:
    """Return a stable fingerprint for one exact controller owner tuple."""
    if not isinstance(service_hash, str) or not service_hash:
        raise ControllerOwnerError('Controller service hash is missing.')
    if (not isinstance(controller_pid, int) or
            isinstance(controller_pid, bool) or controller_pid <= 0):
        raise ControllerOwnerError(
            'Controller parent PID is missing or invalid.')
    if (not isinstance(controller_port, int) or
            isinstance(controller_port, bool) or
            not 1 <= controller_port <= 65535):
        raise ControllerOwnerError('Controller port is missing or invalid.')
    normalized_ip: str | None = None
    if controller_ip is not None:
        if not isinstance(controller_ip, str) or not controller_ip:
            raise ControllerOwnerError('Controller IP is invalid.')
        try:
            normalized_ip = str(ipaddress.ip_address(controller_ip))
        except ValueError as e:
            raise ControllerOwnerError('Controller IP is invalid.') from e
    payload = json.dumps(
        [service_hash, controller_pid, normalized_ip, controller_port],
        separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _get_controller_url(service_name: str,
                        expected_service_hash: str) -> tuple[str, str]:
    """Resolve and fence the controller HTTP URL.

    In single-pod (or daemon == controller pod) deployments the IP read from
    DB either matches our own POD_IP or is None — in both cases we fall back
    to localhost. In HA where the request handler runs on a different pod
    than the controller process, we route via the controller's pod IP from DB.
    The address and owner fingerprint come from one narrow, atomic row read.
    """
    record = serve_state.get_service_controller_owner(service_name)
    if record is None:
        raise ControllerOwnerError(
            f'Controller owner for {service_name!r} is missing.')
    service_hash = record.get('hash')
    if service_hash != expected_service_hash:
        raise ControllerOwnerError(
            f'Service {service_name!r} was replaced while the controller '
            'request was in flight.')
    service_status = record.get('status')
    if (not isinstance(service_status, serve_state.ServiceStatus) or
            service_status in (serve_state.ServiceStatus.SHUTTING_DOWN,
                               serve_state.ServiceStatus.FAILED_CLEANUP)):
        raise ControllerOwnerError(
            f'Controller owner for {service_name!r} is not routable.')
    controller_pid = record.get('controller_pid')
    controller_port = record.get('controller_port')
    controller_ip = record.get('controller_ip')
    owner_fingerprint = make_controller_owner_fingerprint(
        service_hash, typing.cast(int, controller_pid), controller_ip,
        typing.cast(int, controller_port))
    self_ip = os.environ.get('POD_IP')
    normalized_self_ip = None
    if self_ip is not None:
        try:
            normalized_self_ip = str(ipaddress.ip_address(self_ip))
        except ValueError:
            pass
    normalized_controller_ip = None
    if controller_ip is not None:
        normalized_controller_ip = str(ipaddress.ip_address(controller_ip))
    if (normalized_controller_ip is None or
            normalized_controller_ip == normalized_self_ip):
        url = f'http://localhost:{controller_port}'
    else:
        host = (f'[{normalized_controller_ip}]' if ':'
                in normalized_controller_ip else normalized_controller_ip)
        url = f'http://{host}:{controller_port}'
    logger.debug(f'_get_controller_url for {service_name}: url={url} '
                 f'self_ip={self_ip} controller_ip={controller_ip}')
    return url, owner_fingerprint


def _get_local_controller_url(owner: _ControllerOwner) -> tuple[str, str]:
    """Resolve a specifically supervised local child without consulting DB."""
    service_hash, controller_pid, controller_ip, controller_port = owner
    owner_fingerprint = make_controller_owner_fingerprint(
        service_hash, controller_pid, controller_ip, controller_port)
    return f'http://localhost:{controller_port}', owner_fingerprint


def _request_to_controller_with_retry(method: str,
                                      service_name: str,
                                      expected_service_hash: str,
                                      path: str,
                                      *,
                                      fixed_controller_owner: _ControllerOwner |
                                      None = None,
                                      **kwargs):
    """HTTP `method` to the controller with bounded retry on ConnectionError.
    """
    request_fn = getattr(requests, method)
    # Force a bounded timeout.
    if 'timeout' not in kwargs:
        kwargs['timeout'] = _CONTROLLER_HTTP_TIMEOUT_SECONDS
    # Controller callers use the admin ring, independent of the credential the
    # LB uses for sync. During rotation, retry with the next overlap credential
    # only after a 401: transport failures retry the SAME credential, while a
    # 403/5xx is an application result and must not replay the request.
    admin_tokens = get_controller_admin_auth_tokens()
    owner_header_lower = constants.CONTROLLER_OWNER_HEADER.lower()
    base_headers = {
        name: value
        for name, value in dict(kwargs.pop('headers', {}) or {}).items()
        if str(name).lower() != owner_header_lower
    }
    caller_supplied_auth = any(
        str(name).lower() == 'authorization' for name in base_headers)
    token_index = 0
    for attempt in range(_CONTROLLER_HTTP_RETRY_ATTEMPTS):
        if fixed_controller_owner is None:
            controller_url, owner_fingerprint = _get_controller_url(
                service_name, expected_service_hash)
        else:
            if fixed_controller_owner[0] != expected_service_hash:
                raise ControllerOwnerError(
                    'Fixed controller owner does not match the intended '
                    'service incarnation.')
            controller_url, owner_fingerprint = _get_local_controller_url(
                fixed_controller_owner)
        url = controller_url + path
        try:
            while True:
                headers = dict(base_headers)
                headers[constants.CONTROLLER_OWNER_HEADER] = owner_fingerprint
                if admin_tokens and not caller_supplied_auth:
                    headers['Authorization'] = (
                        f'Bearer {admin_tokens[token_index]}')
                request_kwargs = dict(kwargs)
                if headers:
                    request_kwargs['headers'] = headers
                response = request_fn(url, **request_kwargs)
                if (response.status_code == 401 and not caller_supplied_auth and
                        token_index + 1 < len(admin_tokens)):
                    token_index += 1
                    continue
                return response
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout):
            if attempt < _CONTROLLER_HTTP_RETRY_ATTEMPTS - 1:
                logger.debug(
                    f'Connection to controller {url} failed '
                    f'(attempt {attempt + 1}/'
                    f'{_CONTROLLER_HTTP_RETRY_ATTEMPTS}); retrying after '
                    f'{_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS}s.')
                time.sleep(_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS)
                continue
            logger.warning(
                f'Connection to controller {url} failed after '
                f'{_CONTROLLER_HTTP_RETRY_ATTEMPTS} attempts. '
                'Controller may be down, restarting, or mid-HA-flip.')
            raise


def _post_to_controller_with_retry(service_name: str,
                                   expected_service_hash: str, path: str,
                                   **kwargs):
    return _request_to_controller_with_retry('post', service_name,
                                             expected_service_hash, path,
                                             **kwargs)


def _get_to_controller_with_retry(service_name: str, expected_service_hash: str,
                                  path: str, **kwargs):
    return _request_to_controller_with_retry('get', service_name,
                                             expected_service_hash, path,
                                             **kwargs)


def _is_placement_order_generation(value: Any) -> bool:
    return (isinstance(value, str) and len(value) == 64 and
            all(character in '0123456789abcdef' for character in value))


def _normalize_placement_page(payload: dict[str, Any], *, limit: int,
                              offset: int) -> dict[str, Any]:
    """Normalize paged and rolling-upgrade placement responses."""
    generation_fenced_versions = (
        constants.PLACEMENT_STATE_GENERATION_FENCED_PAGINATION_VERSIONS)
    locations = payload.get('locations')
    if locations is None:
        if payload.get('available') is not False:
            raise ValueError(
                'Available placement-state response must include locations.')
        if payload.get('reason') == 'catalog_order_changed':
            if payload.get(
                    'pagination_version') not in generation_fenced_versions:
                raise ValueError(
                    'Changed placement catalog has an invalid version.')
            order_generation = payload.get('order_generation')
            if not _is_placement_order_generation(order_generation):
                raise ValueError(
                    'Changed placement catalog has an invalid generation.')
        return payload
    if not isinstance(locations, list):
        raise ValueError('Placement-state locations must be a list.')

    pagination_version = payload.get('pagination_version')
    if pagination_version in (
            constants.PLACEMENT_STATE_COMPATIBLE_PAGINATION_VERSIONS):
        if (pagination_version in generation_fenced_versions and
                payload.get('enabled') is not False):
            order_generation = payload.get('order_generation')
            if not _is_placement_order_generation(order_generation):
                raise ValueError('Placement-state order generation is invalid.')
        page_offset = payload.get('page_offset')
        next_offset = payload.get('next_offset')
        total_locations = payload.get('total_locations')
        if page_offset != offset:
            raise ValueError('Placement-state page offset does not match the '
                             'requested offset.')
        if len(locations) > limit:
            raise ValueError('Placement-state page exceeds the requested '
                             'limit.')
        if (not isinstance(total_locations, int) or
                isinstance(total_locations, bool) or total_locations < 0 or
                total_locations > constants.PLACEMENT_STATE_MAX_OFFSET):
            raise ValueError('Placement-state total location count is invalid.')
        page_end = offset + len(locations)
        if locations and page_end > total_locations:
            raise ValueError('Placement-state page exceeds the total location '
                             'count.')
        if offset < total_locations and not locations:
            raise ValueError('Placement-state page is empty before the total '
                             'location count.')
        expected_next_offset = (page_end if offset < total_locations and
                                page_end < total_locations else None)
        if next_offset != expected_next_offset:
            raise ValueError('Placement-state next offset is invalid.')
        return payload
    if pagination_version is not None:
        raise ValueError('Placement-state pagination version is unsupported.')

    # A controller from before pagination ignores the query parameters and
    # returns its complete (historically capped at 500) list. Slice that list
    # in the API process so a rolling upgrade keeps the public response
    # bounded. The longer transport timeout below is intentionally retained
    # until every old controller has exited.
    page = locations[offset:offset + limit]
    page_end = offset + len(page)
    normalized = dict(payload)
    normalized.update({
        'pagination_version':
            constants.PLACEMENT_STATE_LEGACY_PAGINATION_VERSION,
        'page_offset': offset,
        'next_offset': page_end if page_end < len(locations) else None,
        'total_locations': len(locations),
        'locations': page,
        'truncated': (payload.get('truncated') is True or
                      page_end < len(locations)),
    })
    return normalized


def get_service_placement_state(
    service_name: str,
    expected_service_hash: str,
    *,
    limit: int = constants.PLACEMENT_STATE_DEFAULT_PAGE_SIZE,
    offset: int = 0,
    expected_order_generation: str | None = None,
) -> dict[str, Any]:
    """Read one bounded page of a controller's resident placer state."""
    if (not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or
            limit > constants.PLACEMENT_STATE_MAX_PAGE_SIZE):
        raise ValueError('Placement-state limit is invalid.')
    if (not isinstance(offset, int) or isinstance(offset, bool) or offset < 0 or
            offset > constants.PLACEMENT_STATE_MAX_OFFSET):
        raise ValueError('Placement-state offset is invalid.')
    params: dict[str, Any] = {
        'limit': limit,
        'offset': offset,
        'include_paid_admission': True,
    }
    if expected_order_generation is not None:
        params['expected_order_generation'] = expected_order_generation
    response = _get_to_controller_with_retry(
        service_name,
        expected_service_hash,
        constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
        params=params,
        timeout=_CONTROLLER_HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError('Placement-state response must be an object.')
    return _normalize_placement_page(payload, limit=limit, offset=offset)


def _get_to_local_controller_with_retry(service_name: str,
                                        controller_owner: _ControllerOwner,
                                        path: str, **kwargs):
    return _request_to_controller_with_retry(
        'get',
        service_name,
        controller_owner[0],
        path,
        fixed_controller_owner=controller_owner,
        **kwargs)
