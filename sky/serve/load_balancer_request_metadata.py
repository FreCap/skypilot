"""Scheduling request metadata policy for the SkyServe load balancer."""

# This module implements historical private SkyServeLoadBalancer methods.
# pylint: disable=protected-access

from typing import Any

import fastapi

from sky.serve import constants


def _priority_header_error(detail: str) -> fastapi.HTTPException:
    return fastapi.HTTPException(
        status_code=400,
        detail=f'{constants.LB_REQUEST_PRIORITY_HEADER} {detail}')


def _parse_request_priority(cls, request: fastapi.Request) -> int:
    """Parse the scheduling priority without coalescing duplicate headers."""
    headers = request.headers
    raw_headers = getattr(headers, 'raw', None)
    values: list[bytes | str] = []
    if isinstance(raw_headers, (list, tuple)):
        for name, value in raw_headers:
            normalized_name = (name.lower() if isinstance(name, bytes) else
                               str(name).lower().encode('ascii'))
            if normalized_name == constants.LB_REQUEST_PRIORITY_HEADER_BYTES:
                values.append(value)
    else:
        # Starlette always exposes raw headers. This fallback keeps direct
        # unit-test requests and compatible ASGI request implementations
        # working without weakening duplicate detection on the real path.
        for name, value in headers.items():
            if str(name).lower() == (
                    constants.LB_REQUEST_PRIORITY_HEADER.lower()):
                values.append(value)
    if not values:
        return constants.LB_REQUEST_PRIORITY_MIN
    if len(values) != 1:
        raise cls._priority_header_error('must appear at most once.')
    value = values[0]
    try:
        text = (value.decode('ascii')
                if isinstance(value, bytes) else str(value))
    except UnicodeDecodeError:
        raise cls._priority_header_error(
            'must be an integer from 0 to 100.') from None
    if not text or any(
            character < '0' or character > '9' for character in text):
        raise cls._priority_header_error('must be an integer from 0 to 100.')
    # Strip leading zeroes before bounding the conversion. This preserves
    # the public integer contract while preventing Python's decimal-string
    # conversion limit from escaping as HTTP 500 on a very long header.
    normalized_text = text.lstrip('0') or '0'
    if len(normalized_text) > len(str(constants.LB_REQUEST_PRIORITY_MAX)):
        raise cls._priority_header_error('must be an integer from 0 to 100.')
    priority = int(normalized_text)
    if not (constants.LB_REQUEST_PRIORITY_MIN <= priority <=
            constants.LB_REQUEST_PRIORITY_MAX):
        raise cls._priority_header_error('must be an integer from 0 to 100.')
    return priority


def _accelerator_header_error(detail: str,
                              status_code: int = 400) -> fastapi.HTTPException:
    headers = ({
        'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)
    } if status_code == 503 else None)
    return fastapi.HTTPException(
        status_code=status_code,
        detail=(f'{constants.LB_REQUEST_ACCELERATORS_HEADER} {detail}'),
        headers=headers)


def _parse_request_accelerators(
        self, request: fastapi.Request) -> tuple[str, ...] | None:
    """Parse and canonicalize the optional ordered exact-card set.

    None is returned only for a legacy controller/LB pair and an omitted
    header. An explicit header never widens when the catalog is unknown.
    """
    headers = request.headers
    raw_headers = getattr(headers, 'raw', None)
    values: list[bytes | str] = []
    if isinstance(raw_headers, (list, tuple)):
        for name, value in raw_headers:
            normalized_name = (name.lower() if isinstance(name, bytes) else
                               str(name).lower().encode('ascii'))
            if (normalized_name ==
                    constants.LB_REQUEST_ACCELERATORS_HEADER_BYTES):
                values.append(value)
    else:
        for name, value in headers.items():
            if str(name).lower() == (
                    constants.LB_REQUEST_ACCELERATORS_HEADER.lower()):
                values.append(value)

    configured = self._configured_accelerators
    if not values:
        return configured
    if len(values) != 1:
        raise self._accelerator_header_error('must appear at most once.')
    if (self._request_accelerator_compatibility_version
            != constants.LB_REQUEST_ACCELERATORS_VERSION or configured is None):
        raise self._accelerator_header_error(
            'cannot be honored until the controller publishes the exact '
            'accelerator catalog; retry after synchronization.',
            status_code=503)
    value = values[0]
    if isinstance(value, bytes):
        if len(value) > constants.LB_REQUEST_ACCELERATORS_MAX_BYTES:
            raise self._accelerator_header_error('is too large.')
        try:
            text = value.decode('ascii')
        except UnicodeDecodeError:
            raise self._accelerator_header_error(
                'must contain ASCII exact accelerator identifiers.') from None
    else:
        text = str(value)
        if len(text.encode('utf-8')) > (
                constants.LB_REQUEST_ACCELERATORS_MAX_BYTES):
            raise self._accelerator_header_error('is too large.')
        if not text.isascii():
            raise self._accelerator_header_error(
                'must contain ASCII exact accelerator identifiers.')
    raw_items = text.split(',')
    if (not raw_items or
            len(raw_items) > constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS):
        raise self._accelerator_header_error(
            f'must contain 1-{constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS} '
            'exact accelerator identifiers.')
    configured_by_name = {
        accelerator.casefold(): accelerator for accelerator in configured
    }
    result: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_items:
        item = raw_item.strip(' \t')
        normalized = item.casefold()
        if not item:
            raise self._accelerator_header_error(
                'must not contain empty accelerator identifiers.')
        if normalized in seen:
            raise self._accelerator_header_error(
                f'contains duplicate accelerator {item!r}.')
        seen.add(normalized)
        canonical = configured_by_name.get(normalized)
        if canonical is None:
            raise self._accelerator_header_error(
                f'contains unknown exact accelerator {item!r}.')
        result.append(canonical)
    return tuple(result)


def _headers_without_request_priority(request: fastapi.Request) -> Any:
    """Return upstream headers with every scheduling header removed."""
    headers = request.headers
    raw_headers = getattr(headers, 'raw', None)
    if isinstance(raw_headers, (list, tuple)):
        scheduling_headers = {
            constants.LB_REQUEST_PRIORITY_HEADER_BYTES,
            constants.LB_REQUEST_ACCELERATORS_HEADER_BYTES,
            constants.LB_ASYNC_ATTEMPT_ID_HEADER.lower().encode('ascii'),
            constants.LB_ASYNC_ATTEMPT_NO_HEADER.lower().encode('ascii'),
            constants.LB_ASYNC_LEDGER_REVISION_HEADER.lower().encode('ascii'),
            constants.LB_ASYNC_LEDGER_STATE_HEADER.lower().encode('ascii'),
        }
        return [(name, value)
                for name, value in raw_headers
                if (name.lower() if isinstance(name, bytes) else str(name).
                    lower().encode('ascii')) not in scheduling_headers]
    scheduling_headers_text = {
        constants.LB_REQUEST_PRIORITY_HEADER.lower(),
        constants.LB_REQUEST_ACCELERATORS_HEADER.lower(),
        constants.LB_ASYNC_ATTEMPT_ID_HEADER.lower(),
        constants.LB_ASYNC_ATTEMPT_NO_HEADER.lower(),
        constants.LB_ASYNC_LEDGER_REVISION_HEADER.lower(),
        constants.LB_ASYNC_LEDGER_STATE_HEADER.lower(),
    }
    return [(name, value)
            for name, value in headers.items()
            if str(name).lower() not in scheduling_headers_text]


_HISTORICAL_MODULE = 'sky.serve.load_balancer'
for _name in (
        '_priority_header_error',
        '_parse_request_priority',
        '_accelerator_header_error',
        '_parse_request_accelerators',
        '_headers_without_request_priority',
):
    _function = globals()[_name]
    _function.__module__ = _HISTORICAL_MODULE
    _function.__qualname__ = f'SkyServeLoadBalancer.{_name}'
