"""Strict load-balancer client for the stable async-ledger API."""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import re
from typing import Any
import uuid

import fastapi
import httpx
import rfc8785

from sky.serve import constants
from sky.serve import serve_utils

IDENTITY_REQUEST_ATTR = '_skyserve_async_ledger_identity'
RECEIPT_REQUEST_ATTR = '_skyserve_async_ledger_receipt'

_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_VALID_STATES = frozenset((
    'REJECTED_PRE_DISPATCH',
    'DISPATCH_MAY_HAVE_OCCURRED',
    'ACCEPTED',
    'AMBIGUOUS',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED',
    'EXPIRED',
))
_TERMINAL_STATES = frozenset(('SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED'))


@dataclasses.dataclass(frozen=True)
class AsyncLedgerIdentity:
    """Validated execution, platform-job, and immutable-intent identities."""

    request_id: str
    intent_sha256: str
    stable_job_id: str

    @property
    def request_key_sha256(self) -> str:
        return hashlib.sha256(self.request_id.encode('utf-8')).hexdigest()


@dataclasses.dataclass(frozen=True)
class AsyncLedgerReceipt:
    """Exact stable-API acknowledgement for one current attempt."""

    request_key_sha256: str
    attempt_id: str
    attempt_no: int
    state: str
    revision: int
    duplicate: bool
    dispatch_authorized: bool


class AsyncLedgerTransportError(RuntimeError):
    """The correctness API did not return a usable durable receipt."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class AsyncLedgerRouteAuthorityConflict(AsyncLedgerTransportError):
    """A typed pre-bind conflict that permits fresh route selection only."""


def _raw_header_values(request: fastapi.Request, header_name: str) -> list[str]:
    """Read one security-relevant header without coalescing duplicates."""
    raw_headers = getattr(request.headers, 'raw', None)
    target = header_name.lower().encode('ascii')
    raw_values: list[bytes | str] = []
    if isinstance(raw_headers, (list, tuple)):
        for name, value in raw_headers:
            normalized = (name.lower() if isinstance(name, bytes) else
                          str(name).lower().encode('ascii'))
            if normalized == target:
                raw_values.append(value)
    else:
        for name, value in request.headers.items():
            if str(name).lower() == header_name.lower():
                raw_values.append(value)
    values = []
    for raw in raw_values:
        try:
            values.append(
                raw.decode('ascii') if isinstance(raw, bytes) else str(raw))
        except UnicodeDecodeError:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f'{header_name} must contain ASCII.') from None
    return values


def has_identity_declaration(request: fastapi.Request) -> bool:
    """Return whether body-backed async-ledger identity was declared."""
    return any(
        _raw_header_values(request, header_name) for header_name in (
            constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER,
            constants.LB_ASYNC_SERVICE_INCARNATION_HEADER,
            constants.LB_ASYNC_INTENT_SHA256_HEADER,
            constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER,
        ))


def require_service_incarnation(request: fastapi.Request,
                                current_service_hash: str | None) -> str:
    """Fence an exact operation to the caller-prepared service incarnation."""
    values = _raw_header_values(request,
                                constants.LB_ASYNC_SERVICE_INCARNATION_HEADER)
    if len(values) != 1 or not values[0] or len(values[0]) > 128:
        raise fastapi.HTTPException(
            status_code=400,
            detail=('Exact async ledger operations require exactly one '
                    'bounded service-incarnation header.'))
    expected = values[0]
    if not current_service_hash:
        raise fastapi.HTTPException(
            status_code=503,
            detail='The load balancer has no service incarnation fence.',
            headers={'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)})
    if expected != current_service_hash:
        raise fastapi.HTTPException(
            status_code=409,
            detail=('The prepared request belongs to a different service '
                    'incarnation; dispatch is forbidden.'))
    return expected


def validate_identity_declaration(
    request: fastapi.Request,
    advertised_protocol_version: int | None,
    current_service_hash: str | None,
) -> AsyncLedgerIdentity | None:
    """Validate exact declaration headers before reading/accounting a body."""
    state = vars(request)
    cached = state.get(IDENTITY_REQUEST_ATTR)
    if isinstance(cached, AsyncLedgerIdentity):
        return cached
    protocol_values = _raw_header_values(
        request, constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER)
    incarnation_values = _raw_header_values(
        request, constants.LB_ASYNC_SERVICE_INCARNATION_HEADER)
    intent_values = _raw_header_values(request,
                                       constants.LB_ASYNC_INTENT_SHA256_HEADER)
    execution_id_values = _raw_header_values(
        request, constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER)
    forbidden = sum((
        _raw_header_values(request, constants.LB_ASYNC_ATTEMPT_ID_HEADER),
        _raw_header_values(request, constants.LB_ASYNC_ATTEMPT_NO_HEADER),
        _raw_header_values(request, constants.LB_ASYNC_LEDGER_REVISION_HEADER),
        _raw_header_values(request, constants.LB_ASYNC_LEDGER_STATE_HEADER),
    ), [])
    if forbidden:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Async ledger receipt headers are server-owned.')
    if not any((protocol_values, incarnation_values, intent_values,
                execution_id_values)):
        return None
    require_service_incarnation(request, current_service_hash)
    if (len(protocol_values) != 1 or len(intent_values) != 1 or
            len(execution_id_values) != 1):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Async ledger protocol, intent, and execution request ID '
            'headers must each appear exactly once.')
    if protocol_values[0] != str(constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Async ledger protocol version is unsupported.')
    if advertised_protocol_version != constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION:
        raise fastapi.HTTPException(
            status_code=503,
            detail='Async ledger authority is not synchronized.',
            headers={'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)})
    if request.method.upper() != 'POST':
        raise fastapi.HTTPException(
            status_code=405,
            detail='Ledger-qualified asynchronous submissions use POST.')
    intent = intent_values[0]
    if _SHA256_RE.fullmatch(intent) is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Async intent must be a lowercase SHA-256 digest.')
    stable_job_ids = _raw_header_values(request, constants.LB_JOB_ID_HEADER)
    if (len(stable_job_ids) != 1 or not stable_job_ids[0] or
            len(stable_job_ids[0])
            > constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Ledger-qualified requests require exactly one bounded '
            'stable job ID.')
    execution_request_id = execution_id_values[0]
    if (not execution_request_id or len(execution_request_id)
            > constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Execution request ID header must be non-empty and bounded.')
    content_types = _raw_header_values(request, 'Content-Type')
    if (len(content_types) != 1 or
            content_types[0].partition(';')[0].strip().lower()
            != 'application/json'):
        raise fastapi.HTTPException(
            status_code=415,
            detail='Ledger-qualified requests require application/json.')
    content_encodings = _raw_header_values(request, 'Content-Encoding')
    if (len(content_encodings) > 1 or
        (content_encodings and
         content_encodings[0].strip().lower() not in ('', 'identity'))):
        raise fastapi.HTTPException(
            status_code=415,
            detail='Ledger-qualified request bodies must use identity '
            'encoding.')
    return AsyncLedgerIdentity(request_id=execution_request_id,
                               intent_sha256=intent,
                               stable_job_id=stable_job_ids[0])


def _canonical_execution_request_id(body: bytes, max_body_bytes: int) -> str:
    """Validate the canonical protocol-1 envelope and return its execution ID."""
    if (type(max_body_bytes) is not int or max_body_bytes < 1 or
            len(body) > max_body_bytes):
        raise fastapi.HTTPException(
            status_code=413,
            detail='Ledger-qualified request body exceeds the configured '
            'load-balancer request-body ceiling.')

    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        payload = dict(pairs)
        if len(payload) != len(pairs):
            raise ValueError('Duplicate JSON member.')
        return payload

    def _invalid_constant(value: str) -> None:
        raise ValueError(f'Invalid JSON constant {value}.')

    try:
        payload = json.loads(body,
                             object_pairs_hook=_object,
                             parse_constant=_invalid_constant)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError,
            OverflowError):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Ledger-qualified request body must be canonical JSON.'
        ) from None
    try:
        canonical = rfc8785.dumps(payload)
    except (rfc8785.CanonicalizationError, RecursionError):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Ledger-qualified request body must be canonical JSON.'
        ) from None
    if not isinstance(payload, dict) or canonical != body:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Ledger-qualified request body must be a canonical JSON '
            'object.')
    if (set(payload) != {'action', 'request_id', 'payload'} or
            payload.get('action') != 'async_predict'):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Async ledger protocol requires the exact async_predict '
            'envelope.')
    request_id = payload.get('request_id')
    if (not isinstance(request_id, str) or not request_id or len(request_id)
            > constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Ledger-qualified payload requires one bounded execution '
            'request_id.')
    return request_id


def parse_identity(
    request: fastapi.Request,
    advertised_protocol_version: int | None,
    current_service_hash: str | None,
    body: bytes | None = None,
    max_body_bytes: int = constants.LB_REQUEST_QUEUE_MAX_BODY_BYTES
) -> AsyncLedgerIdentity | None:
    """Validate explicit protocol opt-in before installing ledger identity."""
    state = vars(request)
    cached = state.get(IDENTITY_REQUEST_ATTR)
    if isinstance(cached, AsyncLedgerIdentity):
        return cached
    identity = validate_identity_declaration(request,
                                             advertised_protocol_version,
                                             current_service_hash)
    if identity is None:
        return None
    if body is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Ledger-qualified request body was not validated.')
    payload_request_id = _canonical_execution_request_id(body, max_body_bytes)
    if identity.request_id != payload_request_id:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Execution request ID header must exactly match the '
            'canonical payload request_id.')
    state[IDENTITY_REQUEST_ATTR] = identity
    return identity


def get_identity(request: fastapi.Request) -> AsyncLedgerIdentity | None:
    identity = vars(request).get(IDENTITY_REQUEST_ATTR)
    return identity if isinstance(identity, AsyncLedgerIdentity) else None


def get_receipt(request: fastapi.Request) -> AsyncLedgerReceipt | None:
    receipt = vars(request).get(RECEIPT_REQUEST_ATTR)
    return receipt if isinstance(receipt, AsyncLedgerReceipt) else None


def set_receipt(request: fastapi.Request, receipt: AsyncLedgerReceipt) -> None:
    vars(request)[RECEIPT_REQUEST_ATTR] = receipt


def _parse_receipt(payload: Any) -> AsyncLedgerReceipt:
    expected = {
        'request_key_sha256', 'attempt_id', 'attempt_no', 'state', 'revision',
        'duplicate', 'dispatch_authorized'
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned a malformed receipt.')
    try:
        attempt = uuid.UUID(payload['attempt_id'])
    except (AttributeError, TypeError, ValueError):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned a malformed receipt.') from None
    if (str(attempt) != payload['attempt_id'] or
            _SHA256_RE.fullmatch(payload['request_key_sha256']) is None or
            type(payload['attempt_no']) is not int or
            not 1 <= payload['attempt_no'] <= (1 << 63) - 1 or
            type(payload['revision']) is not int or
            not 1 <= payload['revision'] <= (1 << 63) - 1 or
            payload['state'] not in _VALID_STATES or
            type(payload['duplicate']) is not bool or
            type(payload['dispatch_authorized']) is not bool):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned a malformed receipt.')
    return AsyncLedgerReceipt(**payload)


def validate_bind_receipt(
        identity: AsyncLedgerIdentity, receipt: AsyncLedgerReceipt,
        prior_receipt: AsyncLedgerReceipt | None) -> AsyncLedgerReceipt:
    """Reject a shape-valid receipt that cannot authorize this exact send."""
    valid_new_attempt = (
        receipt.request_key_sha256 == identity.request_key_sha256 and
        receipt.dispatch_authorized is True and receipt.duplicate is False and
        receipt.state == 'DISPATCH_MAY_HAVE_OCCURRED' and
        receipt.revision == 1 and
        ((prior_receipt is None) or
         (prior_receipt is not None and
          prior_receipt.state == 'REJECTED_PRE_DISPATCH' and
          receipt.attempt_no == prior_receipt.attempt_no + 1 and
          receipt.attempt_id != prior_receipt.attempt_id)))
    valid_duplicate = (receipt.request_key_sha256 == identity.request_key_sha256
                       and receipt.dispatch_authorized is False and
                       receipt.duplicate is True)
    if not valid_new_attempt and not valid_duplicate:
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned an invalid bind receipt.')
    return receipt


def validate_lookup_receipt(identity: AsyncLedgerIdentity,
                            receipt: AsyncLedgerReceipt) -> AsyncLedgerReceipt:
    """Fence a read-only current-attempt receipt to the submitted identity."""
    if (receipt.request_key_sha256 != identity.request_key_sha256 or
            receipt.dispatch_authorized is not False or
            receipt.duplicate is not True):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned an invalid lookup receipt.')
    return receipt


def validate_transition_receipt(identity: AsyncLedgerIdentity,
                                prior: AsyncLedgerReceipt,
                                receipt: AsyncLedgerReceipt,
                                operation: str) -> AsyncLedgerReceipt:
    """Fence one transition acknowledgement to its exact bound attempt."""
    allowed_states = {
        'accepted': frozenset(('ACCEPTED',)) | _TERMINAL_STATES,
        'ambiguous': frozenset(('AMBIGUOUS',)) | _TERMINAL_STATES,
        'rejected': frozenset(('REJECTED_PRE_DISPATCH',)),
        'terminal': _TERMINAL_STATES,
    }.get(operation)
    exact_identity = (receipt.request_key_sha256 == identity.request_key_sha256
                      and receipt.attempt_id == prior.attempt_id and
                      receipt.attempt_no == prior.attempt_no and
                      receipt.dispatch_authorized is False)
    exact_revision = (
        (not receipt.duplicate and receipt.revision == prior.revision + 1) or
        (receipt.duplicate and receipt.revision >= prior.revision))
    if (allowed_states is None or receipt.state not in allowed_states or
            not exact_identity or not exact_revision):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned an invalid transition receipt.')
    return receipt


def validate_predispatch_rejection(
        identity: AsyncLedgerIdentity,
        receipt: AsyncLedgerReceipt) -> AsyncLedgerReceipt:
    if (receipt.request_key_sha256 != identity.request_key_sha256 or
            receipt.state != 'REJECTED_PRE_DISPATCH' or
            receipt.dispatch_authorized is not False):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned an invalid rejection receipt.')
    return receipt


def validate_terminal_observation_receipt(
        request_id: str, attempt_id: str, attempt_no: int,
        minimum_revision: int, terminal_state: str,
        receipt: AsyncLedgerReceipt) -> AsyncLedgerReceipt:
    """Fence an out-of-band completion acknowledgement before aggregation."""
    try:
        canonical_attempt_id = str(uuid.UUID(attempt_id))
    except (AttributeError, TypeError, ValueError):
        canonical_attempt_id = None
    if (not isinstance(request_id, str) or not request_id or
            canonical_attempt_id != attempt_id or type(attempt_no) is not int or
            not 1 <= attempt_no <= (1 << 63) - 1 or
            type(minimum_revision) is not int or minimum_revision < 1 or
            terminal_state not in _TERMINAL_STATES):
        raise AsyncLedgerTransportError(
            503, 'Async ledger terminal identity is malformed.')
    request_key = hashlib.sha256(request_id.encode('utf-8')).hexdigest()
    exact_revision = (
        (not receipt.duplicate and receipt.revision > minimum_revision) or
        (receipt.duplicate and receipt.revision >= minimum_revision))
    if (receipt.request_key_sha256 != request_key or
            receipt.attempt_id != attempt_id or
            receipt.attempt_no != attempt_no or
            receipt.state != terminal_state or
            receipt.state not in _TERMINAL_STATES or
            receipt.dispatch_authorized is not False or not exact_revision):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned an invalid terminal receipt.')
    return receipt


def validate_terminal_lookup_receipt(
        request_id: str, attempt_id: str, attempt_no: int,
        minimum_revision: int,
        receipt: AsyncLedgerReceipt) -> AsyncLedgerReceipt:
    """Resolve the LB revision handoff for a later terminal reporter."""
    try:
        canonical_attempt_id = str(uuid.UUID(attempt_id))
    except (AttributeError, TypeError, ValueError):
        canonical_attempt_id = None
    request_key = (hashlib.sha256(request_id.encode('utf-8')).hexdigest()
                   if isinstance(request_id, str) and request_id else None)
    if (canonical_attempt_id != attempt_id or type(attempt_no) is not int or
            not 1 <= attempt_no <= (1 << 63) - 1 or
            type(minimum_revision) is not int or minimum_revision < 1 or
            receipt.request_key_sha256 != request_key or
            receipt.attempt_id != attempt_id or
            receipt.attempt_no != attempt_no or
            receipt.revision < minimum_revision or
            receipt.state == 'REJECTED_PRE_DISPATCH' or
            receipt.dispatch_authorized is not False or
            receipt.duplicate is not True):
        raise AsyncLedgerTransportError(
            503, 'Async ledger returned an invalid terminal lookup receipt.')
    return receipt


class AsyncRequestLedgerClient:
    """Bounded authenticated transport to the stable API ledger route."""

    def __init__(self, controller_url: str) -> None:
        self._url = controller_url + constants.LB_ASYNC_REQUEST_LEDGER_PATH
        self._request_slots = asyncio.Semaphore(
            constants.LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY)
        self._lookup_slots = asyncio.Semaphore(
            constants.LB_ASYNC_REQUEST_LEDGER_MAX_LOOKUP_CONCURRENCY)
        self._client = httpx.AsyncClient(
            timeout=constants.LB_ASYNC_REQUEST_LEDGER_TIMEOUT_SECONDS,
            limits=httpx.Limits(max_connections=constants.
                                LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY,
                                max_keepalive_connections=constants.
                                LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY))

    async def close(self) -> None:
        await self._client.aclose()

    async def _post_raw(self, payload: dict[str, Any],
                        service_hash: str) -> tuple[int, Any]:
        read_only_lookup = (payload.get('operation') == 'bind' and
                            payload.get('allow_new_attempt') is False)
        if read_only_lookup:
            async with self._lookup_slots:
                return await self._post_raw_bounded(payload, service_hash)
        return await self._post_raw_bounded(payload, service_hash)

    async def _post_raw_bounded(self, payload: dict[str, Any],
                                service_hash: str) -> tuple[int, Any]:
        """Use a small transport window while queued coroutines stay passive."""
        async with self._request_slots:
            return await self._post_raw_in_slot(payload, service_hash)

    async def _post_raw_in_slot(self, payload: dict[str, Any],
                                service_hash: str) -> tuple[int, Any]:
        tokens = serve_utils.get_lb_sync_auth_tokens(required=True)
        token_attempts: tuple[str | None, ...] = (tokens if tokens else (None,))
        for token_index, token in enumerate(token_attempts):
            headers = {constants.SERVICE_HASH_HEADER: service_hash}
            if token is not None:
                headers['Authorization'] = f'Bearer {token}'
            try:
                response = await self._client.post(self._url,
                                                   json=payload,
                                                   headers=headers)
            except httpx.RequestError as error:
                raise AsyncLedgerTransportError(
                    503, 'Async ledger persistence is unavailable.') from error
            if response.status_code == 401 and token_index + 1 < len(
                    token_attempts):
                continue
            response_payload = None
            if len(response.content
                  ) <= constants.LB_ASYNC_REQUEST_LEDGER_MAX_BYTES:
                try:
                    response_payload = response.json()
                except (UnicodeDecodeError, ValueError):
                    pass
            return response.status_code, response_payload
        raise AsyncLedgerTransportError(401,
                                        'Async ledger authentication failed.')

    @staticmethod
    def _receipt_or_error(status_code: int, payload: Any) -> AsyncLedgerReceipt:
        if status_code != 200:
            detail = (payload.get('detail')
                      if isinstance(payload, dict) else None)
            if (status_code == 409 and isinstance(detail, str) and detail and
                    isinstance(payload, dict) and payload.get('error_code')
                    == constants.LB_ASYNC_LEDGER_ROUTE_AUTHORITY_CONFLICT_CODE):
                raise AsyncLedgerRouteAuthorityConflict(status_code, detail)
            raise AsyncLedgerTransportError(
                status_code, detail or 'Async ledger persistence failed.')
        return _parse_receipt(payload)

    async def post(self, payload: dict[str, Any],
                   service_hash: str) -> AsyncLedgerReceipt:
        status_code, response_payload = await self._post_raw(
            payload, service_hash)
        return self._receipt_or_error(status_code, response_payload)

    async def lookup(self, payload: dict[str, Any],
                     service_hash: str) -> AsyncLedgerReceipt | None:
        """Read the current attempt; 404 means no attempt and creates none."""
        status_code, response_payload = await self._post_raw(
            payload, service_hash)
        if status_code == 404:
            return None
        return self._receipt_or_error(status_code, response_payload)
