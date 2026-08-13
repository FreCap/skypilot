"""Local fanout router for SkyServe's asynchronous request protocol.

This module lets one SkyServe replica expose several local worker processes as
one endpoint.  Workers implement the same asynchronous protocol as a regular
replica: ``async_predict`` accepts work, while ``async_capacity`` reports
``running_count`` and ``predict_concurrency``.

The router is deliberately workload agnostic.  Callers supply the public
routes and worker base URLs; the router knows nothing about model images,
accelerators, or how workers are started.
"""

import argparse
import asyncio
import collections
from collections.abc import AsyncIterator
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import json
import logging
import math
import os
import socket
import time
from typing import Any
from urllib import parse as urlparse

import aiohttp
from aiohttp import web
from multidict import CIMultiDict

from sky.utils import asyncio_utils

_ACTION_CAPACITY = 'async_capacity'
_ACTION_PREDICT = 'async_predict'
_ACTION_STATUS = 'async_status'
_ACTION_CANCEL = 'async_cancel'
_KNOWN_ACTIONS = {
    _ACTION_CAPACITY,
    _ACTION_PREDICT,
    _ACTION_STATUS,
    _ACTION_CANCEL,
}
_ACTIVE_REQUEST_STATUSES = frozenset(
    ('QUEUED', 'PENDING', 'IN_PROGRESS', 'RUNNING'))
_TERMINAL_REQUEST_STATUSES = frozenset(
    ('SUCCEEDED', 'FAILED', 'EXPIRED', 'CANCELED', 'CANCELLED'))
_KNOWN_REQUEST_STATUSES = (_ACTIVE_REQUEST_STATUSES |
                           _TERMINAL_REQUEST_STATUSES | {'NOT_FOUND'})
_HOP_BY_HOP_HEADERS = {
    'connection',
    'content-length',
    'keep-alive',
    'proxy-connection',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
}
_LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class _Reservation:
    """A slot held while a predict is being accepted or becoming visible."""

    settled_at: float | None = None


@dataclasses.dataclass
class _Child:
    base_url: str
    capacity: int = 0
    running: int = 0
    known: bool = False
    last_probe_started_at: float = 0.0
    reservations: dict[int,
                       _Reservation] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class _ProbeSample:
    started_at: float
    capacity: int | None
    running: int | None


@dataclasses.dataclass(frozen=True)
class _ChildResponse:
    status: int
    body: bytes
    headers: Sequence[tuple[str, str]]


@dataclasses.dataclass
class _RequestGate:
    """Serializes predictions that carry the same stable request ID."""

    lock: asyncio.Lock = dataclasses.field(default_factory=asyncio.Lock)
    users: int = 0


@dataclasses.dataclass(frozen=True)
class _Owner:
    child_index: int
    ambiguous: bool


class LocalAsyncRouter:
    """Capacity-aware router over local async workers.

    A single instance owns all mutable routing state.  Its asyncio lock makes
    slot reservations atomic without serializing worker HTTP requests.
    """

    def __init__(
        self,
        upstreams: Sequence[str],
        async_path: str,
        readiness_path: str,
        *,
        probe_timeout_seconds: float = 2.0,
        readiness_timeout_seconds: float = 5.0,
        status_timeout_seconds: float = 5.0,
        request_timeout_seconds: float = 3700.0,
        probe_cache_seconds: float = 0.25,
        reservation_grace_seconds: float = 1.0,
        retriable_status_codes: Iterable[int] = (429,),
        release_and_relay_responses: Mapping[int, str] | None = None,
        max_sticky_requests: int = 10000,
        client_max_size: int = 1024**2,
    ) -> None:
        if not upstreams:
            raise ValueError('At least one upstream is required.')
        normalized_upstreams = [_normalize_upstream(url) for url in upstreams]
        if len(set(normalized_upstreams)) != len(normalized_upstreams):
            raise ValueError('Upstream URLs must be unique.')
        self._children = [_Child(url) for url in normalized_upstreams]
        self._async_path = _normalize_path(async_path, 'async_path')
        self._readiness_path = _normalize_path(readiness_path, 'readiness_path')
        self._probe_timeout_seconds = _positive(probe_timeout_seconds,
                                                'probe_timeout_seconds')
        self._readiness_timeout_seconds = _positive(
            readiness_timeout_seconds, 'readiness_timeout_seconds')
        self._status_timeout_seconds = _positive(status_timeout_seconds,
                                                 'status_timeout_seconds')
        self._request_timeout_seconds = _positive(request_timeout_seconds,
                                                  'request_timeout_seconds')
        self._probe_cache_seconds = _nonnegative(probe_cache_seconds,
                                                 'probe_cache_seconds')
        self._reservation_grace_seconds = _nonnegative(
            reservation_grace_seconds, 'reservation_grace_seconds')
        self._retriable_status_codes = frozenset(retriable_status_codes)
        if any(not isinstance(code, int) or isinstance(code, bool) or
               code < 100 or code > 599
               for code in self._retriable_status_codes):
            raise ValueError(
                'Retriable status codes must be between 100 and 599.')
        self._release_and_relay_responses = dict(release_and_relay_responses or
                                                 {})
        if any(not isinstance(code, int) or isinstance(code, bool) or code < 400
               or code > 599 or not isinstance(state, str) or not state
               for code, state in self._release_and_relay_responses.items()):
            raise ValueError(
                'Release-and-relay responses require a 400..599 status and '
                'nonempty state.')
        if (self._retriable_status_codes &
                self._release_and_relay_responses.keys()):
            raise ValueError('Retriable and release-and-relay status codes '
                             'must be disjoint.')
        if max_sticky_requests < 1:
            raise ValueError('max_sticky_requests must be at least 1.')
        self._max_sticky_requests = max_sticky_requests
        if client_max_size < 1:
            raise ValueError('client_max_size must be at least 1.')
        self._client_max_size = client_max_size

        self._state_lock = asyncio.Lock()
        self._probe_lock = asyncio.Lock()
        self._last_probe_finished_at = 0.0
        self._next_child = 0
        self._next_reservation = 0
        self._owners: collections.OrderedDict[
            str, _Owner] = collections.OrderedDict()
        self._ambiguous_owner_count = 0
        self._request_gates: dict[str, _RequestGate] = {}
        self._session: aiohttp.ClientSession | None = None

    def create_app(self) -> web.Application:
        app = web.Application(client_max_size=self._client_max_size)
        app.router.add_post(self._async_path, self.handle_async)
        app.router.add_get(self._readiness_path, self.handle_readiness)
        app.cleanup_ctx.append(self._session_context)
        return app

    async def _session_context(self,
                               app: web.Application) -> AsyncIterator[None]:
        del app
        connector = aiohttp.TCPConnector(limit=0)
        # Preserve the exact response bytes when relaying content-encoded
        # responses.  Otherwise aiohttp decompresses the body while the proxy
        # forwards the original Content-Encoding header.
        self._session = aiohttp.ClientSession(connector=connector,
                                              auto_decompress=False)
        try:
            yield
        finally:
            await self._session.close()
            self._session = None

    async def handle_async(self, request: web.Request) -> web.StreamResponse:
        try:
            body = await request.read()
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _json_error(400, 'Request body must be JSON.')
        if not isinstance(payload, dict):
            return _json_error(400, 'Request body must be a JSON object.')
        action = payload.get('action')
        if action not in _KNOWN_ACTIONS:
            return _json_error(400, f'Unsupported async action: {action!r}.')

        if action == _ACTION_CAPACITY:
            await self._refresh_capacity(force=True)
            return await self._capacity_response()
        if action == _ACTION_PREDICT:
            return await self._handle_predict(request, body, payload)
        return await self._handle_owned_request(request, body, payload)

    async def handle_readiness(self,
                               request: web.Request) -> web.StreamResponse:
        tasks = [
            asyncio.create_task(
                self._request_child(index, request.method,
                                    request.rel_url.raw_path_qs, b'',
                                    request.headers,
                                    self._readiness_timeout_seconds))
            for index in range(len(self._children))
        ]
        try:
            for completed in asyncio.as_completed(tasks):
                response = await completed
                if response is not None and 200 <= response.status < 300:
                    return _relay(response)
            return _json_error(503, 'No local worker is ready.')
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_predict(self, request: web.Request, body: bytes,
                              payload: Mapping[str, Any]) -> web.StreamResponse:
        request_id = payload.get('request_id')
        if not isinstance(request_id, str):
            return await self._dispatch_predict(request, body, payload)

        gate = await self._retain_request_gate(request_id)
        acquired = False
        try:
            await gate.lock.acquire()
            acquired = True
            owner = await self._owner_record(request_id)
            if owner is not None:
                return await self._handle_duplicate_predict(
                    request, request_id, owner)

            discovered_owner, existing, absence_confirmed = (
                await self._discover_request_owner(request, request_id))
            if discovered_owner is not None and existing is not None:
                await self._remember_owner(request_id,
                                           discovered_owner,
                                           ambiguous=False)
                return _relay(existing)
            if not absence_confirmed:
                return _json_error(
                    502, 'Could not prove that this request ID is absent '
                    'from every local worker; request was not dispatched.')
            return await self._dispatch_predict(request, body, payload)
        finally:
            if acquired:
                gate.lock.release()
            await asyncio.shield(self._release_request_gate(request_id, gate))

    async def _dispatch_predict(
            self, request: web.Request, body: bytes,
            payload: Mapping[str, Any]) -> web.StreamResponse:
        await self._refresh_capacity()
        attempted: set[int] = set()
        last_rejection: _ChildResponse | None = None
        submitted_id = payload.get('request_id')
        while len(attempted) < len(self._children):
            reservation = await self._reserve(attempted)
            if reservation is None:
                break
            child_index, token = reservation
            attempted.add(child_index)
            reservation_finalized = False
            try:
                if isinstance(submitted_id, str):
                    # Claim before dispatch. An ambiguous timeout must retain
                    # this owner so a caller retry cannot escape to another
                    # GPU.
                    claimed = await self._remember_owner(submitted_id,
                                                         child_index,
                                                         ambiguous=True)
                    if not claimed:
                        await self._release(child_index, token)
                        reservation_finalized = True
                        return _json_error(
                            429,
                            'Local request-ownership safety budget is full; '
                            'request was not dispatched.')
                response = await self._request_child(
                    child_index, request.method, request.rel_url.raw_path_qs,
                    body, request.headers, self._request_timeout_seconds)
                release_state = None
                response_payload = None
                if response is not None:
                    release_state = self._release_and_relay_responses.get(
                        response.status)
                    if release_state is not None:
                        response_payload = _strict_response_json(response)
                if (response is not None and release_state is not None and
                        isinstance(submitted_id, str) and
                        response_payload is not None and
                        set(response_payload) == {'state', 'request_id'} and
                        response_payload.get('state') == release_state and
                        response_payload.get('request_id') == submitted_id):
                    try:
                        await self._release_rejected_request(
                            child_index, token, submitted_id)
                    except asyncio.CancelledError:
                        # The response proves this worker did not accept the
                        # request. Finish removing both local claims before
                        # propagating cancellation.
                        await self._complete_rejected_request_release(
                            child_index, token, submitted_id)
                        raise
                    reservation_finalized = True
                    return _relay(response)
                if (response is not None and
                        response.status in self._retriable_status_codes):
                    try:
                        await self._release_rejected_request(
                            child_index, token, submitted_id)
                    except asyncio.CancelledError:
                        # A retriable response proves the worker did not accept
                        # the request. Finish removing both local claims before
                        # propagating cancellation, otherwise the stable ID can
                        # remain blocked as ambiguously owned forever.
                        await self._complete_rejected_request_release(
                            child_index, token, submitted_id)
                        raise
                    reservation_finalized = True
                    last_rejection = response
                    continue

                # A success is accepted. Every other outcome is ambiguous:
                # the worker may have accepted the request before its response
                # was lost, so replaying it could duplicate expensive work.
                await self._settle(child_index, token)
                reservation_finalized = True
            finally:
                # Handler cancellation is ambiguous too. Mark the reservation
                # settled so a later capacity sample can reconcile it instead
                # of leaking an uncollectable slot forever.
                if not reservation_finalized:
                    await asyncio.shield(self._settle(child_index, token))
            request_id = _request_id(payload, response)
            if request_id is not None:
                response_payload = (None if response is None else
                                    _response_json(response))
                confirmed = (response is not None and
                             200 <= response.status < 300 and
                             response_payload is not None and
                             response_payload.get('request_id') == request_id)
                await self._remember_owner(request_id,
                                           child_index,
                                           ambiguous=not confirmed)
            if response is None:
                return _json_error(502, 'Local worker request failed.')
            return _relay(response)

        if last_rejection is not None:
            return _relay(last_rejection)
        return _json_error(429, 'All local worker slots are busy.')

    async def _handle_duplicate_predict(self, request: web.Request,
                                        request_id: str,
                                        owner: _Owner) -> web.StreamResponse:
        response = await self._request_status_child(request, request_id,
                                                    owner.child_index)
        if response is None:
            return _json_error(
                502, 'Owning local worker is unreachable; duplicate '
                'request was not dispatched.')
        status = _status_response_status(response, request_id)
        if not 200 <= response.status < 300 or status is None:
            return _json_error(
                502, 'Owning local worker returned an inconclusive status; '
                'duplicate request was not dispatched.')
        if status == 'NOT_FOUND' and owner.ambiguous:
            # The original dispatch may still be completing after an
            # ambiguous transport outcome. Replaying elsewhere is unsafe.
            return _json_error(
                502, 'Owning local worker has not confirmed this request ID; '
                'duplicate request was not dispatched.')
        if status != 'NOT_FOUND':
            await self._remember_owner(request_id,
                                       owner.child_index,
                                       ambiguous=False)
        return _relay(response)

    async def _discover_request_owner(
            self, request: web.Request,
            request_id: str) -> tuple[int | None, _ChildResponse | None, bool]:
        """Find a pre-restart request, or prove that all workers lack it."""

        async def _request_indexed(
                index: int) -> tuple[int, _ChildResponse | None]:
            return index, await self._request_status_child(
                request, request_id, index)

        tasks = [
            asyncio.create_task(_request_indexed(index))
            for index in range(len(self._children))
        ]
        absence_confirmed = True
        try:
            for completed in asyncio.as_completed(tasks):
                index, response = await completed
                if response is None or not 200 <= response.status < 300:
                    absence_confirmed = False
                    continue
                status = _status_response_status(response, request_id)
                if status is None:
                    absence_confirmed = False
                    continue
                if status != 'NOT_FOUND':
                    return index, response, True
            return None, None, absence_confirmed
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _request_status_child(self, request: web.Request, request_id: str,
                                    child_index: int) -> _ChildResponse | None:
        body = json.dumps({
            'action': _ACTION_STATUS,
            'request_id': request_id,
        }).encode()
        headers = _end_to_end_headers(request.headers.items())
        for name in ('Content-Encoding', 'Content-Type'):
            if name in headers:
                del headers[name]
        headers['Content-Type'] = 'application/json'
        return await self._request_child(child_index, request.method,
                                         request.rel_url.raw_path_qs, body,
                                         headers, self._status_timeout_seconds)

    async def _handle_owned_request(
            self, request: web.Request, body: bytes,
            payload: Mapping[str, Any]) -> web.StreamResponse:
        request_id = payload.get('request_id')
        owner = await self._owner_record(request_id)
        if owner is not None:
            assert isinstance(request_id, str)
            response = await self._request_child(owner.child_index,
                                                 request.method,
                                                 request.rel_url.raw_path_qs,
                                                 body, request.headers,
                                                 self._status_timeout_seconds)
            if response is None:
                return _json_error(502, 'Owning local worker is unreachable.')
            status = _status_response_status(response, request_id)
            definitive_cancel = (payload['action'] == _ACTION_CANCEL and
                                 _is_definitive_cancellation(
                                     response, request_id))
            contradictory_cancel = (payload['action'] == _ACTION_CANCEL and
                                    status in ('CANCELED', 'CANCELLED') and
                                    not definitive_cancel)
            terminal_statuses = _TERMINAL_REQUEST_STATUSES
            if payload['action'] == _ACTION_CANCEL:
                terminal_statuses -= {'CANCELED', 'CANCELLED'}
            terminal = status in terminal_statuses
            safe_absence = status == 'NOT_FOUND' and not owner.ambiguous
            if definitive_cancel or terminal or safe_absence:
                await self._forget_owner(request_id)
            elif (status is not None and status != 'NOT_FOUND' and
                  not contradictory_cancel):
                await self._remember_owner(request_id,
                                           owner.child_index,
                                           ambiguous=False)
            return _relay(response)

        # Ownership can be lost if this router restarts while workers survive.
        # Status and cancel are request-id-addressed, so ask every worker and
        # return the first response that is not an explicit NOT_FOUND.
        async def _request_indexed(
                index: int) -> tuple[int, _ChildResponse | None]:
            response = await self._request_child(index, request.method,
                                                 request.rel_url.raw_path_qs,
                                                 body, request.headers,
                                                 self._status_timeout_seconds)
            return index, response

        tasks = [
            asyncio.create_task(_request_indexed(index))
            for index in range(len(self._children))
        ]
        not_found_fallbacks: dict[int, _ChildResponse] = {}
        error_fallbacks: dict[int, _ChildResponse] = {}
        inconclusive = False
        try:
            for completed in asyncio.as_completed(tasks):
                index, response = await completed
                if response is None:
                    inconclusive = True
                    continue
                if not 200 <= response.status < 300:
                    error_fallbacks[index] = response
                    continue
                status = _status_response_status(response, request_id)
                if status is None:
                    inconclusive = True
                    continue
                if status == 'NOT_FOUND':
                    not_found_fallbacks[index] = response
                    continue
                definitive_cancel = (payload['action'] == _ACTION_CANCEL and
                                     _is_definitive_cancellation(
                                         response, request_id))
                contradictory_cancel = (payload['action'] == _ACTION_CANCEL and
                                        status in ('CANCELED', 'CANCELLED') and
                                        not definitive_cancel)
                if contradictory_cancel:
                    inconclusive = True
                    continue
                terminal_statuses = _TERMINAL_REQUEST_STATUSES
                if payload['action'] == _ACTION_CANCEL:
                    terminal_statuses -= {'CANCELED', 'CANCELLED'}
                terminal = status in terminal_statuses
                if not definitive_cancel and not terminal:
                    assert isinstance(request_id, str)
                    await self._remember_owner(request_id,
                                               index,
                                               ambiguous=False)
                return _relay(response)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        if error_fallbacks:
            return _relay(error_fallbacks[min(error_fallbacks)])
        if inconclusive:
            return _json_error(
                502, 'Local workers returned inconclusive request status.')
        if not_found_fallbacks:
            return _relay(not_found_fallbacks[min(not_found_fallbacks)])
        return _json_error(502, 'All local workers are unreachable.')

    async def _refresh_capacity(self, *, force: bool = False) -> None:
        requested_at = time.monotonic()
        now = requested_at
        if (not force and
                now - self._last_probe_finished_at < self._probe_cache_seconds):
            return
        async with self._probe_lock:
            now = time.monotonic()
            if force and self._last_probe_finished_at >= requested_at:
                # A concurrent forced caller already completed the refresh
                # requested by this caller. Coalesce the wave instead of
                # serially probing every child once per waiting request.
                return
            if (not force and now - self._last_probe_finished_at
                    < self._probe_cache_seconds):
                return
            samples = await asyncio.gather(*[
                self._probe_child(index) for index in range(len(self._children))
            ])
            async with self._state_lock:
                for index, sample in enumerate(samples):
                    self._apply_probe(index, sample)
            self._last_probe_finished_at = time.monotonic()

    async def _probe_child(self, child_index: int) -> _ProbeSample:
        started_at = time.monotonic()
        body = json.dumps({'action': _ACTION_CAPACITY}).encode()
        response = await self._request_child(
            child_index, 'POST', self._async_path, body,
            {'Content-Type': 'application/json'}, self._probe_timeout_seconds)
        if response is None or response.status != 200:
            return _ProbeSample(started_at, None, None)
        try:
            payload = json.loads(response.body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _ProbeSample(started_at, None, None)
        if not isinstance(payload, dict):
            return _ProbeSample(started_at, None, None)
        running = payload.get('running_count')
        capacity = payload.get('predict_concurrency')
        if not _nonnegative_integer(running) or not _nonnegative_integer(
                capacity):
            return _ProbeSample(started_at, None, None)
        return _ProbeSample(started_at, capacity, running)

    def _apply_probe(self, child_index: int, sample: _ProbeSample) -> None:
        child = self._children[child_index]
        if sample.started_at < child.last_probe_started_at:
            return
        child.last_probe_started_at = sample.started_at
        if sample.capacity is None or sample.running is None:
            child.known = False
            return
        for token, reservation in list(child.reservations.items()):
            if (reservation.settled_at is not None and
                    sample.started_at >= reservation.settled_at +
                    self._reservation_grace_seconds):
                del child.reservations[token]
        child.capacity = sample.capacity
        child.running = sample.running
        child.known = True

    async def _capacity_response(self) -> web.Response:
        async with self._state_lock:
            admission_blocked = (self._ambiguous_owner_count
                                 >= self._max_sticky_requests)
            known_children = 0
            running = 0
            total = 0
            for child in self._children:
                reserved = len(child.reservations)
                if child.known:
                    known_children += 1
                    total += child.capacity
                    if child.capacity > 0:
                        running += max(
                            child.running,
                            min(child.capacity, child.running + reserved))
                    else:
                        running += child.running + reserved
                else:
                    # Preserve the last confirmed occupancy across transient
                    # misses. With no prior sample, one conservative busy slot
                    # prevents a fresh router from proving false idleness while
                    # surviving workers may still own requests.
                    running += max(1, child.running, reserved)
            if admission_blocked:
                # The router rejects every new stable request ID while the
                # ambiguity budget is exhausted.  Advertise zero free slots
                # too, otherwise the outer load balancer repeatedly selects
                # an admission-disabled machine and may scale up against
                # capacity that cannot accept work.  Retain the logical width
                # so dashboards and capacity consumers keep machine topology
                # accurate while treating every slot as conservatively busy.
                total = max(
                    total,
                    sum(max(1, child.capacity) for child in self._children))
                running = max(running, total)
        all_children_known = known_children == len(self._children)
        status = ('UNKNOWN' if not all_children_known else
                  'READY' if total > 0 else 'DRAINING')
        return web.json_response({
            'status': status,
            'pod_name': socket.gethostname(),
            'running_count': running,
            'predict_concurrency': total,
        })

    async def _reserve(self, excluded: Iterable[int]) -> tuple[int, int] | None:
        excluded_set = set(excluded)
        async with self._state_lock:
            for offset in range(len(self._children)):
                index = (self._next_child + offset) % len(self._children)
                child = self._children[index]
                if index in excluded_set or not child.known:
                    continue
                free = child.capacity - child.running - len(child.reservations)
                if free <= 0:
                    continue
                self._next_reservation += 1
                token = self._next_reservation
                child.reservations[token] = _Reservation()
                self._next_child = (index + 1) % len(self._children)
                return index, token
        return None

    async def _release(self, child_index: int, token: int) -> None:
        async with self._state_lock:
            self._children[child_index].reservations.pop(token, None)

    async def _release_rejected_request(self, child_index: int, token: int,
                                        request_id: Any) -> None:
        async with self._state_lock:
            self._children[child_index].reservations.pop(token, None)
            if isinstance(request_id, str):
                owner = self._owners.get(request_id)
                if owner is not None and owner.child_index == child_index:
                    self._pop_owner(request_id)

    @asyncio_utils.shield
    async def _complete_rejected_request_release(self, child_index: int,
                                                 token: int,
                                                 request_id: Any) -> None:
        """Finish a rejected request's release through repeated cancellation."""
        await self._release_rejected_request(child_index, token, request_id)

    async def _settle(self, child_index: int, token: int) -> None:
        async with self._state_lock:
            reservation = self._children[child_index].reservations.get(token)
            if reservation is not None:
                reservation.settled_at = time.monotonic()

    async def _remember_owner(self,
                              request_id: str,
                              child_index: int,
                              *,
                              ambiguous: bool = False) -> bool:
        async with self._state_lock:
            previous = self._owners.get(request_id)
            becomes_ambiguous = (ambiguous and
                                 (previous is None or not previous.ambiguous))
            if (becomes_ambiguous and
                    self._ambiguous_owner_count >= self._max_sticky_requests):
                return False
            if becomes_ambiguous:
                self._ambiguous_owner_count += 1
            elif (not ambiguous and previous is not None and
                  previous.ambiguous):
                self._ambiguous_owner_count -= 1
            self._owners[request_id] = _Owner(child_index, ambiguous)
            self._owners.move_to_end(request_id)
            while len(self._owners) > self._max_sticky_requests:
                confirmed_id = next(
                    (owner_id for owner_id, owner in self._owners.items()
                     if not owner.ambiguous), None)
                if confirmed_id is None:
                    break
                self._owners.pop(confirmed_id)
            return True

    async def _owner(self, request_id: Any) -> int | None:
        owner = await self._owner_record(request_id)
        return None if owner is None else owner.child_index

    async def _owner_record(self, request_id: Any) -> _Owner | None:
        if not isinstance(request_id, str):
            return None
        async with self._state_lock:
            owner = self._owners.get(request_id)
            if owner is not None:
                self._owners.move_to_end(request_id)
            return owner

    async def _forget_owner(self, request_id: Any) -> None:
        if isinstance(request_id, str):
            async with self._state_lock:
                self._pop_owner(request_id)

    async def _forget_owner_if(self, request_id: str, child_index: int) -> None:
        async with self._state_lock:
            owner = self._owners.get(request_id)
            if owner is not None and owner.child_index == child_index:
                self._pop_owner(request_id)

    def _pop_owner(self, request_id: str) -> None:
        owner = self._owners.pop(request_id, None)
        if owner is not None and owner.ambiguous:
            self._ambiguous_owner_count -= 1

    async def _retain_request_gate(self, request_id: str) -> _RequestGate:
        async with self._state_lock:
            gate = self._request_gates.get(request_id)
            if gate is None:
                gate = _RequestGate()
                self._request_gates[request_id] = gate
            gate.users += 1
            return gate

    async def _release_request_gate(self, request_id: str,
                                    gate: _RequestGate) -> None:
        async with self._state_lock:
            gate.users -= 1
            if gate.users == 0 and self._request_gates.get(request_id) is gate:
                del self._request_gates[request_id]

    async def _request_child(self, child_index: int, method: str, path: str,
                             body: bytes, headers: Mapping[str, str],
                             timeout_seconds: float) -> _ChildResponse | None:
        if self._session is None:
            raise RuntimeError('LocalAsyncRouter application has not started.')
        url = f'{self._children[child_index].base_url}{path}'
        forwarded_headers = _end_to_end_headers(headers.items(), drop_host=True)
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            async with self._session.request(method,
                                             url,
                                             data=body,
                                             headers=forwarded_headers,
                                             timeout=timeout) as response:
                return _ChildResponse(response.status, await response.read(),
                                      tuple(response.headers.items()))
        except (aiohttp.ClientError, asyncio.TimeoutError):
            _LOGGER.warning('Local worker %s did not answer %s %s.',
                            self._children[child_index].base_url, method, path)
            return None


def _normalize_upstream(url: str) -> str:
    parsed = urlparse.urlsplit(url)
    if (parsed.scheme not in ('http', 'https') or not parsed.netloc or
            parsed.query or parsed.fragment or parsed.path not in ('', '/')):
        raise ValueError('Upstream must be an HTTP(S) base URL without a path, '
                         f'query, or fragment. Got: {url!r}')
    return url.rstrip('/')


def _normalize_path(path: str, name: str) -> str:
    if not path.startswith('/') or '?' in path or '#' in path:
        raise ValueError(f'{name} must be an absolute URL path. Got: {path!r}')
    return path


def _positive(value: float, name: str) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be positive.')
    return value


def _nonnegative(value: float, name: str) -> float:
    if not math.isfinite(value) or value < 0:
        raise ValueError(f'{name} must be nonnegative.')
    return value


def _nonnegative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _json_error(status: int, message: str) -> web.Response:
    return web.json_response({
        'status': 'error',
        'error': message,
    },
                             status=status)


def _end_to_end_headers(
    headers: Iterable[tuple[str, str]],
    *,
    drop_host: bool = False,
) -> CIMultiDict[str]:
    """Preserve repeated end-to-end fields and strip connection options."""
    header_items = tuple(headers)
    excluded = set(_HOP_BY_HOP_HEADERS)
    for name, value in header_items:
        if name.lower() == 'connection':
            excluded.update(token.strip().lower()
                            for token in value.split(',')
                            if token.strip())
    if drop_host:
        excluded.add('host')
    result: CIMultiDict[str] = CIMultiDict()
    for name, value in header_items:
        if name.lower() not in excluded:
            result.add(name, value)
    return result


def _relay(response: _ChildResponse) -> web.Response:
    return web.Response(status=response.status,
                        body=response.body,
                        headers=_end_to_end_headers(response.headers))


def _response_json(response: _ChildResponse) -> Mapping[str, Any] | None:
    try:
        payload = json.loads(response.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _strict_response_json(response: _ChildResponse) -> Mapping[str, Any] | None:
    """Parse one JSON object while rejecting duplicate member names."""

    def _object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        payload = dict(pairs)
        if len(payload) != len(pairs):
            raise ValueError('Response JSON contains duplicate member names.')
        return payload

    try:
        payload = json.loads(response.body, object_pairs_hook=_object)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _request_id(payload: Mapping[str, Any],
                response: _ChildResponse | None) -> str | None:
    if response is not None:
        response_payload = _response_json(response)
        if response_payload is not None:
            response_id = response_payload.get('request_id')
            if isinstance(response_id, str):
                return response_id
    submitted_id = payload.get('request_id')
    return submitted_id if isinstance(submitted_id, str) else None


def _status_response_status(response: _ChildResponse,
                            request_id: Any) -> str | None:
    if not isinstance(request_id, str):
        return None
    payload = _response_json(response)
    if payload is None or payload.get('request_id') != request_id:
        return None
    status = payload.get('status')
    return status if status in _KNOWN_REQUEST_STATUSES else None


def _is_definitive_cancellation(response: _ChildResponse,
                                request_id: Any) -> bool:
    if _status_response_status(response,
                               request_id) not in ('CANCELED', 'CANCELLED'):
        return False
    payload = _response_json(response)
    assert payload is not None
    canceled = payload.get('canceled')
    return canceled is not False


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Expose local async workers as one capacity-aware endpoint.'
    )
    parser.add_argument(
        '--upstream',
        action='append',
        help='Worker base URL. Repeat for each worker, or use --upstream-count.'
    )
    parser.add_argument('--upstream-host', default='127.0.0.1')
    parser.add_argument('--upstream-port-start', type=int, default=8081)
    parser.add_argument('--upstream-count', type=int)
    parser.add_argument('--async-path', required=True)
    parser.add_argument('--readiness-path', required=True)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--probe-timeout-seconds', type=float, default=2.0)
    parser.add_argument('--readiness-timeout-seconds', type=float, default=5.0)
    parser.add_argument('--status-timeout-seconds', type=float, default=5.0)
    parser.add_argument('--request-timeout-seconds', type=float, default=3700.0)
    parser.add_argument('--probe-cache-seconds', type=float, default=0.25)
    parser.add_argument('--reservation-grace-seconds', type=float, default=1.0)
    parser.add_argument(
        '--retriable-status-code',
        action='append',
        type=int,
        help='Explicit pre-dispatch rejection status. Defaults to 429.')
    parser.add_argument(
        '--release-and-relay-response',
        action='append',
        help=('Exact CODE:STATE pre-dispatch response that releases local '
              'ownership and is relayed without trying another worker. The '
              'JSON body must contain exactly state and matching request_id.'))
    parser.add_argument(
        '--max-sticky-requests',
        type=int,
        default=10000,
        help=('Maximum confirmed owner cache size and ambiguous ownership '
              'safety budget.'),
    )
    parser.add_argument('--client-max-size-mib', type=int, default=1)
    return parser


def _resolve_upstreams(args: argparse.Namespace) -> Sequence[str]:
    if args.upstream and args.upstream_count is not None:
        raise ValueError('Use either --upstream or --upstream-count, not both.')
    if args.upstream:
        return args.upstream
    if args.upstream_count is None or args.upstream_count < 1:
        raise ValueError('Supply --upstream at least once, or set '
                         '--upstream-count to a positive integer.')
    last_port = args.upstream_port_start + args.upstream_count - 1
    if args.upstream_port_start < 1 or last_port > 65535:
        raise ValueError('The generated upstream port range must be between '
                         '1 and 65535.')
    return [
        f'http://{args.upstream_host}:{port}'
        for port in range(args.upstream_port_start, last_port + 1)
    ]


def _parse_release_and_relay_responses(
        values: Sequence[str] | None) -> dict[int, str]:
    result: dict[int, str] = {}
    for value in values or ():
        code_text, separator, state = value.partition(':')
        try:
            code = int(code_text)
        except ValueError as error:
            raise ValueError(
                'Release-and-relay response must use CODE:STATE.') from error
        if (not separator or not state or code < 400 or code > 599 or
                code in result):
            raise ValueError('Release-and-relay response must use one unique '
                             '400..599 CODE:STATE value.')
        result[code] = state
    return result


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if args.port < 1 or args.port > 65535:
        raise ValueError('port must be between 1 and 65535.')
    if args.client_max_size_mib < 1:
        raise ValueError('client-max-size-mib must be at least 1.')
    router = LocalAsyncRouter(
        _resolve_upstreams(args),
        args.async_path,
        args.readiness_path,
        probe_timeout_seconds=args.probe_timeout_seconds,
        readiness_timeout_seconds=args.readiness_timeout_seconds,
        status_timeout_seconds=args.status_timeout_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        probe_cache_seconds=args.probe_cache_seconds,
        reservation_grace_seconds=args.reservation_grace_seconds,
        retriable_status_codes=(args.retriable_status_code
                                if args.retriable_status_code is not None else
                                (429,)),
        release_and_relay_responses=_parse_release_and_relay_responses(
            args.release_and_relay_response),
        max_sticky_requests=args.max_sticky_requests,
        client_max_size=args.client_max_size_mib * 1024**2,
    )
    web.run_app(router.create_app(), host=args.host, port=args.port)


if __name__ == '__main__':
    logging.basicConfig(level=os.environ.get('SKYPILOT_LOG_LEVEL', 'INFO'))
    main()
