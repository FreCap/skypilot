"""Retry and transport-failure policy for the SkyServe load balancer."""

from collections.abc import Callable
import types
from typing import Any

import httpx

# HTTP semantics define these methods as idempotent: replay after an ambiguous
# transport failure cannot create a second logical operation. POST/PATCH are
# deliberately absent. A configured retriable response status remains an
# explicit per-service opt-in even for those methods.
_IDEMPOTENT_METHODS = frozenset(
    {'GET', 'HEAD', 'PUT', 'DELETE', 'OPTIONS', 'TRACE'})


class _RetriableStatusError(Exception):
    """A replica answered with a status the service marked retriable.

    Returned from _proxy_request_to like transport errors so
    _proxy_with_retries re-routes the request to another replica. Only
    statuses listed in the service's
    load_balancer.retriable_status_codes take this path — everything
    else streams to the client verbatim.
    """

    def __init__(self, status_code: int, url: str) -> None:
        super().__init__(
            f'replica {url} answered retriable status {status_code}')
        self.status_code = status_code


class _PreDispatchError(RuntimeError):
    """A proxy attempt failed before an upstream request could be sent."""


class _RouteAuthorityRetryableError(_PreDispatchError):
    """A new exact bind must discard its stale route and select afresh."""

    def __init__(self, detail: str, route_sync_generation: int,
                 conflict_observed_sync_generation: int,
                 route_projection_generation: int, route_projection_sha256: str,
                 route_source_epoch: int) -> None:
        super().__init__(detail)
        self.route_sync_generation = route_sync_generation
        self.conflict_observed_sync_generation = (
            conflict_observed_sync_generation)
        self.route_projection_generation = route_projection_generation
        self.route_projection_sha256 = route_projection_sha256
        self.route_source_epoch = route_source_epoch


def _is_dead_connection_error(exc: Exception) -> bool:
    """Whether a proxy failure indicates a DEAD replica vs a saturated one.

    A healthy replica overloaded at high RPS trips the connect/read timeout
    (httpx.TimeoutException), so timeouts must NOT count toward eviction --
    evicting a merely-saturated replica shrinks capacity under load and
    cascades. Only genuine connection failures (refused/reset: NetworkError,
    ProtocolError) indicate a dead replica worth evicting.
    """
    if isinstance(exc, httpx.TimeoutException):
        return False
    return isinstance(exc, (httpx.NetworkError, httpx.ProtocolError))


def _is_definitely_not_dispatched(exc: Exception) -> bool:
    """Whether a proxy failure proves the request never reached a replica."""
    return isinstance(exc, (_PreDispatchError, httpx.ConnectError,
                            httpx.ConnectTimeout, httpx.PoolTimeout))


def _can_retry_proxy_failure(method: str, exc: Exception) -> bool:
    """Whether replaying a failed proxy attempt preserves request semantics.

    A configured retriable status is an explicit service-level opt-in. An
    idempotent method is inherently replay-safe. For non-idempotent methods,
    only failures that prove no connection/request dispatch occurred are safe;
    read, write, protocol, and generic timeout failures have an ambiguous
    outcome and must be returned without replaying the operation.
    """
    if isinstance(exc, _RetriableStatusError):
        return True
    if method.upper() in _IDEMPOTENT_METHODS:
        return True
    return _is_definitely_not_dispatched(exc)


_FACADE_GLOBAL_FUNCTION_NAMES = (
    '_is_dead_connection_error',
    '_is_definitely_not_dispatched',
    '_can_retry_proxy_failure',
)


def _bind_facade_globals(
    facade_globals: dict[str, Any]
) -> tuple[Callable[[Exception], bool], Callable[[Exception], bool], Callable[
    [str, Exception], bool]]:
    """Bind extracted policy functions to their historical facade globals."""
    facade_globals['_IDEMPOTENT_METHODS'] = _IDEMPOTENT_METHODS
    for function_name in _FACADE_GLOBAL_FUNCTION_NAMES:
        function = globals()[function_name]
        rebound_function = types.FunctionType(
            function.__code__,
            facade_globals,
            function.__name__,
            function.__defaults__,
            function.__closure__,
        )
        rebound_function.__kwdefaults__ = (None
                                           if function.__kwdefaults__ is None
                                           else function.__kwdefaults__.copy())
        rebound_function.__annotations__ = function.__annotations__.copy()
        rebound_function.__module__ = facade_globals['__name__']
        rebound_function.__qualname__ = function.__qualname__
        rebound_function.__dict__.update(function.__dict__)
        type_params = getattr(function, '__type_params__', None)
        if type_params is not None:
            setattr(rebound_function, '__type_params__', type_params)
        globals()[function_name] = rebound_function
        facade_globals[function_name] = rebound_function
    return (_is_dead_connection_error, _is_definitely_not_dispatched,
            _can_retry_proxy_failure)
