"""Cancellable Skylet gRPC transport and retry helpers."""
import asyncio
from collections.abc import Callable
from collections.abc import Iterator
import typing
from typing import Any, TypeVar

from sky import exceptions
from sky.adaptors import common as adaptors_common
from sky.utils import common_utils
from sky.utils import context as context_lib
from sky.utils import context_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import grpc
else:
    # To avoid requiring grpcio to be installed on the client side.
    grpc = adaptors_common.LazyImport('grpc')

T = TypeVar('T')


def _raise_if_ctx_canceled() -> None:
    """Raise if we are running inside a cancelled SkyPilotContext."""
    ctx = context_lib.get()
    if ctx is not None and ctx.is_canceled():
        raise asyncio.CancelledError(
            'SkyPilotContext cancelled during Skylet retry')


def _cancelled_via_ctx(ctx: 'context_lib.SkyPilotContext',
                       err: 'grpc.RpcError') -> bool:
    """Did this RpcError come from ctx.cancel() firing our call.cancel()?"""
    return ctx.is_canceled() and err.code() == grpc.StatusCode.CANCELLED


def invoke_grpc_unary(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a gRPC unary method; cancel it on SkyPilotContext cancel."""
    ctx = context_lib.get()
    if ctx is None:
        return method(*args, **kwargs)
    call = method.future(*args, **kwargs)
    ctx.register_cancel_callback(call.cancel)
    try:
        return call.result()
    except grpc.RpcError as e:
        if _cancelled_via_ctx(ctx, e):
            raise asyncio.CancelledError(
                'Skylet gRPC call cancelled via SkyPilotContext') from e
        raise
    finally:
        ctx.unregister_cancel_callback(call.cancel)


def invoke_grpc_streaming(method: Any, *args: Any,
                          **kwargs: Any) -> Iterator[Any]:
    """Call a gRPC unary-stream method; cancel it on SkyPilotContext cancel."""
    ctx = context_lib.get()
    call = method(*args, **kwargs)
    if ctx is None:
        yield from call
        return
    ctx.register_cancel_callback(call.cancel)
    try:
        yield from call
    except grpc.RpcError as e:
        if _cancelled_via_ctx(ctx, e):
            raise asyncio.CancelledError(
                'Skylet gRPC stream cancelled via SkyPilotContext') from e
        raise
    finally:
        ctx.unregister_cancel_callback(call.cancel)


def invoke_skylet_with_retries(func: Callable[..., T],
                               max_attempts: int = 5) -> T:
    """Retry a unary Skylet gRPC request through transient tunnel failures."""
    if max_attempts < 1:
        raise ValueError('max_attempts must be at least 1.')
    backoff = common_utils.Backoff(initial_backoff=0.5)
    last_exception: Exception | None = None

    for attempt in range(max_attempts):
        _raise_if_ctx_canceled()
        try:
            return func()
        except grpc.RpcError as e:
            last_exception = e
            _handle_grpc_error(e)
            if attempt + 1 < max_attempts:
                context_utils.sleep_with_cancellation(backoff.current_backoff())

    raise exceptions.SkyletUnavailableError(
        f'Failed to invoke Skylet after {max_attempts} attempts: '
        f'{last_exception}') from last_exception


def invoke_skylet_streaming_with_retries(
        stream_func: Callable[..., Iterator[T]]) -> Iterator[T]:
    """Retry a streaming Skylet gRPC request through transient failures."""
    max_attempts = 3
    backoff = common_utils.Backoff(initial_backoff=0.5)
    last_exception: Exception | None = None

    for attempt in range(max_attempts):
        _raise_if_ctx_canceled()
        try:
            yield from stream_func()
            return
        except grpc.RpcError as e:
            last_exception = e
            _handle_grpc_error(e)
            if attempt + 1 < max_attempts:
                context_utils.sleep_with_cancellation(backoff.current_backoff())

    raise exceptions.SkyletUnavailableError(
        f'Failed to stream Skylet response after {max_attempts} attempts'
    ) from last_exception


def _handle_grpc_error(e: 'grpc.RpcError') -> None:
    if e.code() == grpc.StatusCode.INTERNAL:
        with ux_utils.print_exception_no_traceback():
            raise exceptions.SkyletInternalError(e.details())
    elif e.code() == grpc.StatusCode.FAILED_PRECONDITION:
        # The skylet rejected the request itself, not the transport: the
        # node cannot honor what was asked (e.g. an autodown it has no
        # permission to perform). Permanent, so surface it verbatim
        # rather than burning retries on it.
        with ux_utils.print_exception_no_traceback():
            raise exceptions.NotSupportedError(e.details())
    elif e.code() == grpc.StatusCode.UNAVAILABLE:
        details = e.details() or ''
        if 'Connection refused' in details:
            raise exceptions.SkyletUnavailableError(
                f'Skylet is not running (connection refused): {details}') from e
    elif e.code() == grpc.StatusCode.UNIMPLEMENTED or e.code(
    ) == grpc.StatusCode.UNKNOWN:
        raise exceptions.SkyletMethodNotImplementedError(
            'gRPC method not implemented on server, '
            f'falling back to legacy execution: {e.details()}')
    else:
        raise e
