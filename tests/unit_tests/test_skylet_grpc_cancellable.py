"""Tests for the cancellable gRPC unary / streaming wrappers in backend_utils.

Verifies that when a SkyPilotContext is cancelled mid-call, the underlying
gRPC call object's .cancel() is invoked — which is what unblocks a worker
thread stuck in __next__ / .result() inside threading.Condition.wait().
"""
import asyncio
import contextvars
import threading
import time
from unittest import mock

import grpc
import pytest

from sky import exceptions
from sky.backends import backend_utils
from sky.backends import skylet_rpc
from sky.utils import context
from sky.utils import context_utils


@pytest.fixture(autouse=True)
def _isolate_context():
    """Reset the SkyPilotContext ContextVar between tests.

    Without this, ``context.initialize()`` from an earlier test in the same
    xdist worker leaks the SkyPilotContext into the next test, which makes
    the no-ctx test see a stale context and hang inside the fake unary
    call's ``.result()``.
    """
    # pylint: disable=protected-access
    token = context._CONTEXT.set(None)
    try:
        yield
    finally:
        context._CONTEXT.reset(token)


def _run_in_ctx_thread(ctx_snapshot: contextvars.Context, fn):
    """Spawn a daemon thread that runs ``fn`` inside ``ctx_snapshot``.

    Mirrors how to_thread_with_executor propagates the SkyPilotContext
    (via contextvars.copy_context().run) into worker threads.
    """
    t = threading.Thread(target=ctx_snapshot.run, args=(fn,), daemon=True)
    t.start()
    return t


class _FakeUnaryCall:
    """Stand-in for the grpc future returned by ``method.future(req, ...)``."""

    def __init__(self, block_forever: bool = True):
        self._done = threading.Event()
        self._cancelled = False
        self._block = block_forever

    def cancel(self):
        self._cancelled = True
        self._done.set()

    def cancelled(self):
        return self._cancelled

    def result(self, timeout=None):
        if self._block:
            self._done.wait(timeout=timeout)
        if self._cancelled:
            err = grpc.RpcError()
            err.code = lambda: grpc.StatusCode.CANCELLED  # type: ignore
            raise err
        return 'ok'


class _FakeUnaryMethod:
    """Stand-in for a unary-unary gRPC stub method."""

    def __init__(self):
        self.calls = []

    def future(self, request, timeout=None):
        del request, timeout
        call = _FakeUnaryCall()
        self.calls.append(call)
        return call

    def __call__(self, request, timeout=None):  # plain blocking form
        del request, timeout
        # Not used in cancel test, but exercised by the no-ctx path.
        return 'sync-ok'


class _FakeStreamingCall:
    """Stand-in for the iterator returned by ``method(req, timeout=...)`` for
    unary-stream RPCs."""

    def __init__(self, items, hang_at=None):
        self._items = list(items)
        self._idx = 0
        self._cancelled = threading.Event()
        self._hang_at = hang_at  # yield items, then hang here

    def cancel(self):
        self._cancelled.set()

    def cancelled_event_is_set(self) -> bool:
        return self._cancelled.is_set()

    def __iter__(self):
        return self

    def __next__(self):
        if self._cancelled.is_set():
            err = grpc.RpcError()
            err.code = lambda: grpc.StatusCode.CANCELLED  # type: ignore
            raise err
        if self._hang_at is not None and self._idx == self._hang_at:
            # Block until cancel() is called.
            self._cancelled.wait()
            err = grpc.RpcError()
            err.code = lambda: grpc.StatusCode.CANCELLED  # type: ignore
            raise err
        if self._idx >= len(self._items):
            raise StopIteration
        item = self._items[self._idx]
        self._idx += 1
        return item


def _rpc_error(status_code, details='transient'):
    err = grpc.RpcError()
    err.code = lambda: status_code  # type: ignore
    err.details = lambda: details  # type: ignore
    return err


def test_unary_no_ctx_falls_through_to_blocking_call():
    method = _FakeUnaryMethod()
    # No SkyPilotContext: should NOT use .future().
    result = backend_utils.invoke_grpc_unary(method, 'req', timeout=1.0)
    assert result == 'sync-ok'
    assert not method.calls  # .future() was never invoked.


def test_unary_with_ctx_uses_future_and_cancels_on_ctx_cancel():
    ctx = context.initialize()
    method = _FakeUnaryMethod()
    results = []
    errors = []

    def worker():
        try:
            results.append(
                backend_utils.invoke_grpc_unary(method, 'req', timeout=None))
        except (Exception, asyncio.CancelledError) as e:  # pylint: disable=broad-except
            # CancelledError is BaseException in py3.8+, not Exception.
            errors.append(e)

    snapshot = contextvars.copy_context()
    t = _run_in_ctx_thread(snapshot, worker)
    # Wait for the worker to start the call.
    for _ in range(50):
        if method.calls:
            break
        time.sleep(0.01)
    assert len(method.calls) == 1
    assert not method.calls[0].cancelled()

    ctx.cancel()
    t.join(timeout=2)
    assert not t.is_alive(), 'Worker did not exit after ctx.cancel()'
    assert method.calls[0].cancelled(), (
        'Underlying gRPC call.cancel() was not invoked')
    # The CANCELLED RpcError must surface as asyncio.CancelledError so the
    # request executor classifies the request as CANCELLED, not FAILED.
    assert len(errors) == 1, f'expected one error, got {errors}'
    assert isinstance(errors[0], asyncio.CancelledError), (
        f'expected CancelledError, got {type(errors[0]).__name__}')


def test_unary_cleans_up_callback_on_success():
    ctx = context.initialize()
    method = mock.MagicMock()
    fut = mock.MagicMock()
    fut.result.return_value = 'value'
    method.future.return_value = fut
    out = backend_utils.invoke_grpc_unary(method, 'req', timeout=1.0)
    assert out == 'value'
    # After success, ctx.cancel() must NOT call fut.cancel() — the callback
    # was unregistered.
    fut.cancel.assert_not_called()
    ctx.cancel()
    fut.cancel.assert_not_called()


def test_streaming_yields_items_when_no_cancel():
    context.initialize()
    call = _FakeStreamingCall(['a', 'b', 'c'])
    method = mock.MagicMock(return_value=call)
    out = list(backend_utils.invoke_grpc_streaming(method, 'req', timeout=None))
    assert out == ['a', 'b', 'c']


def test_streaming_cancels_iterator_on_ctx_cancel():
    ctx = context.initialize()
    call = _FakeStreamingCall(['a', 'b'], hang_at=2)
    method = mock.MagicMock(return_value=call)
    received = []
    errors = []

    def consume():
        try:
            for item in backend_utils.invoke_grpc_streaming(method, 'req'):
                received.append(item)
        except (grpc.RpcError, asyncio.CancelledError) as e:
            errors.append(e)

    snapshot = contextvars.copy_context()
    t = _run_in_ctx_thread(snapshot, consume)
    # Wait for the consumer to consume the buffered items and start hanging.
    for _ in range(100):
        if len(received) == 2:
            break
        time.sleep(0.01)
    assert received == ['a', 'b']
    assert t.is_alive(), 'Consumer should be blocked on the next item'

    ctx.cancel()
    t.join(timeout=2)
    assert not t.is_alive(), 'Consumer did not exit after ctx.cancel()'
    # CANCELLED RpcError must be translated to asyncio.CancelledError.
    assert len(errors) == 1, f'expected one error, got {errors}'
    assert isinstance(errors[0], asyncio.CancelledError), (
        f'expected CancelledError, got {type(errors[0]).__name__}')


def test_streaming_cleans_up_callback_on_normal_completion():
    ctx = context.initialize()
    call = _FakeStreamingCall(['only-one'])
    method = mock.MagicMock(return_value=call)
    out = list(backend_utils.invoke_grpc_streaming(method, 'req'))
    assert out == ['only-one']
    # After normal completion, ctx.cancel() should not try to cancel the
    # already-finished iterator (cb was unregistered).
    ctx.cancel()
    assert not call.cancelled_event_is_set()


def test_unary_forwards_extra_grpc_options():
    """Callers must be able to pass metadata / wait_for_ready / credentials."""
    context.initialize()
    method = mock.MagicMock()
    fut = mock.MagicMock()
    fut.result.return_value = 'value'
    method.future.return_value = fut
    backend_utils.invoke_grpc_unary(
        method,
        'req',
        timeout=1.0,
        metadata=(('x-extra', '1'),),
        wait_for_ready=True,
    )
    method.future.assert_called_once_with(
        'req',
        timeout=1.0,
        metadata=(('x-extra', '1'),),
        wait_for_ready=True,
    )


def test_streaming_forwards_extra_grpc_options():
    context.initialize()
    call = _FakeStreamingCall(['only-one'])
    method = mock.MagicMock(return_value=call)
    list(
        backend_utils.invoke_grpc_streaming(
            method,
            'req',
            timeout=None,
            metadata=(('x-extra', '1'),),
        ))
    method.assert_called_once_with(
        'req',
        timeout=None,
        metadata=(('x-extra', '1'),),
    )


def test_invoke_skylet_with_retries_rejects_pre_cancelled_context():
    ctx = context.initialize()
    call_count = {'n': 0}

    def func():
        call_count['n'] += 1
        # Simulate UNAVAILABLE so we go into the retry path.
        err = grpc.RpcError()
        err.code = lambda: grpc.StatusCode.UNAVAILABLE  # type: ignore
        err.details = lambda: 'transient'  # type: ignore
        raise err

    # Patch sleep so the retry loop reaches the cancel check fast.
    with mock.patch.object(skylet_rpc, '_handle_grpc_error') as h:
        # Make _handle_grpc_error a no-op (simulates retry-worthy error).
        h.return_value = None
        ctx.cancel()
        with pytest.raises(asyncio.CancelledError):
            backend_utils.invoke_skylet_with_retries(func)
        # Should never have called func because ctx was already cancelled.
        assert call_count['n'] == 0


def test_unary_retry_backoff_wakes_on_context_cancellation(monkeypatch):
    ctx = context.initialize()
    attempts = 0
    waits = []
    backoff = mock.MagicMock()
    backoff.current_backoff.return_value = 7.0
    monkeypatch.setattr(skylet_rpc.common_utils, 'Backoff',
                        mock.MagicMock(return_value=backoff))

    def func():
        nonlocal attempts
        attempts += 1
        raise _rpc_error(grpc.StatusCode.UNAVAILABLE)

    def cancel_during_wait(seconds):
        waits.append(seconds)
        ctx.cancel()
        raise asyncio.CancelledError('cancelled during retry backoff')

    monkeypatch.setattr(context_utils, 'sleep_with_cancellation',
                        cancel_during_wait)
    monkeypatch.setattr(
        context_utils.time, 'sleep',
        lambda _: pytest.fail('retry backoff bypassed cancellation bridge'))

    with pytest.raises(asyncio.CancelledError,
                       match='cancelled during retry backoff'):
        backend_utils.invoke_skylet_with_retries(func)

    assert attempts == 1
    assert waits == [7.0]


def test_streaming_retry_backoff_wakes_on_context_cancellation(monkeypatch):
    ctx = context.initialize()
    attempts = 0
    waits = []
    backoff = mock.MagicMock()
    backoff.current_backoff.return_value = 11.0
    monkeypatch.setattr(skylet_rpc.common_utils, 'Backoff',
                        mock.MagicMock(return_value=backoff))

    def stream_func():
        nonlocal attempts
        attempts += 1
        raise _rpc_error(grpc.StatusCode.UNAVAILABLE)

    def cancel_during_wait(seconds):
        waits.append(seconds)
        ctx.cancel()
        raise asyncio.CancelledError('cancelled during stream retry backoff')

    monkeypatch.setattr(context_utils, 'sleep_with_cancellation',
                        cancel_during_wait)
    monkeypatch.setattr(
        context_utils.time, 'sleep',
        lambda _: pytest.fail('stream backoff bypassed cancellation bridge'))

    with pytest.raises(asyncio.CancelledError,
                       match='cancelled during stream retry backoff'):
        list(backend_utils.invoke_skylet_streaming_with_retries(stream_func))

    assert attempts == 1
    assert waits == [11.0]


def test_unary_retry_preserves_backoffs_and_skips_terminal_wait(monkeypatch):
    attempts = 0
    waits = []
    backoff = mock.MagicMock()
    backoff.current_backoff.side_effect = [0.5, 1.0, 2.0]
    monkeypatch.setattr(skylet_rpc.common_utils, 'Backoff',
                        mock.MagicMock(return_value=backoff))
    monkeypatch.setattr(context_utils, 'sleep_with_cancellation', waits.append)
    monkeypatch.setattr(context_utils.time, 'sleep', waits.append)

    def func():
        nonlocal attempts
        attempts += 1
        raise _rpc_error(grpc.StatusCode.UNAVAILABLE)

    with pytest.raises(exceptions.SkyletUnavailableError,
                       match='after 3 attempts'):
        backend_utils.invoke_skylet_with_retries(func, max_attempts=3)

    assert attempts == 3
    assert waits == [0.5, 1.0]


def test_unary_retry_without_context_preserves_wait_and_attempt_count(
        monkeypatch):
    attempts = 0
    waits = []
    backoff = mock.MagicMock()
    backoff.current_backoff.return_value = 0.5
    monkeypatch.setattr(skylet_rpc.common_utils, 'Backoff',
                        mock.MagicMock(return_value=backoff))
    monkeypatch.setattr(context_utils.time, 'sleep', waits.append)

    def func():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _rpc_error(grpc.StatusCode.UNAVAILABLE)
        return 'recovered'

    assert backend_utils.invoke_skylet_with_retries(func) == 'recovered'
    assert attempts == 2
    assert waits == [0.5]


def test_streaming_retry_preserves_backoffs_and_skips_terminal_wait(
        monkeypatch):
    attempts = 0
    waits = []
    backoff = mock.MagicMock()
    backoff.current_backoff.side_effect = [0.5, 1.0, 2.0]
    monkeypatch.setattr(skylet_rpc.common_utils, 'Backoff',
                        mock.MagicMock(return_value=backoff))
    monkeypatch.setattr(context_utils, 'sleep_with_cancellation', waits.append)
    monkeypatch.setattr(context_utils.time, 'sleep', waits.append)

    def stream_func():
        nonlocal attempts
        attempts += 1
        raise _rpc_error(grpc.StatusCode.UNAVAILABLE)

    with pytest.raises(exceptions.SkyletUnavailableError,
                       match='after 3 attempts'):
        list(backend_utils.invoke_skylet_streaming_with_retries(stream_func))

    assert attempts == 3
    assert waits == [0.5, 1.0]


def test_invoke_skylet_with_retries_raises_unavailable_on_connection_refused():

    def func():
        raise _rpc_error(grpc.StatusCode.UNAVAILABLE, 'Connection refused')

    with pytest.raises(exceptions.SkyletUnavailableError,
                       match='connection refused'):
        backend_utils.invoke_skylet_with_retries(func)


def test_invoke_skylet_with_retries_raises_fallback_error_on_unimplemented():

    def func():
        raise _rpc_error(grpc.StatusCode.UNIMPLEMENTED, 'missing method')

    with pytest.raises(exceptions.SkyletMethodNotImplementedError,
                       match='falling back to legacy execution'):
        backend_utils.invoke_skylet_with_retries(func)


def test_invoke_skylet_with_retries_honors_single_attempt():
    attempts = 0

    def func():
        nonlocal attempts
        attempts += 1
        raise _rpc_error(grpc.StatusCode.UNAVAILABLE)

    with mock.patch.object(skylet_rpc, '_handle_grpc_error') as handler:
        with pytest.raises(exceptions.SkyletUnavailableError,
                           match='after 1 attempts'):
            backend_utils.invoke_skylet_with_retries(func, max_attempts=1)

    assert attempts == 1
    handler.assert_called_once()
