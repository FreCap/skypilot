"""Unit tests for sky.utils.context_utils module."""
# pylint: disable=protected-access
import asyncio
from typing import Optional, Union
from unittest import mock

import pytest

from sky.utils import context
from sky.utils import context_utils


@context_utils.cancellation_guard
def original_function(arg1: int, arg2: str) -> Optional[Union[int, str]]:
    return None


def test_cancellation_guard_perserves_typecheck():
    # Verify that the decorated function has the same signature
    assert original_function.__name__ == 'original_function'
    assert original_function.__annotations__ == {
        'arg1': int,
        'arg2': str,
        'return': Optional[Union[int, str]]
    }

    # Verify that the decorated function can be called with the same signature
    assert original_function(1, 'test') is None


def test_sleep_with_cancellation_delegates_without_context(monkeypatch):
    sleep = mock.Mock()
    monkeypatch.setattr(context_utils.context, 'get', lambda: None)
    monkeypatch.setattr(context_utils.time, 'sleep', sleep)

    context_utils.sleep_with_cancellation(7.5)

    sleep.assert_called_once_with(7.5)


def test_sleep_with_cancellation_wakes_and_unregisters(monkeypatch):
    ctx = context.SkyPilotContext()
    waits = []

    class _CancellingEvent:
        """Event that cancels while its waiter is registered."""

        def __init__(self):
            self.signalled = False

        def set(self):
            self.signalled = True

        def wait(self, timeout):
            waits.append(timeout)
            ctx.cancel()
            return self.signalled

    monkeypatch.setattr(context_utils.context, 'get', lambda: ctx)
    monkeypatch.setattr(context_utils.threading, 'Event', _CancellingEvent)

    with pytest.raises(asyncio.CancelledError):
        context_utils.sleep_with_cancellation(180)

    assert waits == [180]
    assert not ctx._cancel_callbacks


def test_sleep_with_cancellation_timeout_unregisters_callback(monkeypatch):
    ctx = context.SkyPilotContext()
    waits = []

    class _TimeoutEvent:
        """Event that reaches its timeout without cancellation."""

        def set(self):
            pass

        def wait(self, timeout):
            waits.append(timeout)
            return False

    monkeypatch.setattr(context_utils.context, 'get', lambda: ctx)
    monkeypatch.setattr(context_utils.threading, 'Event', _TimeoutEvent)

    context_utils.sleep_with_cancellation(10)

    assert waits == [10]
    assert not ctx._cancel_callbacks


def test_sleep_with_cancellation_cancellation_at_timeout_raises(monkeypatch):
    ctx = context.SkyPilotContext()

    class _DeadlineEvent:
        """Event that observes cancellation exactly at its timeout."""

        def set(self):
            pass

        def wait(self, timeout):
            assert timeout == 10
            ctx.cancel()
            return False

    monkeypatch.setattr(context_utils.context, 'get', lambda: ctx)
    monkeypatch.setattr(context_utils.threading, 'Event', _DeadlineEvent)

    with pytest.raises(asyncio.CancelledError):
        context_utils.sleep_with_cancellation(10)

    assert not ctx._cancel_callbacks


def test_sleep_with_cancellation_cancellation_during_unregister_raises(
        monkeypatch):

    class _CancelDuringUnregisterContext(context.SkyPilotContext):

        def unregister_cancel_callback(self, callback):
            self.cancel()
            super().unregister_cancel_callback(callback)

    ctx = _CancelDuringUnregisterContext()
    monkeypatch.setattr(context_utils.context, 'get', lambda: ctx)

    with pytest.raises(asyncio.CancelledError):
        context_utils.sleep_with_cancellation(0)


def test_sleep_with_cancellation_rejects_pre_cancelled_context(monkeypatch):
    ctx = context.SkyPilotContext()
    ctx.cancel()
    monkeypatch.setattr(context_utils.context, 'get', lambda: ctx)

    with pytest.raises(asyncio.CancelledError):
        context_utils.sleep_with_cancellation(10)

    assert not ctx._cancel_callbacks
