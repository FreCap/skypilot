"""Event-loop responsiveness tests for API authentication endpoints."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
import threading
from typing import Any
from unittest import mock

import pytest

from sky.server import server


async def _assert_call_runs_off_event_loop(call: Callable[[], Awaitable[Any]],
                                           started: threading.Event,
                                           release: threading.Event,
                                           finished: threading.Event) -> Any:
    test_finished = threading.Event()

    # Prevent a regression from deadlocking the test. If the synchronous DB
    # call runs on the event loop, this fallback is the only code that can
    # release it.
    def release_if_event_loop_is_blocked() -> None:
        if started.wait(timeout=1) and not test_finished.wait(timeout=0.5):
            release.set()

    fallback_thread = threading.Thread(target=release_if_event_loop_is_blocked)
    fallback_thread.start()
    call_task = asyncio.create_task(call())
    try:
        call_started = await asyncio.wait_for(asyncio.to_thread(
            started.wait, 1),
                                              timeout=2)
        assert call_started
        await asyncio.sleep(0)
        assert not finished.is_set()
    finally:
        test_finished.set()
        release.set()
        result = await asyncio.gather(call_task, return_exceptions=True)
        fallback_thread.join(timeout=1)

    assert not fallback_thread.is_alive()
    if isinstance(result[0], BaseException):
        raise result[0]
    return result[0]


@pytest.mark.asyncio
async def test_poll_auth_session_does_not_block_event_loop(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocking_poll_session(_code_verifier: str) -> str:
        started.set()
        release.wait(timeout=2)
        finished.set()
        return 'test-token'

    monkeypatch.setattr(server.auth_sessions.auth_session_store, 'poll_session',
                        blocking_poll_session)
    response = await _assert_call_runs_off_event_loop(
        lambda: server.poll_auth_token('test-verifier'), started, release,
        finished)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_authorize_auth_session_does_not_block_event_loop(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocking_create_session(_code_challenge: str, _token: str) -> None:
        started.set()
        release.wait(timeout=2)
        finished.set()

    request = mock.AsyncMock()
    request.json.return_value = {'code_challenge': 'a' * 43}
    monkeypatch.setattr(server, '_generate_auth_token',
                        mock.Mock(return_value='test-token'))
    monkeypatch.setattr(server.auth_sessions.auth_session_store,
                        'create_session', blocking_create_session)
    response = await _assert_call_runs_off_event_loop(
        lambda: server.authorize_auth_session(request), started, release,
        finished)

    assert response.status_code == 200
