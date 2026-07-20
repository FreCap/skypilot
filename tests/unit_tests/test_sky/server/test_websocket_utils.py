"""Tests for WebSocket-to-SSH backend failure classification."""

import asyncio
from unittest import mock

import pytest

from sky.server import websocket_utils


class _WebSocket:

    def __init__(self, messages, *, send_error=None):
        self._messages = messages
        self.send_bytes = mock.AsyncMock(side_effect=send_error)
        self.close = mock.AsyncMock()

    def iter_bytes(self):
        return self._messages()


async def _delayed_empty_messages():
    await asyncio.sleep(0.01)
    for message in ():
        yield message


async def _empty_messages():
    for message in ():
        yield message


@pytest.mark.asyncio
async def test_backend_read_error_is_classified_as_ssh_failure():
    websocket = _WebSocket(_delayed_empty_messages)

    async def read_from_backend():
        raise ConnectionResetError('backend reset')

    close_backend = mock.AsyncMock()
    ssh_failed = await websocket_utils.run_websocket_proxy(
        websocket,
        read_from_backend,
        mock.AsyncMock(),
        close_backend,
        timestamps_supported=False)

    assert ssh_failed
    websocket.close.assert_awaited_once_with()
    close_backend.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_websocket_send_error_is_not_an_ssh_failure():
    websocket = _WebSocket(_delayed_empty_messages,
                           send_error=ConnectionResetError('client reset'))

    ssh_failed = await websocket_utils.run_websocket_proxy(
        websocket,
        mock.AsyncMock(return_value=b'backend data'),
        mock.AsyncMock(),
        mock.AsyncMock(),
        timestamps_supported=False)

    assert not ssh_failed


@pytest.mark.asyncio
async def test_backend_close_after_client_close_is_not_an_ssh_failure():
    client_closed = asyncio.Event()

    async def close_backend():
        client_closed.set()

    async def read_from_backend():
        await client_closed.wait()
        raise ConnectionResetError('closed with client')

    ssh_failed = await websocket_utils.run_websocket_proxy(
        _WebSocket(_empty_messages),
        read_from_backend,
        mock.AsyncMock(),
        close_backend,
        timestamps_supported=False)

    assert not ssh_failed
