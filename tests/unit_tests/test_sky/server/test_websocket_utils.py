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


async def _invalid_timestamp_messages():
    yield b'\xff'


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


@pytest.mark.asyncio
async def test_invalid_timestamp_message_closes_backend_without_hanging():
    backend_closed = asyncio.Event()
    websocket = _WebSocket(_invalid_timestamp_messages)

    async def read_from_backend():
        await backend_closed.wait()
        return b''

    async def close_backend():
        backend_closed.set()

    ssh_failed = await asyncio.wait_for(websocket_utils.run_websocket_proxy(
        websocket,
        read_from_backend,
        mock.AsyncMock(),
        close_backend,
        timestamps_supported=True),
                                        timeout=1)

    assert not ssh_failed
    assert backend_closed.is_set()
    websocket.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_backend_close_error_does_not_strand_peer_reader():
    reader_started = asyncio.Event()
    reader_cancelled = asyncio.Event()

    async def messages():
        await reader_started.wait()
        for message in ():
            yield message

    websocket = _WebSocket(messages)

    async def read_from_backend():
        reader_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            reader_cancelled.set()

    ssh_failed = await asyncio.wait_for(websocket_utils.run_websocket_proxy(
        websocket,
        read_from_backend,
        mock.AsyncMock(),
        mock.AsyncMock(side_effect=RuntimeError('close failed')),
        timestamps_supported=False),
                                        timeout=1)

    assert not ssh_failed
    assert reader_cancelled.is_set()
    websocket.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_proxy_cancellation_closes_both_endpoints():
    client_started = asyncio.Event()
    backend_started = asyncio.Event()
    never = asyncio.Event()

    async def messages():
        client_started.set()
        await never.wait()
        for message in ():
            yield message

    async def read_from_backend():
        backend_started.set()
        await never.wait()
        return b''

    websocket = _WebSocket(messages)
    close_backend = mock.AsyncMock()
    proxy_task = asyncio.create_task(
        websocket_utils.run_websocket_proxy(websocket,
                                            read_from_backend,
                                            mock.AsyncMock(),
                                            close_backend,
                                            timestamps_supported=False))
    await asyncio.gather(client_started.wait(), backend_started.wait())

    proxy_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await proxy_task

    close_backend.assert_awaited_once_with()
    websocket.close.assert_awaited_once_with()
