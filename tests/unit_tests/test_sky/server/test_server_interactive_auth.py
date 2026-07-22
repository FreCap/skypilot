"""Tests for the server-side interactive SSH authentication bridge."""
import asyncio
import os
import pty
import select
from unittest import mock

import pytest

from sky.server import server


@pytest.mark.asyncio
async def test_client_disconnect_stops_blocked_pty_reader():
    """A closed websocket must not leave the auth handler reading the PTY."""
    master_fd, slave_fd = pty.openpty()
    handler_fd = os.dup(master_fd)

    class ClosedWebsocket:
        """WebSocket whose client has already closed its input stream."""

        def __init__(self):
            self.accepted = False
            self.closed = False

        async def accept(self):
            self.accepted = True

        async def iter_bytes(self):
            if self.closed:
                yield b''

        async def send_bytes(self, data):
            del data

        async def close(self):
            self.closed = True

    websocket = ClosedWebsocket()
    loop = asyncio.get_running_loop()

    try:
        with mock.patch.object(loop,
                               'sock_connect',
                               new=mock.AsyncMock()), \
             mock.patch('sky.server.server.interactive_utils.recv_fd',
                        return_value=handler_fd):
            await asyncio.wait_for(server.ssh_interactive_auth(
                websocket, 'session'),
                                   timeout=1)

        assert websocket.accepted
        assert websocket.closed
        with pytest.raises(OSError):
            os.fstat(handler_fd)
    finally:
        for fd in (master_fd, slave_fd, handler_fd):
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_parent_cancellation_stops_both_proxy_directions():
    """Request cancellation must close the PTY and both forwarding tasks."""
    master_fd, slave_fd = pty.openpty()
    handler_fd = os.dup(master_fd)
    read_started = asyncio.Event()

    class BlockingWebsocket:
        """WebSocket that keeps its client input stream open."""

        def __init__(self):
            self.closed = False

        async def accept(self):
            pass

        async def iter_bytes(self):
            read_started.set()
            await asyncio.Future()
            if self.closed:
                yield b''

        async def send_bytes(self, data):
            del data

        async def close(self):
            self.closed = True

    websocket = BlockingWebsocket()
    loop = asyncio.get_running_loop()

    try:
        with mock.patch.object(loop,
                               'sock_connect',
                               new=mock.AsyncMock()), \
             mock.patch('sky.server.server.interactive_utils.recv_fd',
                        return_value=handler_fd):
            auth_task = asyncio.create_task(
                server.ssh_interactive_auth(websocket, 'session'))
            await asyncio.wait_for(read_started.wait(), timeout=1)
            auth_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(auth_task, timeout=1)

        assert websocket.closed
        with pytest.raises(OSError):
            os.fstat(handler_fd)
    finally:
        for fd in (master_fd, slave_fd, handler_fd):
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_interactive_auth_forwards_both_directions():
    """The cancellable bridge must preserve bidirectional PTY traffic."""
    master_fd, slave_fd = pty.openpty()
    handler_fd = os.dup(master_fd)

    class DuplexWebsocket:
        """WebSocket that exchanges one message in each direction."""

        def __init__(self):
            self.output = []
            self.output_received = asyncio.Event()
            self.closed = False

        async def accept(self):
            pass

        async def iter_bytes(self):
            yield b'123456\n'
            await self.output_received.wait()

        async def send_bytes(self, data):
            self.output.append(data)
            self.output_received.set()

        async def close(self):
            self.closed = True

    websocket = DuplexWebsocket()
    loop = asyncio.get_running_loop()

    try:
        os.write(slave_fd, b'Verification code: ')
        with mock.patch.object(loop,
                               'sock_connect',
                               new=mock.AsyncMock()), \
             mock.patch('sky.server.server.interactive_utils.recv_fd',
                        return_value=handler_fd):
            await asyncio.wait_for(server.ssh_interactive_auth(
                websocket, 'session'),
                                   timeout=1)

        assert b'Verification code: ' in b''.join(websocket.output)
        readable, _, _ = select.select([slave_fd], [], [], 1)
        assert readable
        assert os.read(slave_fd, 4096) == b'123456\n'
        assert websocket.closed
    finally:
        for fd in (master_fd, slave_fd, handler_fd):
            try:
                os.close(fd)
            except OSError:
                pass
