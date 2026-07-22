"""Tests for client-side interactive SSH authentication utilities."""
import asyncio
import os
import pty
import termios
import threading
from unittest import mock

import pytest

from sky.client import interactive_utils
from sky.utils import context_utils


def test_interactive_auth_websocket_bridge_and_terminal_handling():
    """Test client-side interactive authentication via websocket bridge."""
    stdin_master, stdin_slave = pty.openpty()
    stdout_master, stdout_slave = pty.openpty()

    try:
        initial_settings = termios.tcgetattr(stdin_slave)

        class MockWebsocket:

            def __init__(self):
                self.sent = []
                self.to_send = [b'Verification code: ', b'OK\n']
                self.ready = threading.Event()  # Signals: reader is set up
                self.data_received = threading.Event(
                )  # Signals: stdin data sent
                self.settings_during = None  # Capture settings after setraw

            async def __aenter__(self):
                # Capture terminal settings AFTER setraw was called
                self.settings_during = termios.tcgetattr(stdin_slave)
                return self

            async def __aexit__(self, *args):
                pass

            async def send(self, data):
                self.sent.append(data)
                self.data_received.set()

            def __aiter__(self):
                return self

            async def __anext__(self):
                # Signal readiness on first iteration.
                if not self.ready.is_set():
                    self.ready.set()

                if not self.to_send:
                    # Wait for stdin data before completing
                    await asyncio.to_thread(self.data_received.wait, 5.0)
                    raise StopAsyncIteration
                return self.to_send.pop(0)

        mock_ws = MockWebsocket()

        stdin_file = os.fdopen(os.dup(stdin_slave), 'r')
        stdout_file = os.fdopen(os.dup(stdout_slave), 'w')

        def simulate_user():
            """Simulate user typing password."""
            if not mock_ws.ready.wait(timeout=5.0):
                return
            os.write(stdin_master, b'123456\n')
            mock_ws.data_received.wait(timeout=5.0)

        with mock.patch('sys.stdin', stdin_file), \
             mock.patch('sys.stdout', stdout_file), \
             mock.patch('sky.client.interactive_utils.websockets.connect',
                        return_value=mock_ws), \
             mock.patch('sky.server.common.get_server_url',
                        return_value='http://test'), \
             mock.patch('sky.client.service_account_auth.get_service_account_headers',
                        return_value={}), \
             mock.patch('sky.server.common.get_cookie_header_for_url',
                        return_value={}):

            assert os.isatty(stdin_file.fileno()), "stdin must be a tty"

            user_thread = threading.Thread(target=simulate_user)
            user_thread.start()

            asyncio.run(
                interactive_utils._handle_interactive_auth_websocket('test'))

            user_thread.join(timeout=5.0)

        # stdin -> websocket
        assert b'123456\n' in b''.join(mock_ws.sent), "stdin->websocket failed"

        # websocket -> stdout
        stdout_output = os.read(stdout_master, 4096)
        assert b'Verification code:' in stdout_output, "websocket->stdout failed"

        # setraw was called (settings changed during execution)
        assert mock_ws.settings_during != initial_settings, "setraw not called"

        # Terminal settings restored
        final_settings = termios.tcgetattr(stdin_slave)
        assert final_settings == initial_settings, "Terminal not restored!"

    finally:
        for fd in [stdin_master, stdin_slave, stdout_master, stdout_slave]:
            try:
                os.close(fd)
            except OSError:
                pass


def test_interactive_auth_cancellation_cleans_up_forwarders():
    """Cancelling the bridge must stop both forwarding tasks."""
    stdin_read, stdin_write = os.pipe()
    stdout_read, stdout_write = os.pipe()

    async def run_test():
        read_started = asyncio.Event()
        created_tasks = []
        created_transports = []

        class BlockingWebsocket:

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def send(self, data):
                del data

            def __aiter__(self):
                return self

            async def __anext__(self):
                read_started.set()
                await asyncio.Future()

        mock_ws = BlockingWebsocket()
        loop = asyncio.get_running_loop()
        real_create_task = asyncio.create_task
        real_connect_read_pipe = loop.connect_read_pipe
        real_connect_write_pipe = loop.connect_write_pipe

        def capture_task(coro):
            task = real_create_task(coro)
            created_tasks.append(task)
            return task

        async def capture_read_pipe(*args, **kwargs):
            result = await real_connect_read_pipe(*args, **kwargs)
            created_transports.append(result[0])
            return result

        async def capture_write_pipe(*args, **kwargs):
            result = await real_connect_write_pipe(*args, **kwargs)
            created_transports.append(result[0])
            return result

        stdin_file = os.fdopen(os.dup(stdin_read), 'rb', buffering=0)
        stdout_file = os.fdopen(os.dup(stdout_write), 'wb', buffering=0)
        try:
            with mock.patch('sys.stdin', stdin_file), \
                 mock.patch('sys.stdout', stdout_file), \
                 mock.patch(
                     'sky.client.interactive_utils.websockets.connect',
                     return_value=mock_ws), \
                 mock.patch('sky.server.common.get_server_url',
                            return_value='http://test'), \
                 mock.patch(
                     'sky.client.service_account_auth.get_service_account_headers',
                     return_value={}), \
                 mock.patch(
                     'sky.server.common.get_cookie_header_for_url',
                     return_value={}), \
                 mock.patch('asyncio.create_task', side_effect=capture_task), \
                 mock.patch.object(loop,
                                   'connect_read_pipe',
                                   side_effect=capture_read_pipe), \
                 mock.patch.object(loop,
                                   'connect_write_pipe',
                                   side_effect=capture_write_pipe):
                auth_task = real_create_task(
                    interactive_utils._handle_interactive_auth_websocket(
                        'test'))
                await asyncio.wait_for(read_started.wait(), timeout=5)
                auth_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await auth_task
                await asyncio.sleep(0)

            assert len(created_tasks) == 2
            assert all(task.done() for task in created_tasks)
            assert len(created_transports) == 2
            assert all(
                transport.is_closing() for transport in created_transports)
        finally:
            for task in created_tasks:
                task.cancel()
            await asyncio.gather(*created_tasks, return_exceptions=True)
            stdin_file.close()
            stdout_file.close()

    try:
        asyncio.run(run_test())
    finally:
        for fd in (stdin_read, stdin_write, stdout_read, stdout_write):
            try:
                os.close(fd)
            except OSError:
                pass


def test_async_interactive_auth_cancellation_does_not_orphan_lock():
    """Cancellation while waiting must not leave the global lock acquired."""

    class SignallingLock:
        """Threading lock that exposes blocked-acquire progress to the test."""

        def __init__(self):
            self._lock = threading.Lock()
            self._state_lock = threading.Lock()
            self._release_count = 0
            self.acquire_started = threading.Event()
            self.acquire_completed = threading.Event()
            self.cancelled_acquire_released = threading.Event()

        def acquire(self, *args, **kwargs):
            self.acquire_started.set()
            acquired = self._lock.acquire(*args, **kwargs)
            if acquired:
                self.acquire_completed.set()
            return acquired

        def release(self):
            self._lock.release()
            with self._state_lock:
                self._release_count += 1
                if self._release_count == 2:
                    self.cancelled_acquire_released.set()

        def locked(self):
            return self._lock.locked()

    async def run_test():
        auth_lock = SignallingLock()
        auth_lock.acquire()
        auth_lock.acquire_started.clear()
        auth_lock.acquire_completed.clear()

        with mock.patch.object(interactive_utils, '_INTERACTIVE_AUTH_LOCK',
                               auth_lock):
            auth_task = asyncio.create_task(
                interactive_utils.handle_interactive_auth_async(
                    '<sky-interactive session="test"/>'))
            assert await asyncio.to_thread(auth_lock.acquire_started.wait, 1)

            auth_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await auth_task

            auth_lock.release()
            try:
                assert await asyncio.to_thread(auth_lock.acquire_completed.wait,
                                               1)
                assert await asyncio.to_thread(
                    auth_lock.cancelled_acquire_released.wait, 1)
                assert not auth_lock.locked()
            finally:
                # Keep the pre-fix failure from hanging asyncio.run() while it
                # shuts down the default executor.
                if auth_lock.locked():
                    auth_lock.release()

    asyncio.run(run_test())


def test_async_interactive_auth_waits_then_releases_lock():
    """A queued auth session still runs after the current holder exits."""

    async def run_test():
        auth_lock = threading.Lock()
        auth_lock.acquire()
        websocket_handler = mock.AsyncMock()

        with mock.patch.object(interactive_utils, '_INTERACTIVE_AUTH_LOCK',
                               auth_lock), mock.patch.object(
                                   interactive_utils,
                                   '_handle_interactive_auth_websocket',
                                   websocket_handler):
            auth_task = asyncio.create_task(
                interactive_utils.handle_interactive_auth_async(
                    '<sky-interactive session="next"/>'))
            await asyncio.sleep(0.02)
            websocket_handler.assert_not_awaited()

            auth_lock.release()
            await asyncio.wait_for(auth_task, timeout=1)

        websocket_handler.assert_awaited_once_with('next')
        assert not auth_lock.locked()

    asyncio.run(run_test())


def test_interactive_auth_pipe_registration_failure_closes_duplicate_fds():
    """A failed transport setup must not leak either duplicated descriptor."""
    stdin_read, stdin_write = os.pipe()
    stdout_read, stdout_write = os.pipe()

    async def run_test():
        duplicated_fds = []
        real_dup = os.dup

        def capture_dup(fd):
            duplicated_fd = real_dup(fd)
            duplicated_fds.append(duplicated_fd)
            return duplicated_fd

        class MockWebsocket:

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                del args

        stdin_file = os.fdopen(real_dup(stdin_read), 'rb', buffering=0)
        stdout_file = os.fdopen(real_dup(stdout_write), 'wb', buffering=0)
        loop = asyncio.get_running_loop()
        try:
            with mock.patch('sys.stdin', stdin_file), \
                 mock.patch('sys.stdout', stdout_file), \
                 mock.patch('os.dup', side_effect=capture_dup), \
                 mock.patch(
                     'sky.client.interactive_utils.websockets.connect',
                     return_value=MockWebsocket()), \
                 mock.patch('sky.server.common.get_server_url',
                            return_value='http://test'), \
                 mock.patch(
                     'sky.client.service_account_auth.get_service_account_headers',
                     return_value={}), \
                 mock.patch(
                     'sky.server.common.get_cookie_header_for_url',
                     return_value={}), \
                 mock.patch.object(
                     loop,
                     'connect_read_pipe',
                     side_effect=RuntimeError('pipe registration failed')), \
                 pytest.raises(RuntimeError, match='pipe registration failed'):
                await interactive_utils._handle_interactive_auth_websocket(
                    'test')

            assert len(duplicated_fds) == 2
            for fd in duplicated_fds:
                with pytest.raises(OSError):
                    os.fstat(fd)
        finally:
            stdin_file.close()
            stdout_file.close()

    try:
        asyncio.run(run_test())
    finally:
        for fd in (stdin_read, stdin_write, stdout_read, stdout_write):
            try:
                os.close(fd)
            except OSError:
                pass
