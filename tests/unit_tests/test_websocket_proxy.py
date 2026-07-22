"""Tests for the client-side SSH WebSocket proxy script."""

import asyncio
import importlib.util
import pathlib
from unittest import mock

import pytest


def _load_websocket_proxy_module():
    path = (pathlib.Path(__file__).resolve().parents[2] / 'sky' / 'templates' /
            'websocket_proxy.py')
    spec = importlib.util.spec_from_file_location('websocket_proxy', path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Unable to load WebSocket proxy from {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


websocket_proxy = _load_websocket_proxy_module()


async def _run_with_mock_pipes(websocket, timestamps_supported):
    loop = asyncio.get_running_loop()
    transport = mock.Mock()
    transport.is_closing.return_value = True
    protocol = mock.Mock()
    with mock.patch.object(websocket_proxy.os, 'isatty', return_value=False), \
         mock.patch.object(loop, 'connect_read_pipe',
                           new=mock.AsyncMock(
                               return_value=(transport, protocol))), \
         mock.patch.object(loop, 'connect_write_pipe',
                           new=mock.AsyncMock(
                               return_value=(transport, protocol))):
        await websocket_proxy.run_websocket_proxy(websocket,
                                                  timestamps_supported)


@pytest.mark.asyncio
async def test_proxy_cancels_blocked_peers_when_receiver_fails():
    stdin_started = asyncio.Event()
    latency_started = asyncio.Event()
    stdin_cancelled = asyncio.Event()
    latency_cancelled = asyncio.Event()

    async def blocked_stdin(*_args):
        stdin_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stdin_cancelled.set()

    async def failed_receiver(*_args):
        await stdin_started.wait()
        await latency_started.wait()
        raise RuntimeError('invalid WebSocket frame')

    async def blocked_latency(*_args):
        latency_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            latency_cancelled.set()

    with mock.patch.object(websocket_proxy, 'stdin_to_websocket',
                           side_effect=blocked_stdin), \
         mock.patch.object(websocket_proxy, 'websocket_to_stdout',
                           side_effect=failed_receiver), \
         mock.patch.object(websocket_proxy, 'latency_monitor',
                           side_effect=blocked_latency):
        with pytest.raises(RuntimeError, match='invalid WebSocket frame'):
            await asyncio.wait_for(_run_with_mock_pipes(mock.Mock(), True),
                                   timeout=1)

    assert stdin_cancelled.is_set()
    assert latency_cancelled.is_set()


@pytest.mark.asyncio
async def test_proxy_cancels_stdin_when_receiver_closes_without_timestamps():
    stdin_started = asyncio.Event()
    stdin_cancelled = asyncio.Event()

    async def blocked_stdin(*_args):
        stdin_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stdin_cancelled.set()

    async def closed_receiver(*_args):
        await stdin_started.wait()

    latency_monitor = mock.AsyncMock()
    with mock.patch.object(websocket_proxy, 'stdin_to_websocket',
                           side_effect=blocked_stdin), \
         mock.patch.object(websocket_proxy, 'websocket_to_stdout',
                           side_effect=closed_receiver), \
         mock.patch.object(websocket_proxy, 'latency_monitor',
                           latency_monitor):
        await asyncio.wait_for(_run_with_mock_pipes(mock.Mock(), False),
                               timeout=1)

    assert stdin_cancelled.is_set()
    latency_monitor.assert_not_awaited()
