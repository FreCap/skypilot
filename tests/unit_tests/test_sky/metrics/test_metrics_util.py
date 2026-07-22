"""Unit tests for sky.metrics.utils."""
import asyncio
import subprocess
import threading
from unittest import mock

import pytest

from sky.metrics import utils


def test_start_svc_port_forward_terminates_on_exception():
    """Test subprocess is terminated when exception occurs."""
    mock_process = mock.MagicMock(spec=subprocess.Popen)
    mock_process.poll.return_value = None
    mock_process.stdout = mock.MagicMock()
    mock_process.stdout.fileno.return_value = 1

    mock_poller = mock.MagicMock()
    mock_poller.poll.side_effect = Exception('Test error')

    with mock.patch('subprocess.Popen',
                    return_value=mock_process), \
         mock.patch('time.time', side_effect=[0, 1, 2]), \
         mock.patch('select.poll',
                    return_value=mock_poller), \
         mock.patch('time.sleep'):

        with pytest.raises(Exception, match='Test error'):
            utils.start_svc_port_forward(context='test-context',
                                         namespace='test-ns',
                                         service='test-svc',
                                         service_port=8080)

        # Verify subprocess was terminated
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called()


def test_start_svc_port_forward_terminates_on_timeout():
    """Test subprocess is terminated when no local port found."""
    mock_process = mock.MagicMock(spec=subprocess.Popen)
    mock_process.poll.return_value = None
    mock_process.stdout = mock.MagicMock()
    mock_process.stdout.fileno.return_value = 1

    mock_poller = mock.MagicMock()
    mock_poller.poll.return_value = []  # No events (timeout)

    # Simulate timeout by advancing time past the timeout threshold
    with mock.patch('subprocess.Popen',
                    return_value=mock_process), \
         mock.patch('time.time', side_effect=[0] + [11] * 10), \
         mock.patch('select.poll',
                    return_value=mock_poller), \
         mock.patch('time.sleep'):

        with pytest.raises(RuntimeError, match='Failed to extract local port'):
            utils.start_svc_port_forward(context='test-context',
                                         namespace='test-ns',
                                         service='test-svc',
                                         service_port=8080)

        # Verify subprocess was terminated
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called()


@pytest.mark.asyncio
async def test_cancelled_port_forward_start_stops_eventual_process():
    """Cancellation must not lose a process still being created by a worker."""
    start_entered = threading.Event()
    allow_start = threading.Event()
    process_stopped = threading.Event()
    mock_process = mock.MagicMock(spec=subprocess.Popen)

    def _start(*args, **kwargs):
        del args, kwargs
        start_entered.set()
        assert allow_start.wait(timeout=5)
        return mock_process, 12345

    def _stop(process):
        assert process is mock_process
        process_stopped.set()

    with mock.patch.object(utils,
                           'start_svc_port_forward',
                           side_effect=_start), \
         mock.patch.object(utils,
                           'stop_svc_port_forward',
                           side_effect=_stop):
        try:
            request_task = asyncio.create_task(
                utils.send_metrics_request_with_port_forward(
                    context='test-context',
                    namespace='test-ns',
                    service='test-svc',
                    service_port=8080))
            assert await asyncio.to_thread(start_entered.wait, 5)

            request_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request_task

            allow_start.set()
            assert await asyncio.to_thread(process_stopped.wait, 5)
        finally:
            allow_start.set()


@pytest.mark.asyncio
async def test_cancelled_port_forward_start_stops_unclaimed_process():
    """Cancellation must claim a process returned before await resumes."""
    mock_process = mock.MagicMock(spec=subprocess.Popen)

    async def _start_then_cancel(func):
        result = func()
        current_task = asyncio.current_task()
        assert current_task is not None
        current_task.cancel()
        await asyncio.sleep(0)
        return result

    with mock.patch.object(
            utils,
            'start_svc_port_forward',
            return_value=(mock_process, 12345)), \
         mock.patch.object(utils, 'stop_svc_port_forward') as mock_stop, \
         mock.patch.object(utils.asyncio,
                           'to_thread',
                           side_effect=_start_then_cancel):
        with pytest.raises(asyncio.CancelledError):
            await utils.send_metrics_request_with_port_forward(
                context='test-context',
                namespace='test-ns',
                service='test-svc',
                service_port=8080)

    mock_stop.assert_called_once_with(mock_process)
