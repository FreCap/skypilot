"""Regression tests for coroutine request worker cancellation."""

import asyncio
import time
from unittest import mock

import pytest

from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.utils import context_utils


@pytest.mark.asyncio
async def test_execute_request_coroutine_propagates_worker_cancellation():
    request = requests_lib.Request(
        request_id='worker-cancelled',
        name='test-request-name',
        status=requests_lib.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='test-user-id',
        entrypoint=mock.Mock(),
        request_body=payloads.RequestBody(),
    )
    worker_future = asyncio.get_running_loop().create_future()
    worker_future.cancel()
    mock_ctx = mock.Mock()
    mock_ctx.redirect_log.return_value = mock.sentinel.original_output
    set_cancelled = mock.AsyncMock()
    set_succeeded = mock.AsyncMock()
    set_failed = mock.AsyncMock()

    with mock.patch('sky.utils.context.initialize'), \
         mock.patch('sky.utils.context.get', return_value=mock_ctx), \
         mock.patch.object(requests_lib, 'update_status_async',
                           new_callable=mock.AsyncMock), \
         mock.patch.object(requests_lib, 'get_request_log_storage_usage',
                           return_value=mock.Mock(hard_free_bytes=1024)), \
         mock.patch.object(executor, 'get_request_thread_executor',
                           return_value=mock.sentinel.executor), \
         mock.patch.object(context_utils, 'to_thread_with_executor',
                           return_value=worker_future), \
         mock.patch.object(requests_lib, 'get_request_status_async',
                           new_callable=mock.AsyncMock,
                           return_value=requests_lib.StatusWithMsg(
                               requests_lib.RequestStatus.RUNNING)), \
         mock.patch.object(requests_lib, 'set_request_cancelled_async',
                           set_cancelled), \
         mock.patch.object(requests_lib, 'set_request_succeeded_async',
                           set_succeeded), \
         mock.patch.object(requests_lib, 'set_request_failed_async', set_failed):
        task = executor.execute_request_in_coroutine(request)
        with pytest.raises(asyncio.CancelledError):
            await task.task

    set_cancelled.assert_awaited_once_with(request.request_id)
    set_succeeded.assert_not_awaited()
    set_failed.assert_not_awaited()
    mock_ctx.cancel.assert_called_once_with()
