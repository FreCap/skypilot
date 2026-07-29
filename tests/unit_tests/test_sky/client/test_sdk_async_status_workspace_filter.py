"""Focused tests for async status workspace-filter forwarding."""

from unittest import mock

import pytest

from sky.client import sdk_async
from sky.utils import common as common_utils


@pytest.mark.asyncio
async def test_status_forwards_workspace_filter():
    """Explicit workspace filters should be forwarded to the sync SDK."""

    async def mock_stream_and_get(*_args, **_kwargs):
        return [{'name': 'test-cluster', 'status': 'UP'}]

    async def mock_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    with mock.patch('sky.client.sdk.status',
                    return_value='test-request-id') as mock_status, \
         mock.patch('sky.client.sdk_async.stream_and_get',
                    side_effect=mock_stream_and_get) as stream_and_get_mock, \
         mock.patch('sky.client.sdk_async.asyncio.to_thread',
                    side_effect=mock_to_thread):
        result = await sdk_async.status(
            cluster_names=['test-cluster'],
            refresh=common_utils.StatusRefreshMode.FORCE,
            all_users=True,
            workspaces_filter=['alpha'])

    assert result == [{'name': 'test-cluster', 'status': 'UP'}]
    mock_status.assert_called_once()
    assert mock_status.call_args.args == (['test-cluster'],
                                          common_utils.StatusRefreshMode.FORCE,
                                          True)
    assert mock_status.call_args.kwargs == {
        'workspaces_filter': ['alpha'],
        '_include_credentials': False,
    }
    stream_and_get_mock.assert_called_once_with('test-request-id', None, None,
                                                True, None)
