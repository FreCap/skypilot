"""Focused tests for async status workspace-filter forwarding."""

import inspect
from unittest import mock

import pytest

from sky.client import sdk
from sky.client import sdk_async
from sky.utils import common as common_utils


def test_status_serializes_workspace_filter():
    """The sync SDK should include the requested workspaces in the body."""
    response = mock.Mock()
    with mock.patch(
            'sky.client.sdk.server_common.make_authenticated_request',
            return_value=response) as make_request, \
         mock.patch('sky.client.sdk.server_common.get_request_id',
                    return_value='test-request-id') as get_request_id:
        request_id = inspect.unwrap(sdk.status)(cluster_names=['test-cluster'],
                                                workspaces_filter=['alpha'],
                                                _summary_response=True)

    assert request_id == 'test-request-id'
    make_request.assert_called_once()
    assert make_request.call_args.args == ('POST', '/status')
    body = make_request.call_args.kwargs['json']
    assert body['cluster_names'] == ['test-cluster']
    assert body['workspaces_filter'] == ['alpha']
    assert body['summary_response'] is True
    get_request_id.assert_called_once_with(response)


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
