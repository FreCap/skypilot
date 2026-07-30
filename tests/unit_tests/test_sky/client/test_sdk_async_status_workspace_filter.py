"""Focused tests for async status workspace-filter forwarding."""

import inspect
from unittest import mock

import pytest

from sky import exceptions
from sky.client import sdk
from sky.client import sdk_async
from sky.server import constants as server_constants
from sky.utils import common as common_utils


def test_status_serializes_workspace_filter():
    """The sync SDK should include the requested workspaces in the body."""
    response = mock.Mock()
    with mock.patch(
            'sky.client.sdk.versions.get_remote_api_version',
            return_value=server_constants.
            MIN_STATUS_WORKSPACE_FILTER_API_VERSION), \
         mock.patch(
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


@pytest.mark.parametrize('workspaces_filter', [['alpha'], []])
def test_status_rejects_workspace_filter_for_old_server(workspaces_filter):
    """Old servers must not silently ignore an explicit workspace filter."""
    with mock.patch(
            'sky.client.sdk.versions.get_remote_api_version',
            return_value=server_constants.
            MIN_STATUS_WORKSPACE_FILTER_API_VERSION - 1), \
         mock.patch(
             'sky.client.sdk.server_common.make_authenticated_request'
         ) as make_request, \
         mock.patch('sky.utils.ux_utils.print_exception_no_traceback'):
        with pytest.raises(exceptions.APINotSupportedError,
                           match='Filtering cluster status by workspace'):
            inspect.unwrap(sdk.status)(workspaces_filter=workspaces_filter)

    make_request.assert_not_called()


def test_status_without_workspace_filter_supports_old_server():
    """The compatibility guard must not change the omitted-filter call."""
    response = mock.Mock()
    with mock.patch(
            'sky.client.sdk.versions.get_remote_api_version',
            return_value=server_constants.
            MIN_STATUS_WORKSPACE_FILTER_API_VERSION - 1), \
         mock.patch(
             'sky.client.sdk.server_common.make_authenticated_request',
             return_value=response) as make_request, \
         mock.patch('sky.client.sdk.server_common.get_request_id',
                    return_value='test-request-id'):
        request_id = inspect.unwrap(sdk.status)()

    assert request_id == 'test-request-id'
    make_request.assert_called_once()
    assert make_request.call_args.kwargs['json']['workspaces_filter'] is None


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
