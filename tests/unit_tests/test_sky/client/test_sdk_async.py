"""Unit tests for sky/client/sdk_async.py."""
import asyncio
import inspect
from unittest import mock

import pytest

from sky import exceptions
from sky.client import sdk
from sky.client import sdk_async
from sky.schemas.api import responses
from sky.server import constants as server_constants
from sky.server.requests import requests as requests_lib
from sky.utils import common as common_utils


@pytest.fixture
def mock_get():
    """Mock the get() function to return a mock response."""

    async def mock_get_async(*args, **kwargs):
        return mock_get.return_value

    with mock.patch('sky.client.sdk_async.get',
                    side_effect=mock_get_async) as mock_get:
        yield mock_get


@pytest.fixture
def mock_stream_and_get():
    """Mock the stream_and_get() function to return a mock response."""

    async def mock_stream_and_get_async(*args, **kwargs):
        return mock_stream_and_get.return_value

    with mock.patch(
            'sky.client.sdk_async.stream_and_get',
            side_effect=mock_stream_and_get_async) as mock_stream_and_get:
        yield mock_stream_and_get


@pytest.fixture
def mock_to_thread():
    """Mock asyncio.to_thread to run synchronously."""

    async def mock_to_thread_func(func, *args, **kwargs):
        return func(*args, **kwargs)

    with mock.patch('sky.client.sdk_async.asyncio.to_thread',
                    side_effect=mock_to_thread_func):
        yield


@pytest.fixture
def mock_sdk_functions():
    """Mock the underlying SDK functions."""
    with mock.patch('sky.client.sdk.check') as mock_check, \
         mock.patch('sky.client.sdk.enabled_clouds') as mock_enabled_clouds, \
         mock.patch('sky.client.sdk.list_accelerators') as mock_list_accelerators, \
         mock.patch('sky.client.sdk.list_accelerator_counts') as mock_list_accelerator_counts, \
         mock.patch('sky.client.sdk.workspaces') as mock_workspaces, \
         mock.patch('sky.client.sdk.status') as mock_status, \
         mock.patch('sky.client.sdk.endpoints') as mock_endpoints, \
         mock.patch('sky.client.sdk.storage_ls') as mock_storage_ls, \
         mock.patch('sky.client.sdk.storage_delete') as mock_storage_delete, \
         mock.patch('sky.client.sdk.local_up') as mock_local_up, \
         mock.patch('sky.client.sdk.local_down') as mock_local_down, \
         mock.patch('sky.client.sdk.ssh_down') as mock_ssh_down, \
         mock.patch('sky.client.sdk.api_cancel') as mock_api_cancel, \
         mock.patch('sky.client.sdk.api_info') as mock_api_info, \
         mock.patch('sky.client.sdk.api_stop') as mock_api_stop, \
         mock.patch('sky.client.sdk.api_server_logs') as mock_api_server_logs, \
         mock.patch('sky.client.sdk.api_login') as mock_api_login:

        yield {
            'check': mock_check,
            'enabled_clouds': mock_enabled_clouds,
            'list_accelerators': mock_list_accelerators,
            'list_accelerator_counts': mock_list_accelerator_counts,
            'workspaces': mock_workspaces,
            'status': mock_status,
            'endpoints': mock_endpoints,
            'storage_ls': mock_storage_ls,
            'storage_delete': mock_storage_delete,
            'local_up': mock_local_up,
            'local_down': mock_local_down,
            'ssh_down': mock_ssh_down,
            'api_cancel': mock_api_cancel,
            'api_info': mock_api_info,
            'api_stop': mock_api_stop,
            'api_server_logs': mock_api_server_logs,
            'api_login': mock_api_login,
        }


@pytest.mark.asyncio
async def test_check_with_stream(mock_stream_and_get, mock_to_thread,
                                 mock_sdk_functions):
    """Test check() function with stream_logs=True (default)."""
    # Mock the underlying SDK function
    mock_sdk_functions['check'].return_value = 'test-request-id'

    # Mock the stream_and_get result
    expected_result = {'aws': ['us-west-1'], 'gcp': ['us-central1']}
    mock_stream_and_get.return_value = expected_result

    result = await sdk_async.check(('aws', 'gcp'), True)
    assert result == expected_result
    mock_sdk_functions['check'].assert_called_once_with(('aws', 'gcp'), True,
                                                        None)
    # The function should be called with request_id and the default StreamConfig parameters
    # Based on the error: stream_and_get('test-request-id', None, None, True, None)
    mock_stream_and_get.assert_called_once_with('test-request-id', None, None,
                                                True, None)


@pytest.mark.asyncio
async def test_check_no_stream(mock_get, mock_to_thread, mock_sdk_functions):
    """Test check() function with stream_logs=False."""
    # Mock the underlying SDK function
    mock_sdk_functions['check'].return_value = 'test-request-id'

    # Mock the get result
    expected_result = {'aws': ['us-west-1'], 'gcp': ['us-central1']}
    mock_get.return_value = expected_result

    result = await sdk_async.check(('aws', 'gcp'), True, stream_logs=None)
    assert result == expected_result
    mock_sdk_functions['check'].assert_called_once_with(('aws', 'gcp'), True,
                                                        None)
    mock_get.assert_called_once_with('test-request-id')


@pytest.mark.asyncio
async def test_enabled_clouds(mock_stream_and_get, mock_to_thread,
                              mock_sdk_functions):
    """Test enabled_clouds() function."""
    mock_sdk_functions['enabled_clouds'].return_value = 'test-request-id'

    expected_result = ['aws', 'gcp']
    mock_stream_and_get.return_value = expected_result

    result = await sdk_async.enabled_clouds(expand=True)
    assert result == expected_result
    mock_sdk_functions['enabled_clouds'].assert_called_once_with(None, True)
    # The function should be called with request_id and the default StreamConfig parameters
    # Based on the error: stream_and_get('test-request-id', None, None, True, None)
    mock_stream_and_get.assert_called_once_with('test-request-id', None, None,
                                                True, None)


@pytest.mark.asyncio
async def test_list_accelerators(mock_stream_and_get, mock_to_thread,
                                 mock_sdk_functions):
    """Test list_accelerators() function."""
    mock_sdk_functions['list_accelerators'].return_value = 'test-request-id'

    expected_result = {'aws': ['p3.2xlarge'], 'gcp': ['n1-standard-4']}
    mock_stream_and_get.return_value = expected_result

    result = await sdk_async.list_accelerators(gpus_only=True,
                                               name_filter='p3',
                                               region_filter='us-west-1',
                                               quantity_filter=1,
                                               clouds=['aws'],
                                               all_regions=True,
                                               require_price=True,
                                               case_sensitive=True)
    assert result == expected_result
    mock_sdk_functions['list_accelerators'].assert_called_once_with(
        True, 'p3', 'us-west-1', 1, ['aws'], True, True, True)
    # The function should be called with request_id and the default StreamConfig parameters
    # Based on the error: stream_and_get('test-request-id', None, None, True, None)
    mock_stream_and_get.assert_called_once_with('test-request-id', None, None,
                                                True, None)


@pytest.mark.asyncio
async def test_status(mock_stream_and_get, mock_to_thread, mock_sdk_functions):
    """Test status() function."""
    mock_sdk_functions['status'].return_value = 'test-request-id'

    expected_result = [{'name': 'test-cluster', 'status': 'UP'}]
    mock_stream_and_get.return_value = expected_result

    result = await sdk_async.status(
        cluster_names=['test-cluster'],
        refresh=common_utils.StatusRefreshMode.FORCE,
        all_users=True)
    assert result == expected_result
    mock_sdk_functions['status'].assert_called_once_with(
        ['test-cluster'],
        common_utils.StatusRefreshMode.FORCE,
        True,
        _include_credentials=False)
    # The function should be called with request_id and the default StreamConfig parameters
    # Based on the error: stream_and_get('test-request-id', None, None, True, None)
    mock_stream_and_get.assert_called_once_with('test-request-id', None, None,
                                                True, None)


@pytest.mark.asyncio
async def test_endpoints(mock_stream_and_get, mock_to_thread,
                         mock_sdk_functions):
    """Test endpoints() function."""
    mock_sdk_functions['endpoints'].return_value = 'test-request-id'

    expected_result = {8080: 'http://1.2.3.4:8080'}
    mock_stream_and_get.return_value = expected_result

    result = await sdk_async.endpoints('test-cluster', 8080)
    assert result == expected_result
    mock_sdk_functions['endpoints'].assert_called_once_with(
        'test-cluster', 8080)
    # The function should be called with request_id and the default StreamConfig parameters
    # Based on the error: stream_and_get('test-request-id', None, None, True, None)
    mock_stream_and_get.assert_called_once_with('test-request-id', None, None,
                                                True, None)


@pytest.mark.asyncio
async def test_storage_ls(mock_stream_and_get, mock_to_thread,
                          mock_sdk_functions):
    """Test storage_ls() function."""
    mock_sdk_functions['storage_ls'].return_value = 'test-request-id'

    expected_result = [{'name': 'test-storage', 'status': 'READY'}]
    mock_stream_and_get.return_value = expected_result

    result = await sdk_async.storage_ls()
    assert result == expected_result
    mock_sdk_functions['storage_ls'].assert_called_once()
    # The function should be called with request_id and the default StreamConfig parameters
    # Based on the error: stream_and_get('test-request-id', None, None, True, None)
    mock_stream_and_get.assert_called_once_with('test-request-id', None, None,
                                                True, None)


@pytest.mark.asyncio
async def test_error_propagation(mock_stream_and_get, mock_to_thread,
                                 mock_sdk_functions):
    """Test that errors from stream_and_get are properly propagated."""
    mock_sdk_functions['check'].return_value = 'test-request-id'

    # Mock stream_and_get to raise an exception
    mock_stream_and_get.side_effect = ValueError('Test error')

    with pytest.raises(ValueError, match='Test error'):
        await sdk_async.check(('aws', 'gcp'), True)


@pytest.mark.asyncio
async def test_get_error_propagation(mock_get, mock_to_thread,
                                     mock_sdk_functions):
    """Test that errors from get are properly propagated."""
    mock_sdk_functions['check'].return_value = 'test-request-id'

    # Mock get to raise an exception
    mock_get.side_effect = RuntimeError(
        'Failed to get request test-request-id: 404 Not Found')

    with pytest.raises(RuntimeError,
                       match='Failed to get request test-request-id'):
        await sdk_async.check(('aws', 'gcp'), True, stream_logs=None)


# Test async functions that use to_thread but don't stream
@pytest.mark.asyncio
async def test_api_info(mock_to_thread, mock_sdk_functions):
    """Test api_info() function."""
    return_value = {
        'status': 'healthy',
        'api_version': '1.0.0',
        'version': '1.0.0',
        'version_on_disk': '1.0.0',
        'commit': '1234567890',
        'basic_auth_enabled': False,
        'user': None,
    }
    expected_result = responses.APIHealthResponse(**return_value)
    mock_sdk_functions['api_info'].return_value = expected_result

    result = await sdk_async.api_info()
    assert result == expected_result
    mock_sdk_functions['api_info'].assert_called_once()


@pytest.mark.asyncio
async def test_api_stop(mock_to_thread, mock_sdk_functions):
    """Test api_stop() function."""
    mock_sdk_functions['api_stop'].return_value = None

    result = await sdk_async.api_stop()
    assert result is None
    mock_sdk_functions['api_stop'].assert_called_once()


@pytest.mark.asyncio
async def test_api_server_logs(mock_to_thread, mock_sdk_functions):
    """Test api_server_logs() function."""
    mock_sdk_functions['api_server_logs'].return_value = None

    result = await sdk_async.api_server_logs(follow=True, tail=100)
    assert result is None
    mock_sdk_functions['api_server_logs'].assert_called_once_with(True, 100)


@pytest.mark.asyncio
async def test_api_login(mock_to_thread, mock_sdk_functions):
    """Test api_login() function."""
    mock_sdk_functions['api_login'].return_value = None

    result = await sdk_async.api_login('http://test-endpoint',
                                       relogin=True,
                                       service_account_token='sa-token',
                                       no_browser=True)
    assert result is None
    mock_sdk_functions['api_login'].assert_called_once_with(
        'http://test-endpoint',
        relogin=True,
        service_account_token='sa-token',
        no_browser=True)


@pytest.mark.asyncio
async def test_api_login_signature_mirrors_sync():
    """The async wrapper must expose the sync api_login parameters."""
    async_params = inspect.signature(sdk_async.api_login).parameters
    sync_params = inspect.signature(sdk.api_login).parameters
    assert list(async_params) == [*sync_params, 'get_token']
    for name, param in sync_params.items():
        assert async_params[name].default == param.default
        assert async_params[name].kind == param.kind
    assert async_params['get_token'].default is None
    assert async_params['get_token'].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.asyncio
async def test_api_login_preserves_legacy_get_token_keyword(
        mock_to_thread, mock_sdk_functions):
    """The historical get_token keyword keeps its relogin behavior."""
    await sdk_async.api_login('http://test-endpoint', get_token=True)
    mock_sdk_functions['api_login'].assert_called_once_with(
        'http://test-endpoint',
        relogin=True,
        service_account_token=None,
        no_browser=False)


@pytest.mark.asyncio
async def test_api_login_rejects_both_relogin_names(mock_to_thread,
                                                    mock_sdk_functions):
    with pytest.raises(ValueError, match='both relogin and get_token'):
        await sdk_async.api_login(relogin=True, get_token=True)
    mock_sdk_functions['api_login'].assert_not_called()


def _request_payload_dict(status):
    return {
        'request_id': 'req-1',
        'name': 'launch',
        'entrypoint': '',
        'request_body': '',
        'status': status,
        'created_at': 0.0,
        'user_id': 'user',
        'return_value': 'null',
        'error': 'null',
        'pid': None,
        'schedule_type': 'long',
    }


class _FakeGetResponse:

    def __init__(self, status, body, *, headers=None, body_error=None):
        self.status = status
        self._body = body
        self.headers = {} if headers is None else headers
        self._body_error = body_error
        self.closed = False

    async def read(self):
        if self._body_error is not None:
            raise self._body_error
        return b'buffered response body'

    async def json(self):
        return self._body

    async def text(self):
        return 'error body'

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_get_raises_typed_error_from_500_detail():
    """A failed request payload is decoded from the response detail."""
    response = _FakeGetResponse(500,
                                {'detail': _request_payload_dict('FAILED')})
    decoded = mock.MagicMock()
    decoded.status = requests_lib.RequestStatus.FAILED
    decoded.get_error.return_value = {
        'object': exceptions.StorageSpecError('bad storage spec')
    }

    with mock.patch(
            'sky.client.sdk_async.server_common.'
            'make_authenticated_request_async',
            new=mock.AsyncMock(return_value=response)), \
         mock.patch(
             'sky.client.sdk_async.server_common.'
             'check_server_healthy_or_start_fn'), \
         mock.patch(
             'sky.client.sdk_async.requests_lib.Request.decode',
             return_value=decoded) as mock_decode:
        with pytest.raises(exceptions.StorageSpecError, match='bad storage'):
            await sdk_async.get('req-1')

    decoded_payload = mock_decode.call_args.args[0]
    assert decoded_payload.request_id == 'req-1'
    assert decoded_payload.status == 'FAILED'
    assert response.closed


@pytest.mark.asyncio
async def test_get_success_still_decodes_top_level_payload():
    """A successful request payload remains top-level and returns its value."""
    response = _FakeGetResponse(200, _request_payload_dict('SUCCEEDED'))
    decoded = mock.MagicMock()
    decoded.get_error.return_value = None
    decoded.status = requests_lib.RequestStatus.SUCCEEDED
    decoded.get_return_value.return_value = {'result': 42}

    with mock.patch(
            'sky.client.sdk_async.server_common.'
            'make_authenticated_request_async',
            new=mock.AsyncMock(return_value=response)), \
         mock.patch(
             'sky.client.sdk_async.server_common.'
             'check_server_healthy_or_start_fn'), \
         mock.patch(
             'sky.client.sdk_async.requests_lib.Request.decode',
             return_value=decoded) as mock_decode:
        result = await sdk_async.get('req-1')

    assert result == {'result': 42}
    decoded_payload = mock_decode.call_args.args[0]
    assert decoded_payload.request_id == 'req-1'
    assert decoded_payload.status == 'SUCCEEDED'
    assert response.closed


@pytest.mark.asyncio
@pytest.mark.parametrize('transient_status', [502, 503, 504])
async def test_get_retries_transient_gateway_result_with_same_id(
        transient_status):
    transient = _FakeGetResponse(transient_status, {'detail': 'gateway'})
    success = _FakeGetResponse(200, _request_payload_dict('SUCCEEDED'))
    decoded = mock.MagicMock()
    decoded.get_error.return_value = None
    decoded.status = requests_lib.RequestStatus.SUCCEEDED
    decoded.get_return_value.return_value = 'done'
    fetch = mock.AsyncMock(side_effect=[transient, success])

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async', new=fetch), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.requests_lib.Request.decode',
                       return_value=decoded), \
            mock.patch('sky.client.sdk_async.asyncio.sleep',
                       new=mock.AsyncMock()):
        assert await sdk_async.get('req-1') == 'done'

    assert [call.args[2] for call in fetch.await_args_list] == [
        '/api/get?request_id=req-1',
        '/api/get?request_id=req-1',
    ]
    assert all(call.kwargs['raise_for_server_unavailable'] is False
               for call in fetch.await_args_list)
    assert all(call.kwargs['timeout'].sock_read is not None
               for call in fetch.await_args_list)
    assert transient.closed
    assert success.closed


@pytest.mark.asyncio
async def test_get_retries_timeout_while_buffering_body():
    timed_out = _FakeGetResponse(
        200,
        _request_payload_dict('SUCCEEDED'),
        body_error=asyncio.TimeoutError('body ACK lost'))
    success = _FakeGetResponse(200, _request_payload_dict('SUCCEEDED'))
    decoded = mock.MagicMock()
    decoded.get_error.return_value = None
    decoded.status = requests_lib.RequestStatus.SUCCEEDED
    decoded.get_return_value.return_value = 'done'
    fetch = mock.AsyncMock(side_effect=[timed_out, success])

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async', new=fetch), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.requests_lib.Request.decode',
                       return_value=decoded), \
            mock.patch('sky.client.sdk_async.asyncio.sleep',
                       new=mock.AsyncMock()):
        assert await sdk_async.get('req-1') == 'done'

    assert fetch.await_count == 2
    assert timed_out.closed
    assert success.closed


@pytest.mark.asyncio
async def test_get_cancellation_while_buffering_body_closes_without_retry():
    cancelled = _FakeGetResponse(200,
                                 _request_payload_dict('SUCCEEDED'),
                                 body_error=asyncio.CancelledError())
    fetch = mock.AsyncMock(return_value=cancelled)

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async', new=fetch), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.asyncio.sleep',
                       new=mock.AsyncMock()):
        with pytest.raises(asyncio.CancelledError):
            await sdk_async.get('req-1')

    assert fetch.await_count == 1
    assert cancelled.closed


@pytest.mark.asyncio
async def test_get_exhausted_timeouts_raise_typed_unavailable():
    fetch = mock.AsyncMock(
        side_effect=asyncio.TimeoutError('result endpoint unavailable'))

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async', new=fetch), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.asyncio.sleep',
                       new=mock.AsyncMock()):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            await sdk_async.get('req-1')

    assert exc.value.request_id == 'req-1'
    assert fetch.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [
    requests_lib.RequestStatus.PENDING,
    requests_lib.RequestStatus.WAITING,
    requests_lib.RequestStatus.RUNNING,
    requests_lib.RequestStatus.FAILED,
])
async def test_get_rejects_non_success_without_error(status):
    response = _FakeGetResponse(200, _request_payload_dict(status.value))
    decoded = mock.MagicMock()
    decoded.status = status
    decoded.get_error.return_value = None
    decoded.get_return_value.return_value = 'unsafe-result'

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async',
                    new=mock.AsyncMock(return_value=response)), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.requests_lib.Request.decode',
                       return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            await sdk_async.get('req-1')

    decoded.get_return_value.assert_not_called()


@pytest.mark.asyncio
async def test_get_terminal_error_requires_failed_http_response():
    response = _FakeGetResponse(200, _request_payload_dict('FAILED'))
    decoded = mock.MagicMock()
    decoded.status = requests_lib.RequestStatus.FAILED
    decoded.get_error.return_value = {
        'object': exceptions.StorageSpecError('unsafe projection')
    }

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async',
                    new=mock.AsyncMock(return_value=response)), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.requests_lib.Request.decode',
                       return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            await sdk_async.get('req-1')


@pytest.mark.asyncio
async def test_get_success_requires_success_http_response():
    response = _FakeGetResponse(500,
                                {'detail': _request_payload_dict('SUCCEEDED')})
    decoded = mock.MagicMock()
    decoded.status = requests_lib.RequestStatus.SUCCEEDED
    decoded.get_error.return_value = None
    decoded.get_return_value.return_value = 'unsafe-result'

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async',
                    new=mock.AsyncMock(return_value=response)), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.requests_lib.Request.decode',
                       return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            await sdk_async.get('req-1')

    decoded.get_return_value.assert_not_called()


@pytest.mark.asyncio
async def test_get_v95_exact_marker_authorizes_replay():
    response = _FakeGetResponse(
        503, {'detail': "Request 'req-1' should be retried"},
        headers={
            server_constants.API_VERSION_HEADER: '95',
            server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER: 'req-1',
        })

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async',
                    new=mock.AsyncMock(return_value=response)), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'):
        with pytest.raises(exceptions.RequestResultShouldRetryError):
            await sdk_async.get('req-1')

    assert response.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(('version', 'marker'), [
    (None, 'req-1'),
    ('94', 'req-1'),
    ('95', 'req'),
    ('95', 'different-request'),
    ('95', None),
])
async def test_get_rejects_untrusted_or_non_exact_retry_marker(version, marker):
    headers = {}
    if version is not None:
        headers[server_constants.API_VERSION_HEADER] = version
    if marker is not None:
        headers[server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER] = marker
    detail = ("Request 'req-1' should be retried"
              if version != '94' else 'proxy unavailable')
    response = _FakeGetResponse(503, {'detail': detail}, headers=headers)

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async',
                    new=mock.AsyncMock(return_value=response)), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.asyncio.sleep',
                       new=mock.AsyncMock()):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            await sdk_async.get('req-1')


@pytest.mark.asyncio
async def test_get_rejects_exact_v94_legacy_retry_detail_after_marker_cutover():
    response = _FakeGetResponse(503,
                                {'detail': "Request 'req-1' should be retried"},
                                headers={
                                    server_constants.API_VERSION_HEADER: '94',
                                })
    response.json = mock.AsyncMock(wraps=response.json)
    fetch = mock.AsyncMock(return_value=response)
    sleep = mock.AsyncMock()

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async',
                    new=fetch), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.asyncio.sleep', new=sleep):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            await sdk_async.get('req-1')

    assert exc.value.request_id == 'req-1'
    assert fetch.await_count == 3
    assert sleep.await_count == 2
    # Transitional characterization: the floor rejects v94, but the legacy
    # parser remains until the stacked cleanup PR removes it.
    assert response.json.await_count == 3


@pytest.mark.asyncio
async def test_get_cancellation_during_backoff_stops_observation():
    transient = _FakeGetResponse(502, {'detail': 'gateway'})
    success = _FakeGetResponse(200, _request_payload_dict('SUCCEEDED'))
    fetch = mock.AsyncMock(side_effect=[transient, success])

    with mock.patch('sky.client.sdk_async.server_common.'
                    'make_authenticated_request_async', new=fetch), \
            mock.patch('sky.client.sdk_async.server_common.'
                       'check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk_async.asyncio.sleep',
                       new=mock.AsyncMock(side_effect=asyncio.CancelledError)):
        with pytest.raises(asyncio.CancelledError):
            await sdk_async.get('req-1')

    assert fetch.await_count == 1
    assert transient.closed
