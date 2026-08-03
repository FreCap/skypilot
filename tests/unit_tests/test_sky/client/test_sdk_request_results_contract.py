"""Characterization for the synchronous SDK request-result lifecycle."""

import inspect
from unittest import mock

import pytest

import sky
from sky.client import request_results
from sky.client import sdk


@pytest.mark.parametrize(('function', 'parameter_names', 'top_level_export'), [
    (sdk.stream_response, [
        'request_id', 'response', 'output_stream', 'resumable', 'get_result',
        'relay_rich_status'
    ], False),
    (sdk.get, ['request_id'], True),
    (sdk.stream_and_get, [
        'request_id', 'log_path', 'tail', 'follow', 'output_stream',
        'relay_rich_status'
    ], True),
])
def test_request_result_public_facade(function, parameter_names,
                                      top_level_export):
    assert function.__module__ == 'sky.client.sdk'
    assert list(inspect.signature(function).parameters) == parameter_names
    if top_level_export:
        assert getattr(sky, function.__name__) is function
    else:
        assert not hasattr(sky, function.__name__)


def test_request_result_dependencies_keep_sdk_patch_identity():
    assert sdk.server_common is request_results.server_common
    assert sdk.rest is request_results.rest
    assert sdk.payloads is request_results.payloads
    assert sdk.requests_lib is request_results.requests_lib
    assert sdk.rich_utils is request_results.rich_utils


def test_get_uses_historical_exception_projection_seam():
    response = mock.MagicMock(status_code=200)
    response.json.return_value = {'request_id': 'request-id'}
    request = mock.MagicMock()
    remote_error = ValueError('remote failure')
    request.get_error.return_value = {'object': remote_error}
    projected = RuntimeError('projected by facade')

    with mock.patch(
            'sky.server.common.make_authenticated_request',
            return_value=response), mock.patch.object(
                sdk.payloads,
                'RequestPayload',
                return_value=mock.sentinel.payload), mock.patch(
                    'sky.server.requests.requests.Request.decode',
                    return_value=request), mock.patch(
                        'sky.client.sdk._raise_exception_object_on_client',
                        side_effect=projected) as project:
        with pytest.raises(RuntimeError, match='projected by facade'):
            sdk.get('request-id')

    project.assert_called_once_with(remote_error)
    request.get_return_value.assert_not_called()


def test_stream_response_uses_historical_get_seam():
    response = mock.MagicMock()
    output = mock.MagicMock()

    with mock.patch('sky.utils.rich_utils.decode_rich_status',
                    return_value=[]), mock.patch('sky.client.sdk.get',
                                                 return_value='result') as get:
        assert sdk.stream_response('request-id', response,
                                   output_stream=output) == 'result'

    get.assert_called_once_with('request-id')


def test_stream_and_get_uses_historical_stream_seam():
    response = mock.MagicMock(status_code=200)
    response.headers = {'X-Skypilot-Request-ID': 'request-id'}

    with mock.patch('sky.server.common.make_authenticated_request',
                    return_value=response), mock.patch(
                        'sky.server.common.check_server_healthy_or_start_fn'), \
            mock.patch('sky.client.sdk.stream_response',
                       return_value='result') as stream:
        assert sdk.stream_and_get('request-id') == 'result'

    stream.assert_called_once_with('request-id',
                                   response,
                                   None,
                                   resumable=True,
                                   get_result=True,
                                   relay_rich_status=False)
