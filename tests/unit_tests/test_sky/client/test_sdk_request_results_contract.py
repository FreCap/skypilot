"""Safety contract for observing durable API request results.

These tests model a mutation whose submission already returned a request ID.
An ambiguous result read may be repeated, but must never be interpreted as
authorization to repeat the mutation.
"""

import inspect
import logging
from unittest import mock

import pytest
import requests
import urllib3

import sky
from sky import exceptions
from sky.client import common as client_common
from sky.client import request_results
from sky.client import sdk
from sky.server import constants as server_constants
from sky.server.requests import requests as requests_lib

_REQUEST_ID = 'request-result-00000001'
_LOGGER = logging.getLogger(__name__)


def _response(status_code,
              *,
              headers=None,
              json_value=None,
              text='gateway response'):
    response = mock.MagicMock(status_code=status_code)
    response.headers = {} if headers is None else headers
    response.json.return_value = json_value
    response.text = text
    return response


def _decoded_request(*,
                     value='done',
                     error=None,
                     request_id=_REQUEST_ID,
                     status=None):
    request = mock.MagicMock()
    request.request_id = request_id
    if status is None:
        status = (requests_lib.RequestStatus.FAILED if error is not None else
                  requests_lib.RequestStatus.SUCCEEDED)
    request.status = status
    request.get_error.return_value = (None if error is None else {
        'object': error
    })
    request.get_return_value.return_value = value
    return request


def _raise(error):
    raise error


def _get(request_id=_REQUEST_ID):
    return request_results.get(request_id,
                               raise_exception=_raise,
                               logger=_LOGGER)


@pytest.mark.parametrize('transient_status', [502, 503, 504])
def test_transient_gateway_response_retries_the_same_result_request(
        transient_status):
    transient = _response(transient_status)
    success = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request()

    with mock.patch.object(
            request_results.server_common,
            'make_authenticated_request',
            side_effect=[transient, success]) as fetch, mock.patch.object(
                request_results.payloads,
                'RequestPayload',
                return_value=mock.sentinel.payload), mock.patch.object(
                    request_results.requests_lib.Request,
                    'decode',
                    return_value=decoded), mock.patch.object(
                        request_results.context_utils,
                        'sleep_with_cancellation'):
        assert _get() == 'done'

    assert [call.args[:2] for call in fetch.call_args_list] == [
        ('GET', f'/api/get?request_id={_REQUEST_ID}'),
        ('GET', f'/api/get?request_id={_REQUEST_ID}'),
    ]
    for call in fetch.call_args_list:
        assert call.kwargs['retry'] is False
        assert call.kwargs['raise_for_server_unavailable'] is False
        assert call.kwargs['timeout'] == (
            client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
            client_common.API_SERVER_REQUEST_RESULT_READ_TIMEOUT_SECONDS)


def test_exhausted_transport_reads_raise_typed_unavailable_for_same_id():
    with mock.patch.object(
            request_results.server_common,
            'make_authenticated_request',
            side_effect=requests.exceptions.ReadTimeout('lost ACK')) as fetch, \
         mock.patch.object(request_results.context_utils,
                           'sleep_with_cancellation') as sleep:
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            _get()

    assert exc.value.request_id == _REQUEST_ID
    assert fetch.call_count == 3
    assert sleep.call_count == 2
    assert all(call.args[1] == f'/api/get?request_id={_REQUEST_ID}'
               for call in fetch.call_args_list)


def test_urllib3_transport_failure_remains_retryable():
    transient = urllib3.exceptions.ProtocolError('lost response ACK')
    success = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request()

    with mock.patch.object(
            request_results.server_common,
            'make_authenticated_request',
            side_effect=[transient, success]) as fetch, mock.patch.object(
                request_results.payloads,
                'RequestPayload',
                return_value=mock.sentinel.payload), mock.patch.object(
                    request_results.requests_lib.Request,
                    'decode',
                    return_value=decoded), mock.patch.object(
                        request_results.context_utils,
                        'sleep_with_cancellation'):
        assert _get() == 'done'

    assert fetch.call_count == 2


@pytest.mark.parametrize('response_version', [None, '94', 'not-an-int'])
def test_v95_marker_is_rejected_without_v95_response_version(response_version):
    headers = {
        server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER: _REQUEST_ID,
    }
    if response_version is not None:
        headers[server_constants.API_VERSION_HEADER] = response_version
    response = _response(503,
                         headers=headers,
                         json_value={'detail': 'proxy unavailable'})

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.context_utils,
                               'sleep_with_cancellation'):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            _get()


def test_v95_exact_marker_authorizes_replay():
    response = _response(
        503,
        headers={
            server_constants.API_VERSION_HEADER: '95',
            server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER: _REQUEST_ID,
        },
        json_value={'detail': f'Request {_REQUEST_ID!r} should be retried'})

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response):
        with pytest.raises(exceptions.RequestResultShouldRetryError) as exc:
            _get()

    assert exc.value.request_id == _REQUEST_ID


@pytest.mark.parametrize('marker', [
    'different-request-id',
    _REQUEST_ID[:-1],
])
def test_v95_non_exact_marker_never_authorizes_replay(marker):
    response = _response(
        503,
        headers={
            server_constants.API_VERSION_HEADER: '95',
            server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER: marker,
        },
        json_value={'detail': f'Request {_REQUEST_ID!r} should be retried'})

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.context_utils,
                               'sleep_with_cancellation'):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            _get()


def test_v95_detail_without_marker_never_authorizes_replay():
    response = _response(
        503,
        headers={server_constants.API_VERSION_HEADER: '95'},
        json_value={'detail': f'Request {_REQUEST_ID!r} should be retried'})

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.context_utils,
                               'sleep_with_cancellation'):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            _get()


def test_v94_exact_legacy_detail_is_rejected_after_marker_cutover():
    response = _response(
        503,
        headers={server_constants.API_VERSION_HEADER: '94'},
        json_value={'detail': f'Request {_REQUEST_ID!r} should be retried'})

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response) as fetch, mock.patch.object(
                               request_results.context_utils,
                               'sleep_with_cancellation') as sleep:
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            _get()

    assert exc.value.request_id == _REQUEST_ID
    assert fetch.call_count == 3
    assert sleep.call_count == 2
    # Transitional characterization: the floor rejects v94, but the legacy
    # parser remains until the stacked cleanup PR removes it.
    assert response.json.call_count == 3


def test_malformed_success_is_an_unknown_observation():
    response = _response(200, json_value={'not': 'a request payload'})
    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               side_effect=ValueError('malformed')):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            _get()
    assert exc.value.request_id == _REQUEST_ID


def test_malformed_500_detail_is_an_unknown_observation_and_closes_response():
    response = _response(500, json_value={'detail': 'not a request payload'})

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID
    response.close.assert_called_once_with()


@pytest.mark.parametrize('malformed_error', [
    ['not', 'an', 'error', 'envelope'],
    {
        'object': 'not an exception'
    },
    {
        'wrong-key': ValueError('not addressable')
    },
])
def test_malformed_decoded_error_is_an_unknown_observation(malformed_error):
    response = _response(500, json_value={'detail': {'encoded': 'request'}})
    decoded = _decoded_request(error=ValueError('placeholder'))
    decoded.get_error.return_value = malformed_error

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID


def test_decoded_error_failure_is_an_unknown_observation():
    response = _response(500, json_value={'detail': {'encoded': 'request'}})
    decoded = _decoded_request(error=ValueError('placeholder'))
    decoded.get_error.side_effect = ValueError('corrupt error envelope')

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID


@pytest.mark.parametrize('status', [
    requests_lib.RequestStatus.PENDING,
    requests_lib.RequestStatus.WAITING,
    requests_lib.RequestStatus.RUNNING,
    requests_lib.RequestStatus.FAILED,
])
def test_non_success_without_error_is_an_unknown_observation(status):
    response = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request(status=status)

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID
    decoded.get_return_value.assert_not_called()


def test_reconciliation_does_not_treat_cancellation_as_replay_authority():
    response = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request(status=requests_lib.RequestStatus.CANCELLED)

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID
    decoded.get_return_value.assert_not_called()


def test_public_get_preserves_historical_cancellation_projection():
    response = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request(status=requests_lib.RequestStatus.CANCELLED)

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestCancelled):
            _get()

    decoded.get_return_value.assert_not_called()


def test_terminal_error_requires_failed_status_and_error_http_response():
    operation_error = ValueError('provider rejected down')
    response = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request(error=operation_error)

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)


def test_success_requires_success_http_response():
    response = _response(500, json_value={'detail': {'encoded': 'request'}})
    decoded = _decoded_request()

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError):
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)


def test_return_value_decode_failure_is_an_unknown_observation():
    response = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request()
    decoded.get_return_value.side_effect = ValueError('corrupt return value')

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID


def test_reconciliation_rejects_a_decoded_request_id_mismatch():
    response = _response(200, json_value={'encoded': 'request'})
    decoded = _decoded_request(request_id='different-request-id')

    with mock.patch.object(request_results.server_common,
                           'make_authenticated_request',
                           return_value=response), mock.patch.object(
                               request_results.payloads,
                               'RequestPayload',
                               return_value=mock.sentinel.payload), \
            mock.patch.object(request_results.requests_lib.Request,
                              'decode', return_value=decoded):
        with pytest.raises(exceptions.RequestResultUnavailableError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID


def test_reconciliation_wraps_only_a_decoded_application_error():
    operation_error = ValueError('provider rejected down')
    response = _response(500, json_value={'detail': {'encoded': 'request'}})
    decoded = _decoded_request(error=operation_error)

    with mock.patch.object(
            request_results.server_common,
            'make_authenticated_request',
            return_value=response), mock.patch.object(
                request_results.payloads,
                'RequestPayload',
                return_value=mock.sentinel.payload), mock.patch.object(
                    request_results.requests_lib.Request,
                    'decode',
                    return_value=decoded):
        with pytest.raises(exceptions.RequestResultApplicationError) as exc:
            request_results.get_for_reconciliation(_REQUEST_ID, logger=_LOGGER)

    assert exc.value.request_id == _REQUEST_ID
    assert exc.value.error is operation_error


def test_public_sdk_get_keeps_its_original_signature():
    parameters = inspect.signature(inspect.unwrap(sdk.get)).parameters
    assert list(parameters) == ['request_id']


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
    response = mock.MagicMock(status_code=500)
    response.headers = {}
    response.json.return_value = {'detail': {'request_id': 'request-id'}}
    request = mock.MagicMock()
    request.request_id = 'request-id'
    request.status = requests_lib.RequestStatus.FAILED
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
