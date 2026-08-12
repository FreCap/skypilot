"""Request-status body transport and compatibility coverage."""

import inspect
from unittest import mock

from sky.client import sdk
from sky.server import constants as server_constants


def test_api_status_large_filters_use_body_transport() -> None:
    response = mock.Mock(status_code=200)
    response.json.return_value = []
    response.raise_for_status.return_value = None
    cluster_names = [f'service-replica-{index}' for index in range(2159)]
    request_ids = [f'request-{index}' for index in range(2159)]

    with mock.patch.object(
            sdk.versions,
            'get_remote_api_version',
            return_value=server_constants.
            MIN_REQUEST_EXECUTION_QUIESCENCE_API_VERSION), \
         mock.patch.object(sdk.server_common,
                           'is_api_server_local',
                           return_value=False), \
         mock.patch.object(
             sdk.server_common,
             'make_authenticated_request',
             return_value=response) as make_request:
        result = inspect.unwrap(sdk.api_status)(
            request_ids=request_ids,
            all_status=True,
            cluster_names=cluster_names,
            _include_request_names=['sky.launch'],
            _execution_quiescence_candidates_only=True,
            _exact_request_ids=True,
            _use_body=True)

    assert result == []
    make_request.assert_called_once()
    assert make_request.call_args.args == ('POST', '/api/status/query')
    assert 'params' not in make_request.call_args.kwargs
    body = make_request.call_args.kwargs['json']
    assert body['request_ids'] == request_ids
    assert body['cluster_names'] == cluster_names
    assert body['include_request_names'] == ['sky.launch']
    assert body['execution_quiescence_candidates_only'] is True
    assert body['exact_request_ids'] is True


def test_api_status_small_legacy_query_stays_get() -> None:
    response = mock.Mock(status_code=200)
    response.json.return_value = []
    response.raise_for_status.return_value = None

    with mock.patch.object(
            sdk.versions, 'get_remote_api_version', return_value=69), \
         mock.patch.object(sdk.server_common,
                           'is_api_server_local',
                           return_value=False), \
         mock.patch.object(
             sdk.server_common,
             'make_authenticated_request',
             return_value=response) as make_request:
        result = inspect.unwrap(sdk.api_status)(request_ids=['request-1'])

    assert result == []
    assert make_request.call_args.args == ('GET', '/api/status')
    assert 'params' in make_request.call_args.kwargs
    assert 'json' not in make_request.call_args.kwargs


def test_api_status_internal_observation_can_bound_one_attempt() -> None:
    response = mock.Mock(status_code=200)
    response.json.return_value = []
    response.raise_for_status.return_value = None

    with mock.patch.object(
            sdk.versions,
            'get_remote_api_version',
            return_value=server_constants.
            MIN_REQUEST_EXECUTION_QUIESCENCE_API_VERSION), \
         mock.patch.object(sdk.server_common,
                           'is_api_server_local',
                           return_value=False), \
         mock.patch.object(
             sdk.server_common,
             'make_authenticated_request',
             return_value=response) as make_request:
        result = inspect.unwrap(sdk.api_status)(
            request_ids=['request-1'],
            _exact_request_ids=True,
            _use_body=True,
            _request_timeout_seconds=5,
            _retry_on_server_unavailable=False)

    assert result == []
    assert make_request.call_args.args == ('POST', '/api/status/query')
    assert make_request.call_args.kwargs['timeout'] == 5
    assert make_request.call_args.kwargs['retry'] is False
    assert make_request.call_args.kwargs['allow_non_get_without_retry'] is True
