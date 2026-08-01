"""Unit coverage for direct SkyServe replica REST APIs."""

import base64
import json
from unittest import mock

import fastapi
from fastapi.testclient import TestClient
import pytest

from sky.serve import serve_dashboard
from sky.serve.server import server
from sky.server import constants as server_constants


def _client() -> TestClient:
    app = fastapi.FastAPI()
    app.include_router(server.router, prefix='/serve')
    return TestClient(app)


def test_replica_reads_have_a_distinct_api_capability_version():
    assert server_constants.MIN_SERVE_DASHBOARD_HISTORY_API_VERSION == 66
    assert (server_constants.MIN_SERVE_DASHBOARD_DIRECT_READS_API_VERSION ==
            server_constants.MIN_SERVE_DASHBOARD_HISTORY_API_VERSION)
    assert server_constants.MIN_SERVE_DASHBOARD_REPLICA_READS_API_VERSION == 67
    spend_request_version = (
        server_constants.MIN_ESTIMATED_SPEND_NON_REJECTED_REQUESTS_API_VERSION)
    assert spend_request_version == 68
    assert (server_constants.MIN_SERVE_DASHBOARD_REPLICA_READS_API_VERSION
            < server_constants.API_VERSION)
    assert server_constants.API_VERSION == spend_request_version


def test_replica_summaries_batch_repeated_names_without_executor():
    payload = {
        'available': True,
        'observed_at': 42.0,
        'summaries': [],
    }
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_summaries',
                           return_value=payload) as get_summaries, \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client().get('/serve/replica-summaries',
                                 params=[('service_name', 'svc-a'),
                                         ('service_name', 'svc-b')])

    assert response.status_code == 200
    assert response.json() == payload
    get_summaries.assert_called_once_with(['svc-a', 'svc-b'])
    schedule.assert_not_awaited()


def test_replica_summaries_without_names_selects_all_services():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             server.serve_dashboard,
             'get_replica_summaries',
             return_value={
                 'available': True,
                 'observed_at': 42.0,
                 'summaries': [],
             }) as get_summaries:
        response = _client().get('/serve/replica-summaries')

    assert response.status_code == 200
    get_summaries.assert_called_once_with(None)


def test_replica_summaries_report_non_consolidated_capability():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_summaries') as get_summaries:
        response = _client().get('/serve/replica-summaries')

    assert response.status_code == 200
    assert response.json() == {
        'available': False,
        'reason': 'non_consolidated',
        'observed_at': None,
        'summaries': [],
    }
    get_summaries.assert_not_called()


def test_replica_page_is_bounded_and_avoids_executor():
    payload = {
        'available': True,
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'scope': 'past_attempts',
        'replica_unit': 'physical_backend',
        'observed_at': 42.0,
        'total': 1,
        'replicas': [],
        'next_cursor': None,
    }
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_page',
                           return_value=payload) as get_page, \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client().get('/serve/svc/replicas',
                                 params={
                                     'expected_service_hash': 'hash-a',
                                     'scope': 'past_attempts',
                                     'limit': 25,
                                     'cursor': 'cursor-a',
                                 })

    assert response.status_code == 200
    assert response.json() == payload
    get_page.assert_called_once_with('svc', 'hash-a', 'past_attempts', 25,
                                     'cursor-a')
    schedule.assert_not_awaited()


def test_replica_page_reports_non_consolidated_capability():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_page') as get_page:
        response = _client().get('/serve/svc/replicas',
                                 params={
                                     'expected_service_hash': 'hash-a',
                                     'scope': 'current_or_uncertain',
                                 })

    assert response.status_code == 200
    assert response.json() == {
        'available': False,
        'reason': 'non_consolidated',
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'scope': 'current_or_uncertain',
        'replica_unit': None,
        'observed_at': None,
        'total': 0,
        'replicas': [],
        'next_cursor': None,
    }
    get_page.assert_not_called()


@pytest.mark.parametrize(('error', 'status_code'), [
    (serve_dashboard.ServiceNotFoundError('svc'), 404),
    (serve_dashboard.ServiceHashMismatchError('svc'), 409),
    (serve_dashboard.ReplicaCursorMismatchError('cursor'), 409),
    (serve_dashboard.InvalidReplicaCursorError('cursor'), 422),
])
def test_replica_page_maps_read_failures(error, status_code):
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_page',
                           side_effect=error):
        response = _client().get('/serve/svc/replicas',
                                 params={
                                     'expected_service_hash': 'hash-a',
                                     'scope': 'current_or_uncertain',
                                 })

    assert response.status_code == status_code


def test_replica_page_rejects_non_string_cursor_scope():
    cursor_payload = {
        'hash': 'hash-a',
        'last': 1,
        'max': 1,
        'scope': [],
        'version': 1,
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(cursor_payload).encode()).decode().rstrip('=')
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True):
        response = _client().get('/serve/svc/replicas',
                                 params={
                                     'expected_service_hash': 'hash-a',
                                     'scope': 'current_or_uncertain',
                                     'cursor': f'v1.{encoded}',
                                 })

    assert response.status_code == 422


@pytest.mark.parametrize('params', [
    {},
    {
        'expected_service_hash': 'hash-a'
    },
    {
        'expected_service_hash': 'hash-a',
        'scope': 'unknown'
    },
    {
        'expected_service_hash': 'hash-a',
        'scope': 'past_attempts',
        'limit': 0
    },
    {
        'expected_service_hash': 'hash-a',
        'scope': 'past_attempts',
        'limit': 101
    },
    {
        'expected_service_hash': 'hash-a',
        'scope': 'past_attempts',
        'cursor': ''
    },
])
def test_replica_page_validates_query_contract(params):
    response = _client().get('/serve/svc/replicas', params=params)

    assert response.status_code == 422
