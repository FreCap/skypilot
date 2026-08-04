"""Unit coverage for the direct SkyServe history REST API."""

from unittest import mock

import fastapi
from fastapi.testclient import TestClient
import pytest

from sky.serve.server import server


def _client() -> TestClient:
    app = fastapi.FastAPI()
    app.include_router(server.router, prefix='/serve')
    return TestClient(app)


def test_direct_history_reads_only_selected_sections_without_executor():
    payload = {
        'available': True,
        'service_hash': 'hash-a',
        'request_samples': [],
        'autoscaler_samples': [],
    }
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             server.serve_state,
             'get_service_status_snapshot',
             return_value={'hash': 'hash-a', 'pool': False}), \
         mock.patch.object(server.serve_history,
                           'get_status_history',
                           return_value=payload) as get_history, \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client().get('/serve/svc/history',
                                 params=[
                                     ('expected_service_hash', 'hash-a'),
                                     ('hours', '12'),
                                     ('section', 'requests'),
                                     ('section', 'autoscaler'),
                                 ])

    assert response.status_code == 200
    assert response.json() == payload
    get_history.assert_called_once_with(
        'svc',
        hours=12,
        expected_service_hash='hash-a',
        sections={'requests', 'autoscaler'},
    )
    schedule.assert_not_awaited()


def test_direct_history_defaults_to_all_sections():
    payload = {'available': True, 'service_hash': 'hash-a'}
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             server.serve_state,
             'get_service_status_snapshot',
             return_value={'hash': 'hash-a', 'pool': False}), \
         mock.patch.object(server.serve_history,
                           'get_status_history',
                           return_value=payload) as get_history:
        response = _client().get('/serve/svc/history',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == 200
    assert get_history.call_args.kwargs['sections'] == {
        'requests', 'replicas', 'prediction', 'autoscaler'
    }


def test_direct_history_reports_non_consolidated_capability():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot') as get_service, \
         mock.patch.object(server.serve_history,
                           'get_status_history') as get_history:
        response = _client().get('/serve/svc/history',
                                 params=[
                                     ('expected_service_hash', 'hash-a'),
                                     ('section', 'requests'),
                                 ])

    assert response.status_code == 200
    assert response.json() == {
        'available': False,
        'reason': 'non_consolidated',
        'bucket_seconds': 60,
        'retention_hours': 72,
        'request_samples': [],
        'rejection_history_available': False,
        'request_window_seconds': 3600,
        'requests_last_hour': 0,
    }
    get_service.assert_not_called()
    get_history.assert_not_called()


@pytest.mark.parametrize(('reason', 'status_code'),
                         [('service_not_found', 404),
                          ('service_hash_mismatch', 409)])
def test_direct_history_maps_identity_failures(reason, status_code):
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             server.serve_state,
             'get_service_status_snapshot',
             return_value={'hash': 'hash-old', 'pool': False}), \
         mock.patch.object(
             server.serve_history,
             'get_status_history',
             return_value={'available': False, 'reason': reason}):
        response = _client().get('/serve/svc/history',
                                 params={'expected_service_hash': 'hash-old'})

    assert response.status_code == status_code


@pytest.mark.parametrize(('service', 'status_code'), [
    (None, 404),
    ({
        'hash': 'hash-new',
        'pool': False
    }, 409),
    ({
        'hash': 'hash-old',
        'pool': True
    }, 404),
])
def test_direct_history_checks_identity_before_history_read(
        service, status_code):
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value=service), \
         mock.patch.object(server.serve_history,
                           'get_status_history') as get_history:
        response = _client().get('/serve/svc/history',
                                 params={'expected_service_hash': 'hash-old'})

    assert response.status_code == status_code
    get_history.assert_not_called()


@pytest.mark.parametrize('params', [
    {},
    {
        'expected_service_hash': 'hash-a',
        'hours': 0
    },
    {
        'expected_service_hash': 'hash-a',
        'hours': 73
    },
    {
        'expected_service_hash': 'hash-a',
        'section': 'unknown'
    },
])
def test_direct_history_validates_query_bounds(params):
    response = _client().get('/serve/svc/history', params=params)

    assert response.status_code == 422
