"""Unit coverage for direct SkyServe replica REST APIs."""

import base64
import json
from unittest import mock

import fastapi
from fastapi.testclient import TestClient
import pytest

from sky import models
from sky.serve import serve_dashboard
from sky.serve.server import core as serve_core
from sky.serve.server import server
from sky.server import constants as server_constants
from sky.server.requests import payloads
from sky.users import rbac


def _client(auth_user: models.User | None = None) -> TestClient:
    app = fastapi.FastAPI()

    @app.middleware('http')
    async def _request_context(request, call_next):
        request.state.auth_user = auth_user
        request.state.request_id = 'request-id'
        return await call_next(request)

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
    action_fence_version = (
        server_constants.MIN_RESOURCE_ACTION_EXPECTED_CLUSTER_UUID_API_VERSION)
    assert action_fence_version == 69
    execution_quiescence_version = (
        server_constants.MIN_REQUEST_EXECUTION_QUIESCENCE_API_VERSION)
    assert execution_quiescence_version == 70
    pricing_version = (server_constants.MIN_SERVE_DASHBOARD_PRICING_API_VERSION)
    assert pricing_version == 71
    public_capacity_version = (server_constants.MIN_PUBLIC_CAPACITY_API_VERSION)
    assert public_capacity_version == 72
    owner_scoped_request_access_version = (
        server_constants.MIN_OWNER_SCOPED_REQUEST_ACCESS_API_VERSION)
    assert owner_scoped_request_access_version == 73
    ordinary_launch_binding_version = (
        server_constants.MIN_ORDINARY_LAUNCH_BINDING_API_VERSION)
    assert ordinary_launch_binding_version == 74
    placement_projection_version = (
        server_constants.MIN_SERVE_PLACEMENT_PROJECTION_API_VERSION)
    assert placement_projection_version == 77
    preemptible_service_breakdown_version = (
        server_constants.
        MIN_KUBERNETES_PREEMPTIBLE_SERVICE_BREAKDOWN_API_VERSION)
    assert preemptible_service_breakdown_version == 78
    non_pool_launch_binding_version = (
        server_constants.MIN_NON_POOL_LAUNCH_BINDING_API_VERSION)
    assert non_pool_launch_binding_version == 80
    operational_priority_breakdown_version = (
        server_constants.
        MIN_KUBERNETES_OPERATIONAL_PRIORITY_BREAKDOWN_API_VERSION)
    assert operational_priority_breakdown_version == 81
    reserved_fill_status_version = (
        server_constants.
        MIN_SERVE_RESERVED_FILL_RECONCILIATION_STATUS_API_VERSION)
    assert reserved_fill_status_version == 76
    assert (server_constants.MIN_SERVE_DASHBOARD_REPLICA_READS_API_VERSION
            < server_constants.API_VERSION)
    assert execution_quiescence_version < pricing_version
    assert pricing_version < public_capacity_version
    assert public_capacity_version < owner_scoped_request_access_version
    assert owner_scoped_request_access_version < ordinary_launch_binding_version
    assert ordinary_launch_binding_version < reserved_fill_status_version
    assert reserved_fill_status_version < placement_projection_version
    assert (placement_projection_version
            < preemptible_service_breakdown_version)
    assert (preemptible_service_breakdown_version
            < non_pool_launch_binding_version)
    durable_demand_version = (
        server_constants.MIN_SERVE_DURABLE_DEMAND_API_VERSION)
    assert durable_demand_version == 82
    assert non_pool_launch_binding_version < operational_priority_breakdown_version
    assert operational_priority_breakdown_version < durable_demand_version
    route_projection_version = (
        server_constants.MIN_SERVE_ROUTE_PROJECTION_API_VERSION)
    assert route_projection_version == 83
    assert durable_demand_version < route_projection_version
    ordered_capacity_version = (
        server_constants.MIN_SERVE_ORDERED_CAPACITY_ADMISSION_API_VERSION)
    assert ordered_capacity_version == 85
    workload_breakdown_version = (
        server_constants.
        MIN_KUBERNETES_OPERATIONAL_WORKLOAD_BREAKDOWN_API_VERSION)
    assert workload_breakdown_version == 84
    assert route_projection_version < workload_breakdown_version
    assert workload_breakdown_version < ordered_capacity_version
    partial_in_flight_version = (
        server_constants.MIN_SERVE_PARTIAL_IN_FLIGHT_TELEMETRY_API_VERSION)
    assert partial_in_flight_version == 86
    assert ordered_capacity_version < partial_in_flight_version
    termination_evidence_version = (
        server_constants.MIN_EXECUTOR_TERMINATION_EVIDENCE_API_VERSION)
    assert termination_evidence_version == 87
    assert partial_in_flight_version < termination_evidence_version
    incremental_route_version = (
        server_constants.MIN_SERVE_INCREMENTAL_ROUTE_LEASES_API_VERSION)
    assert incremental_route_version == 88
    assert termination_evidence_version < incremental_route_version
    zero_cost_actuation_version = (
        server_constants.MIN_SERVE_ZERO_COST_ACTUATION_API_VERSION)
    assert zero_cost_actuation_version == 89
    assert incremental_route_version < zero_cost_actuation_version
    lazy_version_yaml_version = (
        server_constants.MIN_SERVE_LAZY_VERSION_YAML_API_VERSION)
    assert lazy_version_yaml_version == 90
    assert zero_cost_actuation_version < lazy_version_yaml_version
    assert server_constants.API_VERSION == lazy_version_yaml_version


def test_current_demand_reads_database_without_controller():
    snapshot = {
        'hash': 'hash-a',
        'pool': False,
    }
    summary = {
        'request_telemetry_state': 'fresh',
        'request_telemetry_generation': 7,
        'recent_request_count': 12,
    }
    actuation = {
        'zero_cost_actuation_status': 'available',
        'zero_cost_actuation_reason': 'complete',
        'zero_cost_actuation_mode': 'DURABLE_INTENT',
        'zero_cost_actuation_epoch': 2,
        'zero_cost_actuation_state_counts': {
            'GRANTED': 0,
            'ACTUATING': 1,
            'COMMITTED': 3,
            'RETRYABLE': 0,
            'TERMINAL': 0,
        },
        'pending_zero_cost_actuation_count': 1,
    }
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value=snapshot), \
         mock.patch.object(server.demand_state,
                           'get_request_summary',
                           return_value=summary) as get_summary, \
         mock.patch.object(server.zero_cost_actuation,
                           'get_status_summary',
                           return_value=actuation) as get_actuation, \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client().get('/serve/svc/demand',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == 200
    assert response.json() == {
        'service_name': 'svc',
        'service_hash': 'hash-a',
        **summary,
        **actuation,
    }
    get_summary.assert_called_once_with('svc', 'hash-a')
    get_actuation.assert_called_once_with('svc', 'hash-a')
    schedule.assert_not_awaited()


@pytest.mark.parametrize(('snapshot', 'expected_status'), [(None, 404),
                                                           ({
                                                               'hash': 'hash-a',
                                                               'pool': True
                                                           }, 404),
                                                           ({
                                                               'hash': 'hash-b',
                                                               'pool': False
                                                           }, 409)])
def test_current_demand_fences_service_incarnation(snapshot, expected_status):
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value=snapshot), \
         mock.patch.object(server.demand_state,
                           'get_request_summary') as get_summary:
        response = _client().get('/serve/svc/demand',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == expected_status
    get_summary.assert_not_called()


def test_current_demand_reports_non_consolidated_capability():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(server.demand_state,
                           'get_request_summary') as get_summary:
        response = _client().get('/serve/svc/demand',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == 200
    assert response.json()['request_telemetry_reason'] == 'non_consolidated'
    get_summary.assert_not_called()


def test_current_demand_fences_a_race_after_service_snapshot():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value={
                               'hash': 'hash-a',
                               'pool': False,
                           }), \
         mock.patch.object(
             server.demand_state,
             'get_request_summary',
             return_value={
                 'request_telemetry_state': 'unavailable',
                 'request_telemetry_reason': 'service_incarnation_mismatch',
             }):
        response = _client().get('/serve/svc/demand',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == 409


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
        'service_metadata_included': False,
        'observed_at': None,
        'summaries': [],
    }
    get_summaries.assert_not_called()


@pytest.mark.parametrize('role',
                         [rbac.RoleName.USER.value, rbac.RoleName.VIEWER.value])
def test_exact_replica_summary_is_scoped_to_authenticated_owner(role):
    auth_user = models.User(id='owner-a', name='Owner A')
    payload = {
        'available': True,
        'observed_at': 42.0,
        'summaries': [{
            'service_name': 'owned',
            'service_hash': 'hash-a',
        }],
    }
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[role]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value={
                               'hash': 'hash-a',
                               'pool': False,
                           }) as get_snapshot, \
         mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_summaries',
                           return_value=payload) as get_summaries:
        response = _client(auth_user).get('/serve/replica-summaries',
                                          params={'service_name': 'owned'})

    assert response.status_code == 200
    assert response.json() == payload
    get_snapshot.assert_called_once_with('owned', owner_user_id='owner-a')
    get_summaries.assert_called_once_with(['owned'], 'owner-a')


def test_exact_unauthorized_replica_summary_is_indistinguishable_404():
    auth_user = models.User(id='owner-a', name='Owner A')
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[rbac.RoleName.USER.value]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value=None) as get_snapshot, \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_summaries') as get_summaries:
        response = _client(auth_user).get('/serve/replica-summaries',
                                          params={'service_name': 'other'})

    assert response.status_code == 404
    assert response.json() == {'detail': 'Service not found.'}
    get_snapshot.assert_called_once_with('other', owner_user_id='owner-a')
    get_summaries.assert_not_called()


def test_replica_summary_batch_pushes_owner_scope_into_grouped_query():
    auth_user = models.User(id='owner-a', name='Owner A')
    payload = {
        'available': True,
        'observed_at': 42.0,
        'summaries': [{
            'service_name': 'owned',
            'service_hash': 'hash-a',
        }],
    }
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[rbac.RoleName.USER.value]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot') as get_snapshot, \
         mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_summaries',
                           return_value=payload) as get_summaries:
        response = _client(auth_user).get('/serve/replica-summaries',
                                          params=[('service_name', 'owned'),
                                                  ('service_name', 'other')])

    assert response.status_code == 200
    assert response.json() == payload
    get_snapshot.assert_not_called()
    get_summaries.assert_called_once_with(['owned', 'other'], 'owner-a')


def test_admin_replica_summary_read_remains_unrestricted():
    auth_user = models.User(id='admin-a', name='Admin A')
    payload = {'available': True, 'observed_at': 42.0, 'summaries': []}
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[rbac.RoleName.ADMIN.value]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot') as get_snapshot, \
         mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_replica_summaries',
                           return_value=payload) as get_summaries:
        response = _client(auth_user).get('/serve/replica-summaries')

    assert response.status_code == 200
    get_snapshot.assert_not_called()
    get_summaries.assert_called_once_with(None)


@pytest.mark.parametrize(
    ('path', 'body', 'scheduled_body_type', 'scheduled_handler'),
    [
        ('/serve/status', {
            'service_names': ['owned'],
            'authorized_owner_user_id': 'owner-b',
        }, payloads.ServeAuthorizedStatusBody, serve_core.authorized_status),
        ('/serve/placement', {
            'service_name': 'owned',
            'authorized_owner_user_id': 'owner-b',
        }, payloads.ServeAuthorizedPlacementBody,
         serve_core.authorized_placement),
    ],
)
def test_queued_serve_reads_use_server_owned_worker_capability(
        path, body, scheduled_body_type, scheduled_handler):
    auth_user = models.User(id='owner-a', name='Owner A')
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[rbac.RoleName.VIEWER.value]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value={
                               'hash': 'hash-a',
                               'pool': False,
                           }) as get_snapshot, \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client(auth_user).post(path, json=body)

    assert response.status_code == 200
    get_snapshot.assert_called_once_with('owned', owner_user_id='owner-a')
    scheduled_body = schedule.await_args.kwargs['request_body']
    assert type(scheduled_body) is scheduled_body_type
    assert scheduled_body.authorized_owner_user_id == 'owner-a'
    assert schedule.await_args.kwargs['func'] is scheduled_handler


def test_exact_unauthorized_status_is_404_before_enqueue():
    auth_user = models.User(id='owner-a', name='Owner A')
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[rbac.RoleName.USER.value]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value=None), \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client(auth_user).post('/serve/status',
                                           json={'service_names': ['other']})

    assert response.status_code == 404
    assert response.json() == {'detail': 'Service not found.'}
    schedule.assert_not_awaited()


@pytest.mark.parametrize(
    ('path', 'params'),
    [('/serve/other/demand', {
        'expected_service_hash': 'hash-other'
    }), ('/serve/other/history', {
        'expected_service_hash': 'hash-other'
    }),
     ('/serve/other/replicas', {
         'expected_service_hash': 'hash-other',
         'scope': 'current_or_uncertain',
     }), ('/serve/other/pricing', {
         'expected_service_hash': 'hash-other'
     })],
)
def test_exact_unauthorized_direct_reads_are_indistinguishable_404(
        path, params):
    auth_user = models.User(id='owner-a', name='Owner A')
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=[rbac.RoleName.USER.value]), \
         mock.patch.object(server.serve_state,
                           'get_service_status_snapshot',
                           return_value=None) as get_snapshot, \
         mock.patch.object(server.serve_utils,
                           'is_consolidation_mode') as consolidation:
        response = _client(auth_user).get(path, params=params)

    assert response.status_code == 404
    assert response.json() == {'detail': 'Service not found.'}
    get_snapshot.assert_called_once_with('other', owner_user_id='owner-a')
    consolidation.assert_not_called()


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


def test_pricing_route_passes_raw_repeated_ids_without_executor():
    payload = {
        'available': True,
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'observed_at': 42.0,
        'price_basis': 'version_catalog',
        'aggregate': None,
        'replicas': [],
    }
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_service_pricing',
                           return_value=payload) as get_pricing, \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        response = _client().get('/serve/svc/pricing',
                                 params=[('expected_service_hash', 'hash-a'),
                                         ('replica_id', '7'),
                                         ('replica_id', '7')])

    assert response.status_code == 200
    assert response.json() == payload
    get_pricing.assert_called_once_with('svc', 'hash-a', [7, 7])
    schedule.assert_not_awaited()


def test_pricing_route_no_id_mode_passes_none():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             server.serve_dashboard,
             'get_service_pricing',
             return_value={
                 'available': True,
                 'service_name': 'svc',
                 'service_hash': 'hash-a',
                 'observed_at': 42.0,
                 'price_basis': 'version_catalog',
                 'aggregate': {},
                 'replicas': [],
             }) as get_pricing:
        response = _client().get('/serve/svc/pricing',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == 200
    get_pricing.assert_called_once_with('svc', 'hash-a', None)


def test_pricing_route_reports_non_consolidated_capability():
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(server.serve_dashboard,
                           'get_service_pricing') as get_pricing:
        response = _client().get('/serve/svc/pricing',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == 200
    assert response.json() == {
        'available': False,
        'reason': 'non_consolidated',
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'observed_at': None,
        'price_basis': 'version_catalog',
        'aggregate': None,
        'replicas': [],
    }
    get_pricing.assert_not_called()


@pytest.mark.parametrize(('error', 'status_code'), [
    (serve_dashboard.ServiceNotFoundError('svc'), 404),
    (serve_dashboard.ServiceHashMismatchError('svc'), 409),
])
def test_pricing_route_maps_snapshot_fence_failures(error, status_code):
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(server.serve_dashboard,
                           'get_service_pricing',
                           side_effect=error):
        response = _client().get('/serve/svc/pricing',
                                 params={'expected_service_hash': 'hash-a'})

    assert response.status_code == status_code


@pytest.mark.parametrize('params', [
    [],
    [('expected_service_hash', '')],
    [('expected_service_hash', 'hash-a'), ('replica_id', '0')],
    [('expected_service_hash', 'hash-a'), ('replica_id', '-1')],
    [('expected_service_hash', 'hash-a'), ('replica_id', str(2**31))],
    [('expected_service_hash', 'hash-a'), ('replica_id', 'not-an-int')],
    [('expected_service_hash', 'hash-a')] +
    [('replica_id', str(index + 1)) for index in range(101)],
])
def test_pricing_route_validates_raw_query_before_topology(params):
    with mock.patch.object(server.serve_utils,
                           'is_consolidation_mode') as consolidation:
        response = _client().get('/serve/svc/pricing', params=params)

    assert response.status_code == 422
    consolidation.assert_not_called()
