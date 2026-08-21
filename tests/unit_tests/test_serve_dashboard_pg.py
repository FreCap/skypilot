"""Real-PostgreSQL execution tests for bounded Serve dashboard pricing."""

# pylint: disable=protected-access,redefined-outer-name,unused-import
import uuid

import pytest
from test_serve_resource_action_state_pg import postgres_engine

from sky.serve import serve_dashboard
from sky.serve import serve_state
from sky.serve import spot_placer


@pytest.fixture
def dashboard_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state.Base.metadata.create_all(
        postgres_engine,
        tables=[
            serve_state.placement_normalization_runs_table,
            serve_state.services_table,
            serve_state.replicas_table,
            serve_state.version_specs_table,
        ],
    )
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        lambda: postgres_engine)
    monkeypatch.setattr(serve_state._db_manager, '_engine', postgres_engine)
    return postgres_engine


def _location() -> spot_placer.Location:
    location = spot_placer.Location.from_pickleable({
        'cloud': 'AWS',
        'region': 'us-east-1',
        'zone': 'us-east-1a',
        'use_spot': True,
        'instance_type': 'g6.4xlarge',
        'accelerators': {
            'L4': 1
        },
    })
    assert location is not None
    return location


def _replica_state(replica_id: int, **overrides) -> dict:
    state = {
        'replica_record_id': str(uuid.UUID(int=replica_id)),
        'is_zero_cost': False,
        'location': _location().to_pickleable(),
    }
    state.update(overrides)
    return state


def test_pricing_projection_executes_jsonb_bounds_and_modes(dashboard_database):
    catalog = spot_placer.PlacementCatalog(((_location(), 1.25),),
                                           num_nodes=2).to_dict()
    huge_location = {'oversized': 'x' * 70_000}
    replicas = [
        {
            'service_name': 'svc',
            'replica_id': 1,
            'status': 'READY',
            'version': 3,
            'cluster_name': 'svc-1',
            'created_at': 1.0,
            'is_spot': False,
            'replica_state': _replica_state(1,
                                            is_zero_cost=True,
                                            location=huge_location),
        },
        {
            'service_name': 'svc',
            'replica_id': 2,
            'status': 'READY',
            'version': 3,
            'cluster_name': 'svc-2',
            'created_at': 2.0,
            'is_spot': True,
            'replica_state': _replica_state(2),
        },
        {
            'service_name': 'svc',
            'replica_id': 3,
            'status': 'READY',
            'version': 3,
            'cluster_name': 'svc-3',
            'created_at': 3.0,
            'is_spot': True,
            'replica_state': _replica_state(3, location=huge_location),
        },
        {
            'service_name': 'svc',
            'replica_id': 4,
            'status': 'READY',
            'version': 4,
            'cluster_name': 'svc-4',
            'created_at': 4.0,
            'is_spot': True,
            'replica_state': _replica_state(4),
        },
        {
            'service_name': 'svc',
            'replica_id': 5,
            'status': 'READY',
            'version': 5,
            'cluster_name': 'svc-5',
            'created_at': 5.0,
            'is_spot': True,
            'replica_state': _replica_state(5),
        },
    ]
    with dashboard_database.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc', hash='hash-a', pool=0))
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': 'svc',
            'version': 3,
            'placement_catalog': catalog,
        }, {
            'service_name': 'svc',
            'version': 4,
            'placement_catalog': [],
        }])
        connection.execute(serve_state.replicas_table.insert(), replicas)

    aggregate_response = serve_dashboard.get_service_pricing('svc', 'hash-a')
    row_response = serve_dashboard.get_service_pricing('svc', 'hash-a',
                                                       [1, 2, 3, 4, 5])

    assert aggregate_response['aggregate'] == {
        'available': True,
        'unavailable_reason': None,
        'coverage': 'partial',
        'known_hourly_cost': 2.5,
        'spot_hourly_cost': 2.5,
        'non_spot_hourly_cost': 0.0,
        'tracked_replica_count': 5,
        'priced_replica_count': 2,
        'excluded_replica_count': 3,
        'exclusion_reasons': {
            'invalid_version_catalog': 1,
            'missing_version_catalog': 1,
            'pricing_identity_too_large': 1,
        },
    }
    rows = {row['replica_id']: row for row in row_response['replicas']}
    assert rows[1]['hourly_cost'] == 0.0
    assert rows[1]['price_source'] == 'zero_cost_provenance'
    assert rows[1]['pricing_fingerprint'] is not None
    assert rows[2]['hourly_cost'] == 2.5
    assert rows[2]['price_source'] == 'version_catalog'
    assert rows[3]['pricing_fingerprint'] is None
    assert rows[3]['hourly_cost_exclusion_reason'] == (
        'pricing_identity_too_large')
    assert rows[4]['hourly_cost_exclusion_reason'] == (
        'invalid_version_catalog')
    assert rows[5]['hourly_cost_exclusion_reason'] == (
        'missing_version_catalog')


def test_dashboard_reads_apply_owner_scope_before_selection_and_aggregation(
        dashboard_database):
    services = [{
        'name': 'owned',
        'hash': 'hash-owned',
        'pool': 0,
        'status': 'READY',
        'owner_user_id': 'owner-a',
        'owner_user_name': 'Owner A',
    }, {
        'name': 'other',
        'hash': 'hash-other',
        'pool': 0,
        'status': 'READY',
        'owner_user_id': 'owner-b',
        'owner_user_name': 'Owner B',
    }, {
        'name': 'unattributed',
        'hash': 'hash-null',
        'pool': 0,
        'status': 'READY',
        'owner_user_id': None,
        'owner_user_name': None,
    }]
    with dashboard_database.begin() as connection:
        connection.execute(serve_state.services_table.insert(), services)
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': service['name'],
            'version': 1,
        } for service in services])

    scoped = serve_dashboard.get_replica_summaries(owner_user_id='owner-a')
    unrestricted = serve_dashboard.get_replica_summaries()

    assert [row['service_name'] for row in scoped['summaries']] == ['owned']
    assert {row['service_name'] for row in unrestricted['summaries']
           } == {'owned', 'other', 'unattributed'}
    assert serve_state.get_service_status_snapshot(
        'owned', owner_user_id='owner-a') is not None
    assert serve_state.get_service_status_snapshot(
        'other', owner_user_id='owner-a') is None
    assert serve_state.get_service_status_snapshot(
        'unattributed', owner_user_id='owner-a') is None
    assert serve_state.get_service_from_name(
        'owned', owner_user_id='owner-a') is not None
    assert serve_state.get_service_from_name('other',
                                             owner_user_id='owner-a') is None
    assert serve_state.get_glob_service_names(
        ['*'], pool=False, owner_user_id='owner-a') == ['owned']

    with pytest.raises(serve_dashboard.ServiceNotFoundError):
        serve_dashboard.get_replica_page(
            'other',
            'hash-other',
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE,
            50,
            None,
            owner_user_id='owner-a',
        )
    with pytest.raises(serve_dashboard.ServiceNotFoundError):
        serve_dashboard.get_service_pricing(
            'unattributed',
            'hash-null',
            owner_user_id='owner-a',
        )
