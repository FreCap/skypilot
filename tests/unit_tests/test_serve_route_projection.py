"""Pure contracts for the provider-free SkyServe route document."""
# pylint: disable=protected-access

import copy
import datetime
import hashlib
import json
import types
import uuid

import pytest

from sky.serve import constants
from sky.serve import route_projection
from sky.serve import serve_state
from sky.serve import system_recovery_state


def _replica(replica_id: int,
             url: str,
             *,
             ready: bool = True,
             capable: bool = False):
    del url
    return types.SimpleNamespace(
        replica_id=replica_id,
        replica_record_id=str(uuid.uuid4()),
        version=1,
        status=(serve_state.ReplicaStatus.READY
                if ready else serve_state.ReplicaStatus.NOT_READY),
        is_terminal=False,
        is_zero_cost=(replica_id % 2 == 0),
        system_recovery_disposition=(
            system_recovery_state.SystemRecoveryDisposition.CAPABLE if capable
            else system_recovery_state.SystemRecoveryDisposition.ORDINARY),
    )


def _build(infos, materials, verified, *, marker=lambda info, url: None):
    return route_projection.build_route_view(
        infos,
        materials,
        set(verified), {1}, {1: True},
        service_version=1,
        routing_spec={'load_balancing_policy_name': 'round_robin'},
        capacity_hint={'replica_unit': 'physical_backend'},
        route_allowed=lambda info: True,
        marker_for_route=marker,
        retire_route=lambda info: None)


def test_occupancy_context_excludes_capacity_but_fences_replica_identity():
    response = {
        'service_version': 7,
        'routing_spec': {
            'load_balancing_policy_name': 'least_load'
        },
        'replica_info': {
            'http://replica:8080': {
                'async_occupancy': 'true',
            }
        },
        'num_ready_replicas': 1,
        'capacity_hint': {
            'warm_retention_target_by_accelerator': {}
        },
    }
    identities = {
        'http://replica:8080': {
            'replica_record_id': str(uuid.uuid4()),
        }
    }
    initial = route_projection._occupancy_context_sha256(response, identities)
    explicit_empty = {**response, constants.LB_DRAINING_REPLICA_INFO_KEY: {}}
    assert (route_projection._occupancy_context_sha256(explicit_empty,
                                                       identities) == initial)

    changed_capacity = dict(response)
    changed_capacity['capacity_hint'] = {
        'warm_retention_target_by_accelerator': {
            'H200': 30
        }
    }
    assert (route_projection._occupancy_context_sha256(changed_capacity,
                                                       identities) == initial)

    successor = {
        'http://replica:8080': {
            'replica_record_id': str(uuid.uuid4()),
        }
    }
    assert (route_projection._occupancy_context_sha256(response, successor)
            != initial)

    retired_url = 'http://retired:8080'
    with_draining = {
        **response,
        constants.LB_DRAINING_REPLICA_INFO_KEY: {
            retired_url: {
                'gpu_type': 'L4',
                'gpu_count': '1',
                'is_zero_cost': 'false',
                'async_occupancy': 'true',
            },
        },
    }
    draining_identities = {
        **identities,
        retired_url: {
            'replica_record_id': str(uuid.uuid4()),
            'advertised': False,
        },
    }
    draining = route_projection._occupancy_context_sha256(
        with_draining, draining_identities)
    assert draining != initial
    draining_identities[retired_url] = {
        **draining_identities[retired_url],
        'replica_record_id': str(uuid.uuid4()),
    }
    assert (route_projection._occupancy_context_sha256(with_draining,
                                                       draining_identities)
            != draining)


def test_demand_report_context_fences_queue_mode_not_capacity_or_ack():
    response = {
        'service_version': 7,
        'routing_spec': {
            'load_balancing_policy_name': 'least_load'
        },
        'replica_info': {
            'http://replica:8080': {
                'async_occupancy': 'true',
            }
        },
        'capacity_hint': {
            'warm_retention_target_by_accelerator': {}
        },
        'request_history_accepted': False,
        'queued_compatibility_demand_supported': True,
    }
    identities = {
        'http://replica:8080': {
            'replica_record_id': str(uuid.uuid4()),
        }
    }
    digest = route_projection.demand_report_route_context_sha256(
        response, identities)

    capacity_only = copy.deepcopy(response)
    capacity_only['capacity_hint'] = {
        'warm_retention_target_by_accelerator': {
            'H200': 30
        }
    }
    ready_count_only = copy.deepcopy(response)
    ready_count_only['num_ready_replicas'] = 2
    acknowledged = copy.deepcopy(response)
    acknowledged['request_history_accepted'] = True
    queue_rollback = copy.deepcopy(response)
    queue_rollback['queued_compatibility_demand_supported'] = False
    changed_replica = copy.deepcopy(response)
    changed_replica['replica_info']['http://replica:8080'][
        'async_occupancy'] = 'false'
    changed_identity = copy.deepcopy(identities)
    changed_identity['http://replica:8080']['replica_record_id'] = str(
        uuid.uuid4())
    changed_draining = copy.deepcopy(response)
    changed_draining[constants.LB_DRAINING_REPLICA_INFO_KEY] = {
        'http://retired:8080': {
            'async_occupancy': 'true',
        }
    }
    changed_routing = copy.deepcopy(response)
    changed_routing['routing_spec']['load_balancing_policy_name'] = (
        'round_robin')

    assert (route_projection.demand_report_route_context_sha256(
        capacity_only, identities) == digest)
    assert (route_projection.demand_report_route_context_sha256(
        ready_count_only, identities) == digest)
    assert (route_projection.demand_report_route_context_sha256(
        acknowledged, identities) == digest)
    assert (route_projection.demand_report_route_context_sha256(
        queue_rollback, identities) != digest)
    assert (route_projection.demand_report_route_context_sha256(
        changed_replica, identities) != digest)
    assert (route_projection.demand_report_route_context_sha256(
        response, changed_identity) != digest)
    assert (route_projection.demand_report_route_context_sha256(
        changed_draining, identities) != digest)
    assert (route_projection.demand_report_route_context_sha256(
        changed_routing, identities) != digest)


def test_selected_route_context_ignores_unrelated_fleet_churn():
    selected_url = 'http://selected:8080'
    response = {
        'service_version': 7,
        'routing_spec': {
            'load_balancing_policy_name': 'least_load',
        },
        'replica_info': {
            selected_url: {
                'gpu_type': 'L4',
                'gpu_count': '1',
            },
        },
        'capacity_hint': {
            'ready': 1,
        },
    }
    identities = {
        selected_url: {
            'replica_record_id': str(uuid.uuid4()),
        },
    }
    initial = route_projection.selected_route_context_sha256(
        response, identities, selected_url)
    unrelated_url = 'http://unrelated:8080'
    expanded = copy.deepcopy(response)
    expanded['capacity_hint']['ready'] = 2
    expanded['replica_info'][unrelated_url] = {
        'gpu_type': 'H200',
        'gpu_count': '8',
    }
    expanded_identities = copy.deepcopy(identities)
    expanded_identities[unrelated_url] = {
        'replica_record_id': str(uuid.uuid4()),
    }
    assert route_projection.selected_route_context_sha256(
        expanded, expanded_identities, selected_url) == initial

    changed_selected = copy.deepcopy(expanded_identities)
    changed_selected[selected_url]['replica_record_id'] = str(uuid.uuid4())
    assert route_projection.selected_route_context_sha256(
        expanded, changed_selected, selected_url) != initial

    changed_spec = copy.deepcopy(expanded)
    changed_spec['routing_spec']['load_balancing_policy_name'] = 'round_robin'
    assert route_projection.selected_route_context_sha256(
        changed_spec, expanded_identities, selected_url) != initial


def test_builds_existing_full_response_with_private_exact_identity():
    info = _replica(1, 'http://10.0.0.1:8000')
    material = route_projection.ResolvedRouteMaterial('http://10.0.0.1:8000',
                                                      'L4', 1)

    result = _build([info], {1: material}, {1})

    assert result.response['replica_info'] == {
        material.url: {
            'gpu_type': 'L4',
            'gpu_count': '1',
            'is_zero_cost': 'false',
            'async_occupancy': 'true',
        }
    }
    assert result.response['num_ready_replicas'] == 1
    assert result.identities[material.url] == {
        'replica_id': 1,
        'replica_record_id': info.replica_record_id,
        'service_version': 1,
        'gpu_type': 'L4',
        'gpu_count': 1,
        'advertised': True,
        'alias_expires_at': None,
    }


def test_legacy_identity_is_readable_but_cannot_be_republished():
    legacy_identity = {
        'http://10.0.0.1:8000': {
            'replica_id': 1,
            'replica_record_id': str(uuid.uuid4()),
            'gpu_type': 'L4',
            'gpu_count': 1,
            'advertised': True,
            'alias_expires_at': None,
        }
    }

    assert route_projection._validate_identities(
        legacy_identity) == legacy_identity
    with pytest.raises(route_projection.RouteProjectionValidationError,
                       match='invalid shape'):
        route_projection._validate_identities(legacy_identity,
                                              require_service_version=True)


def test_unresolved_verified_ready_preserves_spurious_empty_signal():
    info = _replica(1, 'http://10.0.0.1:8000')

    result = _build([info], {}, {1})

    assert result.response['replica_info'] == {}
    assert result.response['num_ready_replicas'] == 1


def test_url_collision_fences_only_ambiguous_url():
    first = _replica(1, 'http://10.0.0.1:8000')
    second = _replica(2, 'http://10.0.0.1:8000')
    third = _replica(3, 'http://10.0.0.3:8000')
    shared = route_projection.ResolvedRouteMaterial('http://10.0.0.1:8000',
                                                    'L4', 1)
    healthy = route_projection.ResolvedRouteMaterial('http://10.0.0.3:8000',
                                                     'A100', 1)

    result = _build([first, second, third], {
        1: shared,
        2: shared,
        3: healthy,
    }, {1, 2, 3})

    assert result.response['replica_info'][shared.url] == {
        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
    }
    assert healthy.url in result.response['replica_info']
    assert shared.url not in result.identities
    assert healthy.url in result.identities


def test_recovery_capable_route_without_marker_fails_closed():
    info = _replica(1, 'http://10.0.0.1:8000', capable=True)
    material = route_projection.ResolvedRouteMaterial('http://10.0.0.1:8000',
                                                      'L4', 1)

    result = _build([info], {1: material}, {1})

    assert result.response['replica_info'][material.url] == {
        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
    }
    assert result.identities[material.url]['advertised'] is False


def test_malformed_record_identity_withholds_only_that_replica():
    malformed = _replica(1, 'http://10.0.0.1:8000')
    malformed.replica_record_id = 'legacy-malformed'
    healthy = _replica(2, 'http://10.0.0.2:8000')
    malformed_material = route_projection.ResolvedRouteMaterial(
        'http://10.0.0.1:8000', 'L4', 1)
    healthy_material = route_projection.ResolvedRouteMaterial(
        'http://10.0.0.2:8000', 'A100', 1)

    result = _build([malformed, healthy], {
        1: malformed_material,
        2: healthy_material,
    }, {1, 2})

    assert malformed_material.url not in result.response['replica_info']
    assert healthy_material.url in result.response['replica_info']
    assert result.response['num_ready_replicas'] == 1
    assert result.live_record_ids == {healthy.replica_record_id}


def _incremental_lease(replica_id,
                       record_id,
                       url,
                       now,
                       *,
                       ready=True,
                       valid=True,
                       requires_marker=False,
                       marker=None):
    material = route_projection.RouteLeaseMaterial(
        route=route_projection.ResolvedRouteMaterial(url, 'L4', 1),
        readiness_path='/health',
        probe_timeout_seconds=15,
        post_data=None,
        headers=None,
        async_occupancy=True,
        uses_logical_replicas=False,
        is_zero_cost=False,
        planned_capacity=1,
        route_allowed=True,
        requires_route_marker=requires_marker,
        route_marker=marker)
    payload = route_projection._lease_material_payload(material)
    digest = hashlib.sha256(
        json.dumps(payload,
                   sort_keys=True,
                   separators=(',', ':'),
                   allow_nan=False).encode()).hexdigest()
    return {
        'replica_id': replica_id,
        'replica_record_id': record_id,
        'service_version': 1,
        **payload,
        'material_sha256': digest,
        'ready': ready,
        'valid_until': now + datetime.timedelta(seconds=30 if valid else -1),
        'revoked_at': None,
    }


def test_incremental_view_expires_only_one_replica():
    now = datetime.datetime.now(datetime.timezone.utc)
    first = route_projection.IncrementalRouteReplica(1, str(uuid.uuid4()), 1,
                                                     'READY')
    second = route_projection.IncrementalRouteReplica(2, str(uuid.uuid4()), 1,
                                                      'READY')
    rows = [
        _incremental_lease(1, first.replica_record_id, 'http://10.0.0.1:8000',
                           now),
        _incremental_lease(2,
                           second.replica_record_id,
                           'http://10.0.0.2:8000',
                           now,
                           valid=False),
    ]

    result = route_projection.build_incremental_route_view(
        [first, second],
        rows, {1},
        now=now,
        service_version=1,
        routing_spec={'load_balancing_policy_name': 'round_robin'},
        capacity_hint={'replica_unit': 'physical_backend'})

    assert set(result.response['replica_info']) == {'http://10.0.0.1:8000'}
    assert result.response['num_ready_replicas'] == 1
    assert set(
        result.identities) == {'http://10.0.0.1:8000', 'http://10.0.0.2:8000'}
    assert result.identities['http://10.0.0.2:8000']['advertised'] is False


def test_incremental_view_projects_revoked_async_drain_observation_off_route():
    now = datetime.datetime.now(datetime.timezone.utc)
    replica = route_projection.IncrementalRouteReplica(1, str(uuid.uuid4()), 1,
                                                       'SHUTTING_DOWN')
    row = _incremental_lease(1, replica.replica_record_id,
                             'http://10.0.0.1:8000', now)
    row.update({
        'ready': False,
        'valid_until': None,
        'revoked_at': now,
    })

    result = route_projection.build_draining_route_view([{
        'replica_id': replica.replica_id,
        'replica_record_id': replica.replica_record_id,
        'version': replica.service_version,
        'status': replica.status,
    }], [row], set())

    assert result.replica_info == {
        'http://10.0.0.1:8000': {
            'gpu_type': 'L4',
            'gpu_count': '1',
            'is_zero_cost': 'false',
            'async_occupancy': 'true',
        }
    }
    assert result.identities['http://10.0.0.1:8000']['advertised'] is False


def test_draining_route_view_withholds_existing_identity_url_overlap():
    now = datetime.datetime.now(datetime.timezone.utc)
    record_id = str(uuid.uuid4())
    url = 'http://10.0.0.1:8000'
    row = _incremental_lease(1, record_id, url, now)
    row.update({'ready': False, 'valid_until': None, 'revoked_at': now})

    result = route_projection.build_draining_route_view([{
        'replica_id': 1,
        'replica_record_id': record_id,
        'version': 1,
        'status': 'SHUTTING_DOWN',
    }], [row], {url})

    assert not result.replica_info
    assert not result.identities


def test_incremental_view_requires_closed_recovery_marker():
    now = datetime.datetime.now(datetime.timezone.utc)
    replica = route_projection.IncrementalRouteReplica(1, str(uuid.uuid4()), 1,
                                                       'READY')
    row = _incremental_lease(1,
                             replica.replica_record_id,
                             'http://10.0.0.1:8000',
                             now,
                             requires_marker=True)

    result = route_projection.build_incremental_route_view(
        [replica], [row], {1},
        now=now,
        service_version=1,
        routing_spec={'load_balancing_policy_name': 'round_robin'},
        capacity_hint={})

    assert result.response['replica_info']['http://10.0.0.1:8000'] == {
        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
    }
    assert result.response['num_ready_replicas'] == 0


def test_incremental_producer_preserves_projected_protocol_one():
    assert route_projection.use_incremental_producer({
        'route_source_mode': 'LEGACY_PROXY',
        'route_projection_protocol_version': 1,
    })
    assert not route_projection.use_incremental_producer({
        'route_source_mode': 'DURABLE_PROJECTED',
        'route_projection_protocol_version': 1,
    })
    assert route_projection.use_incremental_producer({
        'route_source_mode': 'DURABLE_PROJECTED',
        'route_projection_protocol_version': 2,
    })
