"""Characterization tests for SkyServe's versioned replica record."""
# pylint: disable=protected-access
import copy
import pickle
from unittest import mock

import pytest

from sky import clouds
from sky.serve import replica_info
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.utils import common_utils


def _replica() -> replica_managers.ReplicaInfo:
    location = spot_placer.Location(cloud=clouds.AWS(),
                                    region='us-east-1',
                                    zone='us-east-1a')
    replica = replica_managers.ReplicaInfo(
        replica_id=7,
        cluster_name='svc-7',
        replica_port='8080',
        is_spot=True,
        location=location,
        version=3,
        resources_override={
            'cloud': clouds.AWS(),
            'region': 'us-east-1',
            'image_id': {
                None: 'global-image',
                'us-east-1': 'regional-image',
            },
        },
        planned_capacity=4,
        unknown_capacity_replacement=True)
    replica.created_at = 100.0
    replica.first_not_ready_time = 200.0
    replica.first_consecutive_failure_time = 210.0
    replica.logical_bridge_capacity_verified = True
    replica.reserved_fill = True
    replica.is_zero_cost = True
    replica.cost_rebalance_for_replica_id = 2
    replica.paid_capacity_pool_key = 'aws|us-east-1|a100'
    replica.status_property = replica_managers.ReplicaStatusProperty(
        sky_launch_status=common_utils.ProcessStatus.SUCCEEDED,
        service_ready_now=True,
        first_ready_time=150.0,
        drain_cap_seconds=120,
        drain_started_at=300.0,
        wait_for_idle_before_termination=True,
        logical_retirement_version=3,
        logical_retirement_controller_epoch='owner-a',
        logical_retirement_generation=11,
        logical_retirement_target_capacity=8,
        logical_retirement_confirmed_generation=10,
        logical_retirement_bounded_deadline=True,
        logical_retirement_committed=True)
    return replica


@pytest.mark.parametrize(('updates', 'expected'), [
    ({}, serve_state.ReplicaStatus.PENDING),
    ({
        'sky_launch_status': common_utils.ProcessStatus.RUNNING
    }, serve_state.ReplicaStatus.PROVISIONING),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'service_ready_now': True,
        'first_ready_time': 10.0,
    }, serve_state.ReplicaStatus.READY),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'first_ready_time': 10.0,
    }, serve_state.ReplicaStatus.NOT_READY),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'sky_down_status': common_utils.ProcessStatus.RUNNING,
    }, serve_state.ReplicaStatus.SHUTTING_DOWN),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'sky_down_status': common_utils.ProcessStatus.FAILED,
    }, serve_state.ReplicaStatus.FAILED_CLEANUP),
])
def test_status_projection_contract(updates, expected):
    status = replica_managers.ReplicaStatusProperty()
    for name, value in updates.items():
        setattr(status, name, value)
    assert status.to_replica_status() is expected


def test_storage_round_trip_is_lossless_and_does_not_mutate_source():
    replica = _replica()
    assert replica.resources_override is not None
    cloud_before = replica.resources_override['cloud']
    image_id_before = copy.deepcopy(replica.resources_override['image_id'])

    state = replica.to_storage_dict()
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert replica.resources_override['cloud'] is cloud_before
    assert replica.resources_override['image_id'] == image_id_before
    assert restored.to_storage_dict() == state
    assert restored.location == replica.location
    assert restored.resources_override['image_id'] == {
        None: 'global-image',
        'us-east-1': 'regional-image',
    }
    assert restored.status is serve_state.ReplicaStatus.READY


def test_legacy_null_image_key_and_missing_fields_remain_compatible():
    state = _replica().to_storage_dict()
    state['resources_override']['image_id'] = {
        'null': 'global-image',
        'us-east-1': 'regional-image',
    }
    state.pop('planned_capacity')
    state.pop('logical_bridge_capacity_verified')
    state['status_property'].pop('logical_retirement_committed')

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert restored.resources_override['image_id'] == {
        None: 'global-image',
        'us-east-1': 'regional-image',
    }
    assert restored.planned_capacity == 1
    assert restored.logical_bridge_capacity_verified is False
    assert restored.status_property.logical_retirement_committed is None


def test_public_class_and_pickle_identity_remain_stable():
    assert replica_managers.ReplicaInfo is replica_info.ReplicaInfo
    assert (replica_managers.ReplicaStatusProperty
            is replica_info.ReplicaStatusProperty)
    assert replica_managers._NOT_PROVIDED is replica_info._NOT_PROVIDED
    assert replica_managers.ReplicaInfo.__module__ == (
        'sky.serve.replica_managers')
    assert replica_managers.ReplicaStatusProperty.__module__ == (
        'sky.serve.replica_managers')

    replica = _replica()
    restored = pickle.loads(pickle.dumps(replica, protocol=5))
    assert type(restored) is replica_managers.ReplicaInfo
    assert type(
        restored.status_property) is (replica_managers.ReplicaStatusProperty)
    assert restored.to_storage_dict() == replica.to_storage_dict()


def test_info_projection_reuses_one_cluster_record_and_endpoint_lookup():
    replica = _replica()
    handle = mock.MagicMock()
    handle.launched_resources.cloud = clouds.AWS()
    handle.launched_resources.region = 'us-east-1'
    handle.launched_resources.infra.formatted_str.return_value = (
        'aws (us-east-1)')
    handle.launched_nodes = 1
    cluster_record = {'handle': handle, 'launched_at': 90.0}

    with mock.patch.object(replica, 'handle',
                           return_value=handle) as handle_read, \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name') as cluster_read, \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints',
                           return_value={8080: '1.2.3.4:8080'}) as endpoint, \
         mock.patch.object(
             replica_managers.resources_utils,
             'get_readable_resources_repr',
             return_value=('1x A100', '1x A100 (full)')) as resource_repr, \
         mock.patch.object(replica_managers.estimated_spend,
                           'estimate_hourly_cost',
                           return_value=(2.5, None)) as estimate:
        result = replica.to_info_dict(with_handle=False,
                                      cluster_record=cluster_record)

    cluster_read.assert_not_called()
    handle_read.assert_called_once_with(cluster_record)
    endpoint.assert_called_once_with('svc-7',
                                     8080,
                                     cluster_record=cluster_record)
    resource_repr.assert_called_once_with(handle, simplified_only=False)
    estimate.assert_called_once_with(handle.launched_resources, 1, None)
    assert result['endpoint'] == 'http://1.2.3.4:8080'
    assert result['hourly_cost'] == 2.5
    assert result['resources_str_full'] == '1x A100 (full)'
    assert result['time_to_ready_seconds'] == 50.0


def test_probe_contains_input_and_transport_failures():
    replica = _replica()
    client = mock.Mock()
    client.get.side_effect = ValueError('invalid user header')

    with mock.patch.object(replica_managers.replica_tls,
                           'probe_client',
                           return_value=client):
        actual, ready, probe_time = replica.probe(
            readiness_path='/health',
            post_data=None,
            timeout=7,
            headers={'X-User': 'value'},
            resolved_url='https://replica.example')

    assert actual is replica
    assert ready is False
    assert isinstance(probe_time, float)
    client.get.assert_called_once_with('https://replica.example/health',
                                       headers={'X-User': 'value'},
                                       timeout=7)


def test_probe_reports_exact_start_immediately_before_transport_call():
    replica = _replica()
    events = []
    response = mock.Mock(status_code=200)

    def _get(*_args, **_kwargs):
        events.append(('request', None))
        return response

    client = mock.Mock()
    client.get.side_effect = _get

    with mock.patch.object(replica_managers.replica_tls,
                           'probe_client',
                           return_value=client):
        _, ready, _ = replica.probe(
            readiness_path='/health',
            post_data=None,
            timeout=7,
            headers=None,
            resolved_url='https://replica.example',
            request_started_callback=lambda started_at: events.append(
                ('start', started_at)))

    assert ready
    assert [event for event, _ in events] == ['start', 'request']
    assert isinstance(events[0][1], float)
