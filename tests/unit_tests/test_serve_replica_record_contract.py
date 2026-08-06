"""Characterization tests for SkyServe's versioned replica record."""
# pylint: disable=protected-access
import copy
import pickle
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import exceptions
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.serve import replica_info
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
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


def _protocol_v2_replica() -> replica_managers.ReplicaInfo:
    replica = _replica()
    context = 'phx-context'
    physical_uid = 'physical-uid'
    replica.location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                            region=context,
                                            zone=None,
                                            accelerators={
                                                'H200': 1
                                            },
                                            use_spot=False).to_pickleable()
    replica.resources_override = {
        'cloud': clouds.Kubernetes(),
        'region': context,
        'accelerators': {
            'H200': 1,
        },
    }
    replica.reserved_fill_pool_key = reserved_capacity_broker.make_pool_key(
        context,
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    replica.reserved_fill_service_generation = 7
    replica.reserved_fill_physical_cluster_uid = physical_uid
    replica.reserved_fill_kubernetes_context = context
    return replica


def _protocol_v2_handle(
        context: str = 'phx-context') -> backends.CloudVmRayResourceHandle:
    handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
    handle.cluster_name = 'svc-7'
    handle.launched_resources = mock.Mock(cloud=clouds.Kubernetes(),
                                          region=context)
    return handle


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


def test_legacy_pickle_migration_materializes_zero_cost_provenance():
    legacy_state = dict(vars(_replica()))
    legacy_state['_version'] = 10
    legacy_state.pop('is_zero_cost')
    restored = replica_managers.ReplicaInfo.__new__(
        replica_managers.ReplicaInfo)

    restored.__setstate__(legacy_state)

    assert vars(restored)['is_zero_cost'] is False


def test_current_record_requires_explicit_zero_cost_provenance():
    replica = _replica()
    del replica.is_zero_cost

    with pytest.raises(AttributeError, match='is_zero_cost'):
        replica.to_storage_dict()


@pytest.mark.parametrize('marker', [0, 1, 'yes'])
def test_storage_rejects_non_boolean_reserved_fill_marker(marker):
    replica = _replica()
    setattr(replica, 'reserved_fill', marker)
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='must be a boolean'):
        replica.to_storage_dict()

    state = _replica().to_storage_dict()
    state['reserved_fill'] = marker
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='must be a boolean'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_protocol_v2_storage_round_trip_preserves_strict_cleanup_authority():
    state = _protocol_v2_replica().to_storage_dict()
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    fence = reserved_capacity.parse_protocol_v2_cleanup_fence(restored)
    assert fence == reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context='phx-context', physical_cluster_uid='physical-uid')


def test_deserialized_malformed_v2_tuple_fails_closed_at_cleanup_parser():
    state = _protocol_v2_replica().to_storage_dict()
    state['resources_override']['region'] = 'replacement-context'
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='resource pin is malformed'):
        reserved_capacity.parse_protocol_v2_cleanup_fence(restored)


def test_protocol_v2_endpoint_resolution_enters_exact_physical_fence():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle()
    cluster_record = {'name': 'svc-7', 'handle': handle}
    uid_fence = mock.MagicMock()
    uid_fence.return_value.__enter__.return_value = None

    with mock.patch.object(kubernetes_adaptor,
                           'physical_cluster_uid_fence', uid_fence), \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints',
                           return_value={8080: '10.0.0.1:8080'}) as endpoint:
        assert replica._resolve_url(  # pylint: disable=protected-access
            cluster_record=cluster_record,
            handle=handle) == ('http://10.0.0.1:8080')

    uid_fence.assert_called_once_with('phx-context', 'physical-uid')
    endpoint.assert_called_once_with('svc-7',
                                     8080,
                                     cluster_record=cluster_record)


def test_protocol_v2_endpoint_uid_mismatch_precedes_provider_call():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle()
    cluster_record = {'name': 'svc-7', 'handle': handle}
    uid_fence = mock.MagicMock()
    uid_fence.return_value.__enter__.side_effect = (
        exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

    with mock.patch.object(kubernetes_adaptor,
                           'physical_cluster_uid_fence', uid_fence), \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints') as endpoint, \
         pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='UID mismatch'):
        replica._resolve_url(  # pylint: disable=protected-access
            cluster_record=cluster_record,
            handle=handle)

    endpoint.assert_not_called()


def test_protocol_v2_endpoint_rejects_retargeted_handle_before_uid_lookup():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle(context='replacement-context')
    cluster_record = {'name': 'svc-7', 'handle': handle}

    with mock.patch.object(
            kubernetes_adaptor,
            'physical_cluster_uid_fence') as uid_fence, \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints') as endpoint, \
         pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='durable replica handle'):
        replica._resolve_url(  # pylint: disable=protected-access
            cluster_record=cluster_record,
            handle=handle)

    uid_fence.assert_not_called()
    endpoint.assert_not_called()


def test_pool_probe_propagates_explicit_provider_phase_admission():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle()
    backend = mock.Mock()
    backend.get_job_status.return_value = {
        1: replica_info.job_lib.JobStatus.SUCCEEDED
    }
    provider_fence = mock.MagicMock()
    provider_fence.return_value.__enter__.return_value = None
    admission = mock.sentinel.provider_phase_admission

    with mock.patch.object(replica_info.global_user_state,
                           'get_handle_from_cluster_name',
                           return_value=handle), \
         mock.patch.object(replica_info.backend_utils,
                           'check_cluster_available',
                           return_value=handle), \
         mock.patch.object(replica_info.backend_utils,
                           'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(reserved_capacity,
                           'protocol_v2_provider_fence',
                           provider_fence):
        _, ready, _ = replica.probe_pool(provider_phase_admission=admission)

    assert ready
    assert provider_fence.call_args_list == [
        mock.call(replica, handle, phase_admission=admission),
        mock.call(replica, handle, phase_admission=admission),
    ]


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
