"""Identity fencing for legacy/shadow SkyServe teardown owners."""
# pylint: disable=protected-access

import contextlib
from unittest import mock
import uuid

import pytest

from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.utils import common_utils
from sky.utils import thread_utils


def _replica() -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)


def _protocol_v2_replica() -> replica_managers.ReplicaInfo:
    info = _replica()
    info.reserved_fill = True
    info.reserved_fill_pool_key = reserved_capacity_broker.make_pool_key(
        'phx-context',
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='physical-a')
    info.reserved_fill_service_generation = 1
    info.reserved_fill_physical_cluster_uid = 'physical-a'
    info.reserved_fill_kubernetes_context = 'phx-context'
    info.location = {
        'cloud': 'Kubernetes',
        'region': 'phx-context',
        'accelerators': {
            'H200': 1,
        },
    }
    info.resources_override = {
        'cloud': 'Kubernetes',
        'region': 'phx-context',
        'accelerators': {
            'H200': 1,
        },
    }
    return info


def _manager_for_down_test():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._resource_action_mode = 'shadow'
    manager._resource_scope = None
    manager._is_pool = False
    manager._launch_thread_pool = thread_utils.ThreadSafeDict()
    manager._down_thread_pool = thread_utils.ThreadSafeDict()
    manager._persist_replica = mock.Mock()
    manager._schedule_failed_cleanup_retry = mock.Mock()
    return manager


def _protocol_v2_cluster_record():
    handle = mock.Mock(
        spec=replica_managers.cloud_vm_ray_backend.CloudVmRayResourceHandle)
    handle.cluster_name = 'svc-1'
    handle.launched_resources = mock.Mock(
        cloud=replica_managers.clouds.Kubernetes(), region='phx-context')
    return {
        'workspace': 'workspace-a',
        'handle': handle,
        'cluster_hash': 'generation-a',
    }


def test_terminate_cluster_forwards_exact_resource_action_uuid(tmp_path):
    expected_uuid = '33333333-3333-4333-8333-333333333333'
    with mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name',
                           return_value={
                               'workspace': 'workspace-a'
                           }), \
         mock.patch.object(replica_managers.skypilot_config,
                           'local_active_workspace_ctx') as workspace_ctx, \
         mock.patch('sky.core.down') as down, \
         mock.patch.object(replica_managers.time, 'sleep'):
        workspace_ctx.return_value.__enter__.return_value = None
        replica_managers.terminate_cluster(
            'svc-1',
            str(tmp_path / 'down.log'),
            expected_cluster_record_uuid=expected_uuid)

    down.assert_called_once_with('svc-1',
                                 _expected_cluster_record_uuid=expected_uuid)


def test_terminate_cluster_does_not_retry_identity_conflict(tmp_path):
    conflict = replica_managers.global_user_state.ClusterRecordIdentityConflictError(
        'replacement row')
    with mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name',
                           return_value={}), \
         mock.patch('sky.core.down', side_effect=conflict) as down, \
         mock.patch.object(replica_managers.time, 'sleep'), \
         pytest.raises(
             replica_managers.global_user_state.
             ClusterRecordIdentityConflictError):
        replica_managers.terminate_cluster(
            'svc-1',
            str(tmp_path / 'down.log'),
            expected_cluster_record_uuid=(
                '33333333-3333-4333-8333-333333333333'))

    down.assert_called_once()


def test_protocol_v2_legacy_cleanup_forwards_hash_and_owner_guard(tmp_path):
    cleanup_fence = replica_managers.reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context='phx-context', physical_cluster_uid='physical-a')
    guard = mock.Mock(return_value=True)
    with mock.patch.object(replica_managers.context,
                           'get',
                           return_value=mock.MagicMock()), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name',
                           return_value=_protocol_v2_cluster_record()), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence',
                           return_value=contextlib.nullcontext()), \
         mock.patch('sky.core.down') as down, \
         mock.patch.object(replica_managers.time, 'sleep'):
        replica_managers.terminate_cluster.__wrapped__(
            'svc-1',
            str(tmp_path / 'down.log'),
            cleanup_fence=cleanup_fence,
            continue_guard=guard)

    down.assert_called_once_with('svc-1',
                                 _expected_cluster_hash='generation-a',
                                 _continue_guard=guard)


def test_protocol_v2_action_cleanup_keeps_uuid_authoritative(tmp_path):
    cleanup_fence = replica_managers.reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context='phx-context', physical_cluster_uid='physical-a')
    expected_uuid = '33333333-3333-4333-8333-333333333333'
    guard = mock.Mock(return_value=True)
    with mock.patch.object(replica_managers.context,
                           'get',
                           return_value=mock.MagicMock()), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name',
                           return_value=_protocol_v2_cluster_record()), \
         mock.patch.object(
             replica_managers.global_user_state,
             'get_cluster_record_identity_snapshot',
             return_value=mock.Mock()), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence',
                           return_value=contextlib.nullcontext()), \
         mock.patch('sky.core.down') as down, \
         mock.patch.object(replica_managers.time, 'sleep'):
        replica_managers.terminate_cluster.__wrapped__(
            'svc-1',
            str(tmp_path / 'down.log'),
            cleanup_fence=cleanup_fence,
            expected_cluster_record_uuid=expected_uuid,
            continue_guard=guard)

    down.assert_called_once_with('svc-1',
                                 _expected_cluster_record_uuid=expected_uuid,
                                 _continue_guard=guard)


def test_protocol_v2_cleanup_reacquires_phase_workspace_and_uid_per_retry(
        tmp_path):
    cleanup_fence = replica_managers.reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context='phx-context', physical_cluster_uid='physical-a')
    events = []

    @contextlib.contextmanager
    def phase(mode):
        events.append(('phase-enter', mode))
        try:
            yield mock.sentinel.admission
        finally:
            events.append(('phase-exit', mode))

    @contextlib.contextmanager
    def workspace(name):
        events.append(('workspace-enter', name))
        try:
            yield
        finally:
            events.append(('workspace-exit', name))

    @contextlib.contextmanager
    def physical_fence(_context, _uid):
        events.append(('fence-enter', None))
        try:
            yield
        finally:
            events.append(('fence-exit', None))

    def down(*_args, **kwargs):
        events.append(('down', kwargs.get('_expected_cluster_hash')))
        if sum(event[0] == 'down' for event in events) == 1:
            raise RuntimeError('transient provider failure')

    first_record = _protocol_v2_cluster_record()
    second_record = _protocol_v2_cluster_record()
    second_record['workspace'] = 'workspace-b'
    second_record['cluster_hash'] = 'generation-b'
    records = iter((first_record, second_record))

    def read_record(_cluster_name):
        record = next(records)
        events.append(('record-read', record['workspace']))
        return record

    with mock.patch.object(replica_managers.context,
                           'get',
                           return_value=mock.MagicMock()), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name',
                           side_effect=read_record) as get_record, \
         mock.patch.object(replica_managers.provider_phase,
                           'provider_phase',
                           side_effect=phase), \
         mock.patch.object(replica_managers.skypilot_config,
                           'local_active_workspace_ctx',
                           side_effect=workspace), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence',
                           side_effect=physical_fence), \
         mock.patch('sky.core.down', side_effect=down), \
         mock.patch.object(replica_managers.common_utils.Backoff,
                           'current_backoff',
                           return_value=0), \
         mock.patch.object(replica_managers.time, 'sleep'):
        replica_managers.terminate_cluster.__wrapped__(
            'svc-1',
            str(tmp_path / 'down.log'),
            cleanup_fence=cleanup_fence,
            max_retry=2)

    mode = replica_managers.provider_phase.ProviderPhaseMode.V2_FENCED
    assert events == [('phase-enter', mode), ('record-read', 'workspace-a'),
                      ('workspace-enter', 'workspace-a'), ('fence-enter', None),
                      ('down', 'generation-a'), ('fence-exit', None),
                      ('workspace-exit', 'workspace-a'), ('phase-exit', mode),
                      ('phase-enter', mode), ('record-read', 'workspace-b'),
                      ('workspace-enter', 'workspace-b'), ('fence-enter', None),
                      ('down', 'generation-b'), ('fence-exit', None),
                      ('workspace-exit', 'workspace-b'), ('phase-exit', mode)]
    assert get_record.call_count == 2


def test_protocol_v2_rotated_uuid_rejects_before_provider_capture(tmp_path):
    cleanup_fence = replica_managers.reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context='phx-context', physical_cluster_uid='physical-a')
    expected_uuid = '33333333-3333-4333-8333-333333333333'
    with mock.patch.object(replica_managers.context,
                           'get',
                           return_value=mock.MagicMock()), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name',
                           return_value=_protocol_v2_cluster_record()), \
         mock.patch.object(
             replica_managers.global_user_state,
             'get_cluster_record_identity_snapshot',
             side_effect=(replica_managers.global_user_state.
                          ClusterRecordIdentityConflictError('rotated'))), \
         mock.patch.object(
             replica_managers.kubernetes_adaptor,
             'physical_cluster_uid_fence') as provider_fence, \
         mock.patch('sky.core.down') as down, \
         mock.patch.object(replica_managers.time, 'sleep'), \
         pytest.raises(
             replica_managers.global_user_state.
             ClusterRecordIdentityConflictError,
             match='rotated'):
        replica_managers.terminate_cluster.__wrapped__(
            'svc-1',
            str(tmp_path / 'down.log'),
            cleanup_fence=cleanup_fence,
            expected_cluster_record_uuid=expected_uuid)

    provider_fence.assert_not_called()
    down.assert_not_called()


def test_action_aware_replica_worker_carries_persisted_uuid(tmp_path):
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._resource_action_mode = 'shadow'
    manager._resource_scope = None
    manager._is_pool = False
    manager._launch_thread_pool = thread_utils.ThreadSafeDict()
    manager._down_thread_pool = thread_utils.ThreadSafeDict()
    info = _replica()
    expected_uuid = uuid.UUID('33333333-3333-4333-8333-333333333333')
    identity = serve_state.ReplicaResourceActionIdentity(
        replica_id=1,
        cluster_name='svc-1',
        replica_incarnation=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        desired_generation=1,
        sky_cluster_record_uuid=expected_uuid)
    manager._persist_replica = mock.Mock()
    down_thread = mock.Mock()

    with mock.patch.object(
            replica_managers.serve_state,
            'get_replica_info_with_resource_action_identity',
            return_value=(info, identity)), \
         mock.patch.object(replica_managers.serve_utils,
                           'generate_replica_log_file_name',
                           return_value=str(tmp_path / 'replica.log')), \
         mock.patch.object(replica_managers.global_user_state,
                           'cluster_with_name_exists',
                           return_value=True), \
         mock.patch.object(replica_managers.thread_utils,
                           'SafeThread',
                           return_value=down_thread) as safe_thread:
        manager._terminate_replica(1,
                                   sync_down_logs=False,
                                   replica_drain_delay_seconds=0,
                                   is_scale_down=True)

    assert manager._down_thread_pool[1] is down_thread
    assert safe_thread.call_args.kwargs['target'] is (
        replica_managers.terminate_cluster)
    assert safe_thread.call_args.kwargs['kwargs'][
        'expected_cluster_record_uuid'] == str(expected_uuid)


def test_legacy_replica_worker_remains_name_only(tmp_path):
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._resource_action_mode = 'legacy'
    manager._resource_scope = None
    manager._is_pool = False
    manager._launch_thread_pool = thread_utils.ThreadSafeDict()
    manager._down_thread_pool = thread_utils.ThreadSafeDict()
    info = _replica()
    manager._persist_replica = mock.Mock()

    with mock.patch.object(
            replica_managers.serve_state,
            'get_replica_info_with_resource_action_identity',
            return_value=(info, None)), \
         mock.patch.object(replica_managers.serve_utils,
                           'generate_replica_log_file_name',
                           return_value=str(tmp_path / 'replica.log')), \
         mock.patch.object(replica_managers.global_user_state,
                           'cluster_with_name_exists',
                           return_value=True), \
         mock.patch.object(replica_managers.thread_utils,
                           'SafeThread',
                           return_value=mock.Mock()) as safe_thread:
        manager._terminate_replica(1,
                                   sync_down_logs=False,
                                   replica_drain_delay_seconds=0,
                                   is_scale_down=True)

    assert safe_thread.call_args.kwargs['kwargs'][
        'expected_cluster_record_uuid'] is None


def test_protocol_v2_absent_cluster_record_is_retained_for_retry(tmp_path):
    manager = _manager_for_down_test()
    info = _protocol_v2_replica()

    with mock.patch.object(
            replica_managers.serve_state,
            'get_replica_info_with_resource_action_identity',
            return_value=(info, None)), \
         mock.patch.object(replica_managers.serve_utils,
                           'generate_replica_log_file_name',
                           return_value=str(tmp_path / 'replica.log')), \
         mock.patch.object(replica_managers.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False):
        manager._terminate_replica(1,
                                   sync_down_logs=False,
                                   replica_drain_delay_seconds=0,
                                   is_scale_down=True)

    assert info.status_property.sky_down_status == common_utils.ProcessStatus.FAILED
    assert info.status_property.sky_launch_status == common_utils.ProcessStatus.FAILED
    assert info.status == serve_state.ReplicaStatus.FAILED_CLEANUP
    manager._persist_replica.assert_called_once_with(1, info)
    manager._schedule_failed_cleanup_retry.assert_called_once_with(1)
    assert 1 not in manager._down_thread_pool


def test_malformed_protocol_v2_cleanup_authority_is_retained(tmp_path):
    manager = _manager_for_down_test()
    info = _protocol_v2_replica()
    info.reserved_fill_kubernetes_context = 'retargeted-context'

    with mock.patch.object(
            replica_managers.serve_state,
            'get_replica_info_with_resource_action_identity',
            return_value=(info, None)), \
         mock.patch.object(replica_managers.serve_utils,
                           'generate_replica_log_file_name',
                           return_value=str(tmp_path / 'replica.log')), \
         mock.patch.object(replica_managers.global_user_state,
                           'cluster_with_name_exists') as cluster_exists:
        manager._terminate_replica(1,
                                   sync_down_logs=False,
                                   replica_drain_delay_seconds=0,
                                   is_scale_down=True)

    cluster_exists.assert_not_called()
    assert info.status_property.sky_down_status == common_utils.ProcessStatus.FAILED
    assert info.status_property.sky_launch_status == common_utils.ProcessStatus.FAILED
    assert info.status == serve_state.ReplicaStatus.FAILED_CLEANUP
    manager._persist_replica.assert_called_once_with(1, info)
    manager._schedule_failed_cleanup_retry.assert_called_once_with(1)
    assert 1 not in manager._down_thread_pool
