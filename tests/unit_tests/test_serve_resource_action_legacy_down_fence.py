"""Identity fencing for legacy/shadow SkyServe teardown owners."""

from unittest import mock
import uuid

import pytest

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.utils import thread_utils


def _replica() -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)


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
