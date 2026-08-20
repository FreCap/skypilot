"""Execution-boundary tests for durable ordinary Serve launches."""
# pylint: disable=protected-access

import contextlib
import types
from unittest import mock
import uuid

import pytest

from sky import exceptions
from sky import execution
from sky.backends import cloud_vm_ray_backend
from sky.serve import constants as serve_constants
from sky.serve import ordinary_launch_binding
from sky.server.requests import request_names
from sky.utils import common


class _Dag:
    """Minimal one-task DAG accepted by the execution helper."""

    def __init__(self, task):
        self.tasks = [task]

    def __len__(self):
        return len(self.tasks)


def _task():
    resource = types.SimpleNamespace(cloud=None,
                                     region=None,
                                     accelerators=None,
                                     job_recovery=None,
                                     autostop_config=None,
                                     hooks=None)
    return types.SimpleNamespace(
        resources=[resource],
        best_resources=resource,
        service=types.SimpleNamespace(pool=False),
        use_spot=False,
        storage_mounts=None,
        sync_storage_mounts=lambda: None,
        workdir=None,
        file_mounts=None,
        get_required_cloud_features=set,
    )


def test_bound_effect_guard_covers_provider_and_service_job_boundaries():
    events: list[str] = []
    task = _task()
    task.storage_mounts = {
        'data': types.SimpleNamespace(
            construct=lambda: events.append('storage_construct'))
    }
    handle = mock.MagicMock()
    handle.get_cluster_name.return_value = 'svc-1'
    backend = mock.MagicMock()
    backend.register_info.side_effect = lambda **_kwargs: events.append(
        'register')
    backend.provision.side_effect = lambda *_args, **_kwargs: (events.append(
        'provision') or (handle, False))
    backend.execute.side_effect = lambda *_args, **_kwargs: (events.append(
        'execute') or 17)
    backend.post_execute.side_effect = lambda *_args, **_kwargs: events.append(
        'post_execute')
    launch_context = {
        ordinary_launch_binding.ASSOCIATION_ID_KEY: str(uuid.uuid4())
    }

    @contextlib.contextmanager
    def _effect_guard(context):
        assert context is launch_context
        events.append('guard_enter')
        try:
            yield
        finally:
            events.append('guard_exit')

    def _begin(context):
        assert context is launch_context
        events.append('service_job_begin')

    def _record(context, job_id):
        assert context is launch_context
        assert job_id == 17
        events.append('service_job_recorded')

    with mock.patch.object(
            execution.global_user_state,
            'cluster_with_name_exists',
            return_value=False), mock.patch.object(
                execution.global_user_state, 'update_last_use'), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
        mock.patch.object(execution.ordinary_launch_request,
                           '_provider_effect_guard',
                           side_effect=_effect_guard), \
         mock.patch.object(execution.ordinary_launch_request,
                           '_begin_service_job_io',
                           side_effect=_begin), \
         mock.patch.object(execution.ordinary_launch_request,
                           '_record_service_job',
                           side_effect=_record):
        job_id, result_handle = execution._execute_dag(
            _Dag(task),
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.PROVISION, execution.Stage.EXEC],
            cluster_name='svc-1',
            detach_setup=False,
            no_setup=True,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=launch_context)

    assert job_id == 17
    assert result_handle is handle
    assert events == [
        'guard_enter',
        'storage_construct',
        'register',
        'provision',
        'service_job_begin',
        'execute',
        'service_job_recorded',
        'post_execute',
        'guard_exit',
    ]


def test_bound_storage_failure_is_inside_effect_guard():
    events: list[str] = []
    task = _task()

    def _construct_then_fail():
        events.append('storage_construct')
        raise RuntimeError('storage write interrupted')

    task.storage_mounts = {
        'data': types.SimpleNamespace(construct=_construct_then_fail)
    }
    backend = mock.MagicMock()
    launch_context = {
        ordinary_launch_binding.ASSOCIATION_ID_KEY: str(uuid.uuid4())
    }

    @contextlib.contextmanager
    def _effect_guard(context):
        assert context is launch_context
        events.append('guard_enter')
        try:
            yield
        finally:
            events.append('guard_exit')

    with mock.patch.object(
            execution.global_user_state,
            'cluster_with_name_exists',
            return_value=False), mock.patch.object(
                execution.container_image_consumers,
                'derive',
                return_value=mock.sentinel.image_consumer), mock.patch.object(
                    execution.ordinary_launch_request,
                    '_provider_effect_guard',
                    side_effect=_effect_guard):
        with pytest.raises(RuntimeError, match='storage write interrupted'):
            execution._execute_dag(_Dag(task),
                                   dryrun=False,
                                   stream_logs=False,
                                   handle=None,
                                   backend=backend,
                                   retry_until_up=False,
                                   optimize_target=common.OptimizeTarget.COST,
                                   stages=[execution.Stage.PROVISION],
                                   cluster_name='svc-1',
                                   detach_setup=False,
                                   no_setup=True,
                                   clone_disk_from=None,
                                   skip_unnecessary_provisioning=False,
                                   _quiet_optimizer=False,
                                   _is_launched_by_jobs_controller=False,
                                   _is_launched_by_sky_serve_controller=True,
                                   _extra_launch_context=launch_context)

    assert events == ['guard_enter', 'storage_construct', 'guard_exit']
    backend.register_info.assert_not_called()


def _bound_context() -> dict[str, object]:
    return {
        ordinary_launch_binding.ASSOCIATION_ID_KEY: str(uuid.uuid4()),
        ordinary_launch_binding.LAUNCH_GENERATION_KEY: 1,
        ordinary_launch_binding.BOUND_REQUEST_ID_KEY: str(uuid.uuid4()),
        ordinary_launch_binding.INPUT_DIGEST_KEY: 'a' * 64,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        ordinary_launch_binding.REPLICA_ID_KEY: 1,
        ordinary_launch_binding.REPLICA_RECORD_ID_KEY: str(uuid.uuid4()),
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY:
            ordinary_launch_binding.LEGACY_FAIL_CLOSED_CONTROLLER_PID,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY:
            ordinary_launch_binding.LEGACY_FAIL_CLOSED_CONTROLLER_IP,
    }


def test_bound_execute_bypasses_only_legacy_pid_ip_fence():
    context = _bound_context()
    resource = mock.MagicMock(autostop_config=None)
    storage_mount = mock.Mock()
    task = mock.MagicMock(resources=[resource],
                          storage_mounts={'data': storage_mount},
                          managed_secret_refs={})
    dag = mock.MagicMock(tasks=[task])
    expected = (mock.sentinel.job_id, mock.sentinel.handle)

    with mock.patch.object(execution.dag_utils,
                           'convert_entrypoint_to_dag',
                           return_value=dag), mock.patch.object(
                               execution.admin_policy_utils,
                               'apply_and_use_config_in_current_request',
                               return_value=contextlib.nullcontext(dag)), \
         mock.patch.object(execution, '_resolve_managed_secrets'), \
         mock.patch.object(execution,
                           '_validate_service_replica_launch_fence') as legacy, \
         mock.patch.object(execution, '_execute_dag', return_value=expected):
        result = execution._execute(
            mock.sentinel.entrypoint,
            _request_name=(request_names.AdminPolicyRequestName.CLUSTER_LAUNCH),
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=context)

    assert result == expected
    legacy.assert_not_called()
    storage_mount.construct.assert_not_called()


def test_excluded_profile_uses_production_legacy_fence_entrypoint():
    context = {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'service-hash',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 2,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 1,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(uuid.uuid4()),
    }
    resource = mock.MagicMock(autostop_config=None)
    task = mock.MagicMock(resources=[resource],
                          storage_mounts=None,
                          managed_secret_refs={})
    dag = mock.MagicMock(tasks=[task])
    expected = (mock.sentinel.job_id, mock.sentinel.handle)

    with mock.patch.object(execution.dag_utils,
                           'convert_entrypoint_to_dag',
                           return_value=dag), mock.patch.object(
                               execution.admin_policy_utils,
                               'apply_and_use_config_in_current_request',
                               return_value=contextlib.nullcontext(dag)), \
         mock.patch.object(execution, '_resolve_managed_secrets'), \
         mock.patch.object(
             execution.serve_state,
             'get_placement_projection_record',
             return_value=(True, None, None, None)), \
         mock.patch.object(
             execution, '_validate_service_replica_launch_fence') as legacy, \
         mock.patch.object(execution, '_execute_dag', return_value=expected):
        result = execution._execute(
            mock.sentinel.entrypoint,
            _request_name=(request_names.AdminPolicyRequestName.CLUSTER_LAUNCH),
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=context)

    assert result == expected
    legacy.assert_called_once_with(context)


def test_partial_bound_context_fails_before_legacy_fence():
    context = _bound_context()
    context.pop(ordinary_launch_binding.BOUND_REQUEST_ID_KEY)

    with mock.patch.object(execution,
                           '_validate_service_replica_launch_fence') as legacy:
        with pytest.raises(ValueError, match='request_id'):
            execution._execute(
                mock.sentinel.entrypoint,
                _request_name=(
                    request_names.AdminPolicyRequestName.CLUSTER_LAUNCH),
                _is_launched_by_sky_serve_controller=True,
                _extra_launch_context=context)
    legacy.assert_not_called()


def _retrying_provisioner(
        context: dict[str,
                      object]) -> cloud_vm_ray_backend.RetryingVmProvisioner:
    provisioner = cloud_vm_ray_backend.RetryingVmProvisioner.__new__(
        cloud_vm_ray_backend.RetryingVmProvisioner)
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = context
    return provisioner


def test_backend_bound_provider_guard_requires_active_association():
    context = _bound_context()
    provisioner = _retrying_provisioner(context)

    with mock.patch.object(
            cloud_vm_ray_backend.ordinary_launch_binding,
            'require_active_provider_effect_authorization') as active, \
         mock.patch.object(
             cloud_vm_ray_backend.serve_state,
             'service_replica_launch_authority_guard') as legacy_guard:
        with provisioner._service_replica_launch_provider_guard():
            pass

    assert active.call_args_list == [mock.call(context), mock.call(context)]
    legacy_guard.assert_not_called()


def test_backend_bound_provider_guard_carries_exact_replica_snapshot():
    context = _bound_context()
    provisioner = _retrying_provisioner(context)
    durable_replica = mock.sentinel.durable_replica
    authorization = types.SimpleNamespace(durable_replica_info=durable_replica)

    with mock.patch.object(cloud_vm_ray_backend.ordinary_launch_binding,
                           'require_active_provider_effect_authorization',
                           return_value=authorization) as active:
        with provisioner._service_replica_launch_provider_owner_guard(
        ) as snapshot:
            assert snapshot is not None
            assert snapshot.durable_replica_info is durable_replica

    assert active.call_args_list == [mock.call(context), mock.call(context)]


def test_backend_bound_standalone_validation_uses_active_association():
    context = _bound_context()
    provisioner = _retrying_provisioner(context)

    with mock.patch.object(
            cloud_vm_ray_backend.ordinary_launch_binding,
            'require_active_provider_effect_authorization') as active, \
         mock.patch.object(
             cloud_vm_ray_backend.serve_state,
             'service_replica_launch_fence_holds') as legacy_fence:
        provisioner._validate_service_replica_launch_fence()

    active.assert_called_once_with(context)
    legacy_fence.assert_not_called()


def test_backend_bound_provider_guard_fails_closed_without_active_association():
    context = _bound_context()
    provisioner = _retrying_provisioner(context)

    with mock.patch.object(
            cloud_vm_ray_backend.ordinary_launch_binding,
            'require_active_provider_effect_authorization',
            side_effect=(ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'missing active guard'))):
        with pytest.raises(exceptions.ServeReplicaLaunchFenceError,
                           match='no exact active association'):
            with provisioner._service_replica_launch_provider_guard():
                pytest.fail('provider body must not run')


def _system_recovery_excluded_context(request_id: str) -> dict[str, object]:
    return {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'service-hash',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 2,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.
            ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 1,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY: request_id,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY: 3,
    }


def test_backend_system_exclusion_requires_active_request_identity():
    context = _system_recovery_excluded_context('bound-request')
    provisioner = _retrying_provisioner(context)

    with mock.patch.object(
            cloud_vm_ray_backend.request_storage,
            'active_execution_claim',
            return_value=types.SimpleNamespace(request_id='other-request')), \
         mock.patch.object(cloud_vm_ray_backend.serve_state,
                           'service_replica_launch_fence_holds') as fence:
        with pytest.raises(exceptions.ServeReplicaLaunchFenceError,
                           match='active request execution claim'):
            provisioner._validate_service_replica_launch_fence()
    fence.assert_not_called()


def test_backend_system_exclusion_accepts_exact_active_request_identity():
    context = _system_recovery_excluded_context('bound-request')
    provisioner = _retrying_provisioner(context)

    with mock.patch.object(
            cloud_vm_ray_backend.request_storage,
            'active_execution_claim',
            return_value=types.SimpleNamespace(request_id='bound-request')), \
         mock.patch.object(cloud_vm_ray_backend.serve_state,
                           'service_replica_launch_fence_holds',
                           return_value=True) as fence:
        provisioner._validate_service_replica_launch_fence()
    fence.assert_called_once_with(context)
