"""Executor-side tests for durable reserved-fill placement fencing."""
# pylint: disable=protected-access
import types
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import exceptions
from sky import execution
from sky.adaptors import kubernetes
from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.utils import common


def _fill_context() -> dict[str, object]:
    pool_key = reserved_capacity_broker.make_pool_key(
        'phx-context',
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='physical-uid')
    context: dict[str, object] = {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
        constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 3,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
    }
    context.update(
        reserved_capacity.make_protocol_v2_launch_fence(
            pool_key=pool_key,
            service_generation=7,
            service_version=3,
            physical_cluster_uid='physical-uid',
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1))
    return context


def _worker_projection() -> dict[str, object]:
    return {
        'projection_version': 2,
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'phx-context',
        'namespace': 'inference',
        'service_account_name': 'inference-worker',
        'scheduler_name': 'default-scheduler',
        'priority_class_name': 'inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': 'arn:aws:iam::123456789012:role/inference-worker',
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-H200'],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
        'kueue_admission': {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        },
    }


def _policy_fill_context() -> dict[str, object]:
    context = _fill_context()
    projection = _worker_projection()
    context.update({
        constants.RESERVED_FILL_LAUNCH_GATE_GENERATION_KEY: 11,
        constants.RESERVED_FILL_LAUNCH_RECLAIM_FLEET_BUNDLE_SHA256_KEY: 'a' *
                                                                        64,
        constants.RESERVED_FILL_LAUNCH_RECLAIM_POLICY_REVISION_KEY: 'policy-v2',
        constants.RESERVED_FILL_LAUNCH_RECLAIM_PROVIDER_INVENTORY_SHA256_KEY:
            'b' * 64,
        constants.RESERVED_FILL_LAUNCH_WORKER_PROJECTION_SHA256_KEY:
            kubernetes_identity.worker_projection_sha256(projection),
    })
    return context


def _task(*, context='phx-context', accelerator='H200', count=1):
    resource = types.SimpleNamespace(cloud=clouds.Kubernetes(),
                                     region=context,
                                     accelerators={accelerator: count},
                                     job_recovery=None,
                                     autostop_config=None,
                                     hooks=None)
    return types.SimpleNamespace(
        resources=[resource],
        best_resources=resource,
        service=types.SimpleNamespace(pool=False),
        use_spot=False,
        storage_mounts=None,
        file_mounts=None,
        workdir=None,
        envs_and_secrets=None,
        get_required_cloud_features=set,
    )


class _Dag:
    """Minimal one-task DAG accepted by execution._execute_dag()."""

    def __init__(self, task):
        self.tasks = [task]

    def __len__(self):
        return len(self.tasks)


def _execute_fill_stages(task,
                         backend,
                         stages,
                         *,
                         provisioning_skipped=False,
                         handle=None):
    """Runs the provider tail with provider-independent boundaries stubbed."""
    if handle is None:
        handle = mock.MagicMock()
        handle.get_cluster_name.return_value = 'svc-replica'
    backend.provision.return_value = (handle, provisioning_skipped)
    with mock.patch.object(execution.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False), \
         mock.patch.object(execution.global_user_state, 'add_cluster_event'), \
         mock.patch.object(execution.global_user_state, 'update_last_use'), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(execution.provider_phase,
                           'provider_phase',
                           side_effect=lambda _mode: execution.contextlib.nullcontext()), \
         mock.patch.object(kubernetes,
                           'physical_cluster_uid_fence',
                           return_value=execution.contextlib.nullcontext()):
        result = execution._execute_dag(
            _Dag(task),
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=stages,
            cluster_name='svc-replica',
            detach_setup=False,
            no_setup=execution.Stage.SETUP not in stages,
            clone_disk_from=None,
            skip_unnecessary_provisioning=provisioning_skipped,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=_fill_context())
    return result, handle


def _cloud_vm_backend_and_handle(task):
    backend = backends.CloudVmRayBackend()
    backend.provision = mock.MagicMock()
    handle = backends.CloudVmRayResourceHandle(
        cluster_name='svc-replica',
        cluster_name_on_cloud='svc-replica-cloud',
        cluster_yaml='/tmp/svc-replica.yaml',
        launched_nodes=1,
        launched_resources=task.best_resources)
    return backend, handle


def _rejected_effect_guard(error):
    guard = mock.MagicMock()
    guard.__enter__.side_effect = error
    return guard


def test_execution_parser_preserves_ordinary_and_rejects_external_fill():
    assert execution._parse_reserved_fill_launch_fence(
        {}, is_launched_by_sky_serve_controller=False) is None
    with pytest.raises(exceptions.RequestCancelled, match='SkyServe'):
        execution._parse_reserved_fill_launch_fence(
            _fill_context(), is_launched_by_sky_serve_controller=False)


def test_policy_fill_reloads_and_authenticates_exact_v2_projection():
    context = _policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None and fence.policy_bound
    projection = _worker_projection()

    with mock.patch.object(execution.serve_state,
                           'get_placement_projection_record',
                           return_value=(True, None, None, [projection])):
        execution._load_service_worker_projections(context, fence)

    assert context[constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY] == [
        projection
    ]


@pytest.mark.parametrize('mutation', ['digest', 'protocol'])
def test_policy_fill_rejects_stale_or_v1_projection_before_execution(mutation):
    context = _policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None and fence.policy_bound
    projection = _worker_projection()
    if mutation == 'digest':
        projection['namespace'] = 'mutated'
    else:
        projection.pop('projection_version')
        projection.pop('kueue_admission')

    with mock.patch.object(
            execution.serve_state,
            'get_placement_projection_record',
            return_value=(True, None, None, [projection])), \
         pytest.raises(exceptions.RequestCancelled,
                       match='stale persisted worker admission'):
        execution._load_service_worker_projections(context, fence)

    assert constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY not in context


@pytest.mark.parametrize(
    ('context', 'accelerator', 'count'),
    [
        ('other-context', 'H200', 1),
        ('phx-context', 'L4', 1),
        ('phx-context', 'H200', 2),
        ('phx-context', 'H200', 0.5),
    ],
)
def test_final_resource_drift_fails_before_identity_read(
        context, accelerator, count):
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_fill_context())
    assert fence is not None
    task = _task(context=context, accelerator=accelerator, count=count)
    with mock.patch.object(
            reserved_capacity,
            'get_kubernetes_physical_cluster_uid') as get_uid, \
         pytest.raises(exceptions.RequestCancelled):
        execution._validate_reserved_fill_final_resources(task, fence)
    get_uid.assert_not_called()


def test_capture_identity_error_preserves_typed_classification():
    task = _task()
    dag = _Dag(task)
    identity_error = exceptions.KubernetesPhysicalClusterIdentityError(
        'identity changed')
    provider_fence = mock.MagicMock()
    provider_fence.__enter__.side_effect = identity_error

    with mock.patch.object(execution.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(kubernetes,
                           'physical_cluster_uid_fence',
                           return_value=provider_fence), \
         pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='identity changed'):
        execution._execute_dag(dag,
                               dryrun=False,
                               stream_logs=False,
                               handle=None,
                               backend=mock.MagicMock(),
                               retry_until_up=False,
                               optimize_target=common.OptimizeTarget.COST,
                               stages=[execution.Stage.PROVISION],
                               cluster_name='svc-replica',
                               detach_setup=False,
                               no_setup=False,
                               clone_disk_from=None,
                               skip_unnecessary_provisioning=False,
                               _quiet_optimizer=False,
                               _is_launched_by_jobs_controller=False,
                               _is_launched_by_sky_serve_controller=True,
                               _extra_launch_context=_fill_context())


def test_execute_dag_rejects_final_drift_before_fence_or_backend():
    task = _task(context='retargeted-context')
    dag = _Dag(task)
    backend = mock.MagicMock()

    with mock.patch.object(execution.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(kubernetes,
                           'physical_cluster_uid_fence') as provider_fence, \
         pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='no longer matches'):
        execution._execute_dag(dag,
                               dryrun=False,
                               stream_logs=False,
                               handle=None,
                               backend=backend,
                               retry_until_up=False,
                               optimize_target=common.OptimizeTarget.COST,
                               stages=[execution.Stage.PROVISION],
                               cluster_name='svc-replica',
                               detach_setup=False,
                               no_setup=False,
                               clone_disk_from=None,
                               skip_unnecessary_provisioning=False,
                               _quiet_optimizer=False,
                               _is_launched_by_jobs_controller=False,
                               _is_launched_by_sky_serve_controller=True,
                               _extra_launch_context=_fill_context())

    provider_fence.assert_not_called()
    assert not backend.mock_calls


def test_execute_dag_holds_process_fence_around_backend_provision(
        monkeypatch, tmp_path):
    task = _task()
    dag = _Dag(task)
    handle = object()
    backend = mock.MagicMock()

    def provision(*args, **kwargs):
        del args, kwargs
        assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
            'phx-context') == 'physical-uid'
        return handle, False

    backend.provision.side_effect = provision
    capture_path = tmp_path / 'capture.yaml'
    capture_path.write_text('capture', encoding='utf-8')
    monkeypatch.setattr(kubernetes, '_capture_fenced_kubeconfig',
                        lambda _context: str(capture_path))
    raw_client = mock.MagicMock()
    monkeypatch.setattr(kubernetes, '_new_api_client_from_fence_capture',
                        lambda _context, _path: raw_client)
    monkeypatch.setattr(kubernetes,
                        '_read_physical_cluster_uid_from_api_client',
                        lambda _client: 'physical-uid')
    with mock.patch.object(
            execution.global_user_state,
            'cluster_with_name_exists',
            return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(
             reserved_capacity,
             'get_kubernetes_physical_cluster_uid') as get_uid:
        _, result_handle = execution._execute_dag(
            dag,
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.PROVISION],
            cluster_name='svc-replica',
            detach_setup=False,
            no_setup=False,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=_fill_context())

    assert result_handle is handle
    get_uid.assert_not_called()
    backend.provision.assert_called_once()
    assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
        'phx-context') is None


def test_v2_does_not_install_post_fence_replanning_callback(
        monkeypatch, tmp_path):
    task = _task()
    dag = _Dag(task)
    handle = object()
    backend = backends.CloudVmRayBackend()
    backend.register_info = mock.MagicMock()
    backend.provision = mock.MagicMock(return_value=(handle, False))
    capture_path = tmp_path / 'capture.yaml'
    capture_path.write_text('capture', encoding='utf-8')
    monkeypatch.setattr(kubernetes, '_capture_fenced_kubeconfig',
                        lambda _context: str(capture_path))
    monkeypatch.setattr(kubernetes, '_new_api_client_from_fence_capture',
                        lambda _context, _path: mock.MagicMock())
    monkeypatch.setattr(kubernetes,
                        '_read_physical_cluster_uid_from_api_client',
                        lambda _client: 'physical-uid')

    with mock.patch.object(execution.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer):
        _, result_handle = execution._execute_dag(
            dag,
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.OPTIMIZE, execution.Stage.PROVISION],
            cluster_name='svc-replica',
            detach_setup=False,
            no_setup=False,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=_fill_context())

    assert result_handle is handle
    assert backend.register_info.call_args.kwargs['planner'] is None


def test_materialized_authority_flip_blocks_setup_before_runtime():
    task = _task()
    backend = mock.MagicMock()
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before setup')

    backend.checkpoint_reserved_fill_materialized_authority.return_value = None
    backend.reserved_fill_materialized_effect_guard.return_value = (
        _rejected_effect_guard(authority_error))

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(task, backend,
                             [execution.Stage.PROVISION, execution.Stage.SETUP])

    assert exc_info.value is authority_error
    backend.setup.assert_not_called()
    backend.execute.assert_not_called()


def test_materialized_authority_flip_blocks_workdir_sync():
    task = _task()
    task.workdir = '/tmp/workdir'
    task.envs_and_secrets = {}
    backend = mock.MagicMock()
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before sync')

    backend.checkpoint_reserved_fill_materialized_authority.return_value = None
    backend.reserved_fill_materialized_effect_guard.return_value = (
        _rejected_effect_guard(authority_error))

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(
            task, backend,
            [execution.Stage.PROVISION, execution.Stage.SYNC_WORKDIR])

    assert exc_info.value is authority_error
    backend.sync_workdir.assert_not_called()


def test_materialized_authority_flip_blocks_file_mount_sync():
    task = _task()
    task.file_mounts = {'/remote/data': '/local/data'}
    backend = mock.MagicMock()
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before file mounts')

    backend.checkpoint_reserved_fill_materialized_authority.return_value = None
    backend.reserved_fill_materialized_effect_guard.return_value = (
        _rejected_effect_guard(authority_error))

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(
            task, backend,
            [execution.Stage.PROVISION, execution.Stage.SYNC_FILE_MOUNTS])

    assert exc_info.value is authority_error
    backend.sync_file_mounts.assert_not_called()


def test_materialized_authority_flip_blocks_pre_exec_autostop():
    task = _task()
    task.resources[0].autostop_config = types.SimpleNamespace(enabled=True,
                                                              idle_minutes=5,
                                                              down=True,
                                                              wait_for=None)
    backend, handle = _cloud_vm_backend_and_handle(task)
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before autostop')

    backend.checkpoint_reserved_fill_materialized_authority = mock.MagicMock()
    backend.reserved_fill_materialized_effect_guard = mock.MagicMock(
        return_value=_rejected_effect_guard(authority_error))
    with mock.patch.object(execution,
                           '_check_autostop_feasibility_early'), \
         mock.patch.object(execution,
                           'apply_launch_autostop') as apply_autostop, \
         pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(
            task,
            backend, [execution.Stage.PROVISION, execution.Stage.PRE_EXEC],
            handle=handle)

    assert exc_info.value is authority_error
    apply_autostop.assert_not_called()


def test_materialized_authority_flip_blocks_pre_exec_hook_update():
    task = _task()
    task.resources[0].hooks = [{
        'events': ['stop'],
        'run': 'echo hook',
    }]
    backend, handle = _cloud_vm_backend_and_handle(task)
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before hook update')

    backend.checkpoint_reserved_fill_materialized_authority = mock.MagicMock()
    backend.reserved_fill_materialized_effect_guard = mock.MagicMock(
        return_value=_rejected_effect_guard(authority_error))
    backend.set_autostop = mock.MagicMock()
    with mock.patch.object(
            execution,
            '_compute_set_autostop_args_for_hooks_only_relaunch',
            return_value={
                'idle_minutes_to_autostop': -1,
                'wait_for': None,
                'down': False,
                'hooks': task.resources[0].hooks,
            }), \
         pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(
            task,
            backend, [execution.Stage.PROVISION, execution.Stage.PRE_EXEC],
            handle=handle)

    assert exc_info.value is authority_error
    backend.set_autostop.assert_not_called()


def test_materialized_authority_flip_blocks_service_job_submission():
    task = _task()
    backend = mock.MagicMock()
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before execute')

    backend.reserved_fill_materialized_effect_guard.return_value = (
        _rejected_effect_guard(authority_error))

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(task, backend,
                             [execution.Stage.PROVISION, execution.Stage.EXEC])

    assert exc_info.value is authority_error
    backend.execute.assert_not_called()
    backend.post_execute.assert_called_once()


def test_materialized_v2_provision_uses_fresh_guard_for_execute():
    task = _task()
    backend = mock.MagicMock()
    events = []

    @execution.contextlib.contextmanager
    def effect_guard():
        events.append('guard-enter')
        try:
            yield
        finally:
            events.append('guard-exit')

    backend.reserved_fill_materialized_effect_guard.side_effect = effect_guard

    def execute(*_args, **_kwargs):
        assert events == ['guard-enter']
        events.append('execute')
        return 17

    backend.execute.side_effect = execute
    (job_id, _), _ = _execute_fill_stages(
        task, backend, [execution.Stage.PROVISION, execution.Stage.EXEC])

    assert job_id == 17
    assert events == ['guard-enter', 'execute', 'guard-exit']


def test_materialized_v2_provision_without_tail_guard_fails_closed():
    task = _task()
    backend = backends.CloudVmRayBackend()
    backend.provision = mock.MagicMock()
    backend.execute = mock.MagicMock(return_value=17)
    backend.post_execute = mock.MagicMock()

    with pytest.raises(exceptions.ReservedFillLaunchFenceError):
        _execute_fill_stages(task, backend,
                             [execution.Stage.PROVISION, execution.Stage.EXEC])

    backend.execute.assert_not_called()
    backend.post_execute.assert_called_once()


def test_cursor_restore_broken_pipe_cannot_override_materialized_fence():
    task = _task()
    backend = mock.MagicMock()
    authority_error = exceptions.ReservedFillLaunchFenceError(
        'materialized authority changed before setup')
    backend.checkpoint_reserved_fill_materialized_authority.side_effect = (
        authority_error)

    with mock.patch('builtins.print',
                    side_effect=BrokenPipeError('stdout closed')), \
         pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _execute_fill_stages(task, backend,
                             [execution.Stage.PROVISION, execution.Stage.SETUP])

    assert exc_info.value is authority_error
    backend.setup.assert_not_called()


def test_execute_dag_ordinary_launch_does_not_install_provider_fence():
    task = _task()
    dag = _Dag(task)
    handle = object()
    backend = mock.MagicMock()

    def provision(*args, **kwargs):
        del args, kwargs
        assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
            'phx-context') is None
        return handle, False

    backend.provision.side_effect = provision
    phase = mock.MagicMock()
    phase.__enter__.return_value = mock.sentinel.phase_admission
    with mock.patch.object(
            execution.global_user_state,
            'cluster_with_name_exists',
            return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(
             reserved_capacity,
             'get_kubernetes_physical_cluster_uid') as get_uid, \
         mock.patch.object(execution.provider_phase,
                           'provider_phase',
                           return_value=phase) as enter_phase:
        _, result_handle = execution._execute_dag(
            dag,
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.PROVISION],
            cluster_name='ordinary-cluster',
            detach_setup=False,
            no_setup=False,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=False,
            _extra_launch_context={})

    assert result_handle is handle
    enter_phase.assert_called_once_with(
        execution.provider_phase.ProviderPhaseMode.AMBIENT_LEGACY)
    phase.__enter__.assert_called_once()
    phase.__exit__.assert_called_once()
    backend.provision.assert_called_once()
    get_uid.assert_not_called()


def test_execute_dag_enters_v2_phase_before_physical_capture():
    task = _task()
    dag = _Dag(task)
    backend = mock.MagicMock()
    backend.provision.return_value = (mock.sentinel.handle, False)
    events = []

    class RecordingContext:

        def __init__(self, name):
            self._name = name

        def __enter__(self):
            events.append(f'{self._name}-enter')

        def __exit__(self, *_args):
            events.append(f'{self._name}-exit')

    with mock.patch.object(execution.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(execution.provider_phase,
                           'provider_phase',
                           side_effect=lambda _mode: RecordingContext('phase')), \
         mock.patch.object(kubernetes,
                           'physical_cluster_uid_fence',
                           return_value=RecordingContext('physical')):
        _, result_handle = execution._execute_dag(
            dag,
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.PROVISION],
            cluster_name='svc-replica',
            detach_setup=False,
            no_setup=False,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=_fill_context())

    assert result_handle is mock.sentinel.handle
    assert events == [
        'phase-enter', 'physical-enter', 'physical-exit', 'phase-exit'
    ]


def test_outer_executor_defers_fence_to_inner_planning(monkeypatch):
    """The outer wrapper performs no identity read before inner planning."""
    task = _task()
    dag = _Dag(task)

    def execute_all_stages(*args, **kwargs):
        del args
        assert kubernetes.active_physical_cluster_command_target(
            'phx-context') is None
        fence = kwargs['_reserved_fill_launch_fence']
        assert fence.kubernetes_context == 'phx-context'
        assert isinstance(kwargs['_provider_fence_stack'],
                          execution.contextlib.ExitStack)
        return 1, mock.sentinel.handle

    monkeypatch.setattr(execution, '_execute_dag_under_provider_fence',
                        execute_all_stages)
    result = execution._execute_dag(dag,
                                    dryrun=False,
                                    stream_logs=False,
                                    handle=None,
                                    backend=mock.MagicMock(),
                                    retry_until_up=False,
                                    optimize_target=common.OptimizeTarget.COST,
                                    stages=list(execution.Stage),
                                    cluster_name='svc-replica',
                                    detach_setup=False,
                                    no_setup=False,
                                    clone_disk_from=None,
                                    skip_unnecessary_provisioning=False,
                                    _quiet_optimizer=False,
                                    _is_launched_by_jobs_controller=False,
                                    _is_launched_by_sky_serve_controller=True,
                                    _extra_launch_context=_fill_context())

    assert result == (1, mock.sentinel.handle)
    assert kubernetes.active_physical_cluster_command_target(
        'phx-context') is None
