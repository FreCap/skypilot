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
            physical_cluster_uid='physical-uid',
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1))
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
        get_required_cloud_features=set,
    )


class _Dag:
    """Minimal one-task DAG accepted by execution._execute_dag()."""

    def __init__(self, task):
        self.tasks = [task]

    def __len__(self):
        return len(self.tasks)


def test_execution_parser_preserves_ordinary_and_rejects_external_fill():
    assert execution._parse_reserved_fill_launch_fence(
        {}, is_launched_by_sky_serve_controller=False) is None
    with pytest.raises(exceptions.RequestCancelled, match='SkyServe'):
        execution._parse_reserved_fill_launch_fence(
            _fill_context(), is_launched_by_sky_serve_controller=False)


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
