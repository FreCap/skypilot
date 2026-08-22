"""Executor-side tests for durable reserved-fill placement fencing."""
# pylint: disable=protected-access
import concurrent.futures
import types
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import exceptions
from sky import execution
from sky import resources as resources_lib
from sky import task as task_lib
from sky.adaptors import kubernetes
from sky.client import sdk
from sky.provision import common as provision_common
from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.serve import kueue_lane_lineage
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_projection_authority
from sky.server.requests import request_names
from sky.utils import common
from sky.utils import dag_utils


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


def _worker_projection(
    protocol_version: int = kubernetes_identity.
    PLACEMENT_PROJECTION_PROTOCOL_VERSION,
) -> dict[str, object]:
    projection: dict[str, object] = {
        'projection_version': protocol_version,
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
    if protocol_version >= 3:
        projection['provision_timeout'] = -1
        projection['scratch'] = {
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        }
    return projection


def _policy_fill_context(
    protocol_version: int = kubernetes_identity.
    PLACEMENT_PROJECTION_PROTOCOL_VERSION,
) -> dict[str, object]:
    context = _fill_context()
    projection = _worker_projection(protocol_version)
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


def _bound_policy_fill_context() -> dict[str, object]:
    """Build the complete persisted envelope used by a bound fill request."""
    intent_key = 'c' * 64
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference=f'reserved-fill:{intent_key}',
        authorization_generation=7,
        authorization_payload={'intent_key': intent_key})
    context = _policy_fill_context()
    context.update({
        ordinary_launch_binding.ASSOCIATION_ID_KEY: '22222222-2222-4222-8222-222222222222',
        ordinary_launch_binding.BOUND_REQUEST_ID_KEY: 'request-id',
        ordinary_launch_binding.REPLICA_ID_KEY: 5,
        ordinary_launch_binding.REPLICA_RECORD_ID_KEY: '11111111-1111-4111-8111-111111111111',
        ordinary_launch_binding.LAUNCH_GENERATION_KEY: 4,
        ordinary_launch_binding.INPUT_DIGEST_KEY: 'f' * 64,
        ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: 2,
        ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY:
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        ordinary_launch_binding.PROFILE_KIND_KEY: profile.kind.value,
        ordinary_launch_binding.PROFILE_VERSION_KEY: profile.version,
        ordinary_launch_binding.PROFILE_DIGEST_KEY: profile.digest,
        ordinary_launch_binding.CAPABILITY_COHORT_EPOCH_KEY: 1,
        ordinary_launch_binding.CAPABILITY_PROFILE_SET_DIGEST_KEY:
            ordinary_launch_binding.supported_non_pool_profile_set_digest(),
        ordinary_launch_binding.RECEIPT_PROTOCOL_VERSION_KEY:
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION,
        ordinary_launch_binding.AUTHORIZATION_KIND_KEY:
            profile.authorization_kind.value,
        ordinary_launch_binding.AUTHORIZATION_REFERENCE_KEY:
            profile.authorization_reference,
        ordinary_launch_binding.AUTHORIZATION_GENERATION_KEY:
            profile.authorization_generation,
        ordinary_launch_binding.AUTHORIZATION_DIGEST_KEY:
            profile.authorization_digest,
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [_worker_projection()],
    })
    return context


def _task(*, context='phx-context', accelerator='H200', count=1, num_nodes=1):
    resource = types.SimpleNamespace(cloud=clouds.Kubernetes(),
                                     region=context,
                                     accelerators={accelerator: count},
                                     job_recovery=None,
                                     autostop_config=None,
                                     hooks=None)
    resource.assert_launchable = lambda: resource
    return types.SimpleNamespace(
        resources=[resource],
        best_resources=resource,
        num_nodes=num_nodes,
        service=types.SimpleNamespace(pool=False),
        use_spot=False,
        storage_mounts=None,
        file_mounts=None,
        workdir=None,
        envs_and_secrets=None,
        get_required_cloud_features=set,
    )


def _real_task():
    task = task_lib.Task()
    task.set_resources(
        resources_lib.Resources(cloud=clouds.Kubernetes(),
                                region='phx-context',
                                accelerators={'H200': 1}))
    return task


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


def _execute_runtime_registration(
        launch_context: dict[str, object],
        fence: reserved_capacity.ProtocolV2LaunchFence | None, backend):
    """Run only far enough to inspect executor-to-backend registration."""
    task = _task()
    if isinstance(backend, backends.CloudVmRayBackend):
        handle = backends.CloudVmRayResourceHandle(
            cluster_name='svc-replica',
            cluster_name_on_cloud='svc-replica-cloud',
            cluster_yaml='/tmp/svc-replica.yaml',
            launched_nodes=1,
            launched_resources=task.best_resources)
    else:
        handle = mock.MagicMock()
    backend.register_info = mock.MagicMock()
    backend.provision = mock.MagicMock(return_value=(handle, False))
    with mock.patch.object(execution.global_user_state,
                           'cluster_with_name_exists',
                           return_value=False), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(
             execution,
             '_reserved_fill_effect_epoch',
             side_effect=lambda _context: execution.contextlib.nullcontext()), \
         mock.patch.object(
             execution.ordinary_launch_request,
             '_provider_effect_guard',
             side_effect=lambda _context: execution.contextlib.nullcontext()), \
         mock.patch.object(
             execution.provider_phase,
             'provider_phase',
             side_effect=lambda _mode: execution.contextlib.nullcontext()), \
         mock.patch.object(
             kubernetes,
             'physical_cluster_uid_fence',
             return_value=execution.contextlib.nullcontext()), \
         mock.patch.object(
             execution,
             '_apply_service_worker_runtime_projection_to_task'), \
         execution.contextlib.ExitStack() as provider_fence_stack:
        execution._execute_dag_under_provider_fence(
            _Dag(task),
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.PROVISION],
            cluster_name='svc-replica',
            detach_setup=False,
            no_setup=True,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=launch_context,
            _reserved_fill_launch_fence=fence,
            _provider_fence_stack=provider_fence_stack)
    return backend


def test_bound_reserved_fill_passes_kueue_runtime_to_cloud_vm_backend():
    context = _bound_policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    backend = backends.CloudVmRayBackend()
    runtime_factory_impl = (
        execution.kueue_lane_observer.runtime_for_reserved_fill_launch)
    durable_admission = types.SimpleNamespace(
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
        pod_namespace='inference',
        pod_name='svc-replica-cloud-head',
        pod_uid='persisted-pod-uid')
    engine = types.SimpleNamespace(begin=lambda: execution.contextlib.
                                   nullcontext(mock.sentinel.connection))

    with mock.patch.object(execution.kueue_lane_observer,
                           'runtime_for_reserved_fill_launch',
                           wraps=runtime_factory_impl) as runtime_factory, \
         mock.patch.object(
             execution.kueue_lane_observer.serve_state_schema,
             'get_database_engine',
             return_value=engine), \
         mock.patch.object(
             execution.kueue_lane_observer,
             '_lock_and_validate_materialization',
             return_value=(mock.sentinel.repository, mock.sentinel.identity,
                           durable_admission)):
        _execute_runtime_registration(context, fence, backend)

    runtime_factory.assert_called_once_with(context, fence)
    registration = backend.register_info.call_args.kwargs
    runtime = registration['kueue_admission_runtime']
    assert isinstance(runtime, provision_common.KueuePodAdmissionRuntime)
    identity = runtime.identity
    assert identity.intent_key == 'c' * 64
    assert (
        identity.replica_record_uuid == '11111111-1111-4111-8111-111111111111')
    assert identity.pool_physical_uid == 'physical-uid'
    assert (identity.worker_projection_sha256 == fence.worker_projection_sha256)
    assert runtime.accelerator == 'h200'
    assert callable(runtime.observer)
    assert callable(runtime.observer.begin_observation)
    assert runtime.persisted_pod_identity == (
        provision_common.KueuePersistedPodIdentity(
            namespace='inference',
            pod_name='svc-replica-cloud-head',
            pod_uid='persisted-pod-uid'))


def test_unbound_legacy_reserved_fill_never_builds_kueue_runtime():
    context = _fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    backend = backends.CloudVmRayBackend()

    with mock.patch.object(
            execution.kueue_lane_observer,
            'runtime_for_reserved_fill_launch') as runtime_factory:
        _execute_runtime_registration(context, fence, backend)

    runtime_factory.assert_not_called()
    registration = backend.register_info.call_args.kwargs
    assert registration['kueue_admission_runtime'] is None


def test_unbound_policy_reserved_fill_without_projection_fails_closed():
    context = _policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    backend = backends.CloudVmRayBackend()

    with mock.patch.object(
            execution.kueue_lane_observer,
            'runtime_for_reserved_fill_launch') as runtime_factory:
        with pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                           match='lost its immutable worker projection'):
            _execute_runtime_registration(context, fence, backend)

    runtime_factory.assert_not_called()


def test_ordinary_launch_does_not_build_kueue_runtime():
    backend = backends.CloudVmRayBackend()

    with mock.patch.object(
            execution.kueue_lane_observer,
            'runtime_for_reserved_fill_launch') as runtime_factory:
        _execute_runtime_registration({}, None, backend)

    runtime_factory.assert_not_called()
    registration = backend.register_info.call_args.kwargs
    assert registration['kueue_admission_runtime'] is None


def test_ordinary_non_cloud_vm_backend_receives_no_kueue_arguments():
    backend = mock.MagicMock()

    with mock.patch.object(
            execution.kueue_lane_observer,
            'runtime_for_reserved_fill_launch') as runtime_factory:
        _execute_runtime_registration({}, None, backend)

    runtime_factory.assert_not_called()
    registration = backend.register_info.call_args.kwargs
    assert not any(key.startswith('kueue_') for key in registration)


def test_execution_parser_preserves_ordinary_and_rejects_external_fill():
    assert execution._parse_reserved_fill_launch_fence(
        {}, is_launched_by_sky_serve_controller=False) is None
    with pytest.raises(exceptions.RequestCancelled, match='SkyServe'):
        execution._parse_reserved_fill_launch_fence(
            _fill_context(), is_launched_by_sky_serve_controller=False)


def test_policy_fill_reloads_and_authenticates_exact_current_projection():
    context = _policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None and fence.policy_bound
    projection = _worker_projection()

    with mock.patch.object(execution.serve_state,
                           'get_placement_projection_record',
                           return_value=(True, None, None, [projection])), \
         mock.patch.object(execution.serve_state,
                           'get_placement_catalog',
                           return_value={'num_nodes': 1}):
        execution._load_service_worker_projections(context, fence)

    assert context[constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY] == [
        projection
    ]


@pytest.mark.parametrize('protocol_version', [2, 3, 4, 5])
def test_policy_fill_rejects_historical_projection_before_provider(
        protocol_version):
    projection = _worker_projection(protocol_version=protocol_version)
    context = _policy_fill_context(protocol_version)
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None and fence.policy_bound

    with mock.patch.object(execution.serve_state,
                           'get_placement_projection_record',
                           return_value=(True, None, None, [projection])), \
         pytest.raises(exceptions.RequestCancelled,
                       match='invalid or stale persisted worker admission'):
        execution._load_service_worker_projections(context, fence)

    assert constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY not in context
    assert kubernetes_identity.validate_worker_placement_projections(
        [projection]) == [projection]


def test_worker_projection_versions_cannot_mix_in_one_persisted_record():
    with pytest.raises(ValueError, match='must not mix protocol versions'):
        kubernetes_identity.validate_worker_placement_projections(
            [_worker_projection(2),
             _worker_projection(3)])


def test_historical_v5_decodes_but_cannot_publish_reclaim_admission():
    projection = _worker_projection(5)
    decoded, _ = (
        reserved_fill_projection_authority.projected_admission_for_candidate(
            [projection],
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1))
    assert decoded == projection

    with pytest.raises(ValueError, match='required version 6'):
        reserved_fill_projection_authority.projected_admissions_for_edge(
            [projection],
            access_context='phx-context',
            accelerator_names=('H200',),
            accelerator_count=1,
            require_current_protocol=True)


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
        execution._validate_reserved_fill_final_resources(task, fence, None)
    get_uid.assert_not_called()


def test_v2_finalizes_frozen_singleton_without_optimizer_state():
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_fill_context())
    assert fence is not None
    task = _task()
    pinned = task.resources[0]
    task.best_resources = None

    execution._finalize_reserved_fill_resources(task, fence)

    assert task.best_resources is pinned


def test_v2_real_frozen_resource_round_trip_is_exact_and_launchable():
    source_resource = resources_lib.Resources(
        cloud=clouds.Kubernetes(),
        region='phx-context',
        instance_type='4CPU--16GB--H200:1',
        accelerators={'H200': 1},
        use_spot=False)
    source_resource.assert_launchable()
    source_task = task_lib.Task(run='echo ready').set_resources(source_resource)

    prepared = sdk.prepare_launch_request_for_server_controller(
        source_task,
        'svc-replica',
        workspace='default',
        extra_launch_context=_fill_context())
    launch_body = prepared.body
    fence = reserved_capacity.parse_protocol_v2_launch_fence(
        launch_body.extra_launch_context)
    assert fence is not None

    finalized_resources = []
    for _ in range(2):
        # Each body inspection and DAG load starts from the immutable bytes,
        # matching independent first-attempt and replay executor processes.
        deserialized_dag = dag_utils.load_dag_from_yaml_str(prepared.body.task)
        deserialized_task = deserialized_dag.tasks[0]
        assert deserialized_task.best_resources is None
        execution._finalize_reserved_fill_resources(deserialized_task, fence)
        finalized = deserialized_task.best_resources
        assert finalized is not None
        assert finalized.assert_launchable() is finalized
        finalized_resources.append(finalized)

    assert finalized_resources[0] is not finalized_resources[1]
    expected_config = source_resource.to_yaml_config()
    assert [resource.to_yaml_config() for resource in finalized_resources
           ] == [expected_config] * 2


def test_v2_policy_mode_rejects_configured_admin_policy():
    with mock.patch.object(reserved_capacity.skypilot_config,
                           'get_nested',
                           return_value='company.Policy'), \
         pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='admin policy to be absent'):
        reserved_capacity.require_protocol_v2_admin_policy_absent()


def test_v2_controller_preparation_rejects_before_request_freeze():
    error = reserved_capacity.ReservedFillLaunchFenceError('policy present')
    with mock.patch.object(
            reserved_capacity,
            'require_protocol_v2_admin_policy_absent',
            side_effect=error) as require_policy_absent, \
         mock.patch.object(sdk, '_freeze_launch_request') as freeze, \
         pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='policy present'):
        sdk.prepare_launch_request_for_server_controller(
            _real_task(),
            'svc-replica',
            workspace='workspace',
            extra_launch_context=_fill_context())

    require_policy_absent.assert_called_once_with()
    freeze.assert_not_called()


def test_v2_executor_rejects_policy_before_policy_application():
    error = reserved_capacity.ReservedFillLaunchFenceError('policy present')
    with mock.patch.object(
            reserved_capacity,
            'require_protocol_v2_admin_policy_absent',
            side_effect=error) as require_policy_absent, \
         mock.patch.object(
             execution.admin_policy_utils,
             'apply_and_use_config_in_current_request') as apply_policy, \
         pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='policy present'):
        execution._execute(
            _real_task(),
            cluster_name='svc-replica',
            _request_name=request_names.AdminPolicyRequestName.CLUSTER_LAUNCH,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=_fill_context())

    require_policy_absent.assert_called_once_with()
    apply_policy.assert_not_called()


def test_v2_finalization_rejects_non_singleton_with_valid_best_resource():
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_fill_context())
    assert fence is not None
    task = _task()
    task.resources.append(
        types.SimpleNamespace(cloud=clouds.Kubernetes(),
                              region='phx-context',
                              accelerators={'H200': 1}))

    with pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='one exact resource'):
        execution._finalize_reserved_fill_resources(task, fence)


def test_v2_finalization_uses_frozen_singleton_not_optimizer_residue():
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_fill_context())
    assert fence is not None
    task = _task()
    pinned = task.resources[0]
    task.best_resources = _task().resources[0]

    execution._finalize_reserved_fill_resources(task, fence)

    assert task.best_resources is pinned


def test_v2_finalization_rejects_non_launchable_singleton():
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_fill_context())
    assert fence is not None
    task = _task()
    task.best_resources = None
    task.resources[0].assert_launchable = mock.MagicMock(
        side_effect=AssertionError('not launchable'))

    with pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='finalized launchable resource'):
        execution._finalize_reserved_fill_resources(task, fence)


def test_v2_finalization_rejects_unfenced_singleton_request():
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_fill_context())
    assert fence is not None
    task = _task(context='retargeted-context')
    task.best_resources = None

    with pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='no longer matches'):
        execution._finalize_reserved_fill_resources(task, fence)


def test_multi_node_kueue_fill_fails_before_provider_identity_read():
    context = _bound_policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    task = _task(num_nodes=2)

    with mock.patch.object(
            kubernetes,
            'physical_cluster_uid_fence') as physical_identity, \
         pytest.raises(reserved_capacity.ReservedFillLaunchFenceError,
                       match='exactly one task node'):
        execution._validate_reserved_fill_final_resources(
            task, fence,
            context[constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY])

    physical_identity.assert_not_called()


def test_execution_rejects_multi_node_kueue_catalog():
    context = _policy_fill_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    projection = _worker_projection()

    with mock.patch.object(execution.serve_state,
                           'get_placement_projection_record',
                           return_value=(True, None, None, [projection])), \
         mock.patch.object(execution.serve_state,
                           'get_placement_catalog',
                           return_value={'num_nodes': 2}), \
         pytest.raises(exceptions.RequestCancelled,
                       match='invalid or stale') as exc_info:
        execution._load_service_worker_projections(context, fence)
    assert 'placement_catalog.num_nodes == 1' in str(exc_info.value.__cause__)


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


@pytest.mark.parametrize('cluster_exists', [False, True])
def test_v2_reconstructs_final_resources_before_optimizer(
        monkeypatch, tmp_path, cluster_exists):
    task = _task()
    pinned = task.resources[0]
    task.best_resources = None
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
                           return_value=cluster_exists), \
         mock.patch.object(execution.container_image_consumers,
                           'derive',
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(execution.optimizer.Optimizer,
                           'optimize',
                           side_effect=AssertionError(
                               'v2 must not invoke optimizer')) as optimize:
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
    assert task.best_resources is pinned
    optimize.assert_not_called()
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


def test_execute_dag_bounds_v2_phase_to_physical_capture():
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
        'phase-enter', 'physical-enter', 'phase-exit', 'physical-exit'
    ]


def test_pool_a_gated_wait_allows_compatible_v2_work_but_not_ambient_context(
        monkeypatch, tmp_path):
    """A passive wait retains only its joinable exact physical capture."""
    task = _task()
    dag = _Dag(task)
    backend = mock.MagicMock()
    events = []
    service_authority_active = False
    provider_phase_active = False

    @execution.contextlib.contextmanager
    def effect_guard(context):
        nonlocal service_authority_active
        assert context is launch_context
        assert not service_authority_active
        service_authority_active = True
        events.append('service-effect-enter')
        try:
            yield
        finally:
            events.append('service-effect-exit')
            service_authority_active = False

    @execution.contextlib.contextmanager
    def phase(mode):
        nonlocal provider_phase_active
        assert not provider_phase_active
        provider_phase_active = True
        events.append(f'phase-{mode.value}-enter')
        try:
            yield
        finally:
            events.append(f'phase-{mode.value}-exit')
            provider_phase_active = False

    def provision(*_args, **_kwargs):
        # Model pool A blocked behind Kueue. Database-only update/recovery and
        # same-UID protocol-v2 work must make progress here; the old whole-tail
        # service/provider scopes kept both flags true for this callback. The
        # immutable physical capture remains active and fails closed for an
        # unrelated tokenless legacy caller against this exact context.
        assert not service_authority_active
        assert not provider_phase_active
        assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
            'phx-context') == 'physical-uid'
        events.append('pool-a-passive-wait')
        events.append('service-update')

        def same_uid_v2_work():
            with kubernetes.physical_cluster_uid_fence('phx-context',
                                                       'physical-uid',
                                                       require_existing=True):
                with phase(
                        execution.provider_phase.ProviderPhaseMode.V2_FENCED):
                    events.append('same-uid-v2-recovery')
                with effect_guard(launch_context), phase(
                        execution.provider_phase.ProviderPhaseMode.V2_FENCED):
                    events.append('pool-b-materialize')

        def tokenless_same_context_read():
            kubernetes.active_physical_cluster_command_target('phx-context')

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(same_uid_v2_work).result(timeout=5)
            blocked_read = executor.submit(tokenless_same_context_read)
            with pytest.raises(
                    kubernetes.KubernetesPhysicalClusterFenceBusyError):
                blocked_read.result(timeout=5)
        events.append('same-context-ambient-blocked')
        return mock.sentinel.handle, False

    launch_context = _fill_context()
    backend.provision.side_effect = provision
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
                           return_value=mock.sentinel.image_consumer), \
         mock.patch.object(execution.ordinary_launch_request,
                           '_provider_effect_guard',
                           side_effect=effect_guard), \
         mock.patch.object(execution.provider_phase,
                           'provider_phase',
                           side_effect=phase):
        _, result_handle = execution._execute_dag(
            dag,
            dryrun=False,
            stream_logs=False,
            handle=None,
            backend=backend,
            retry_until_up=False,
            optimize_target=common.OptimizeTarget.COST,
            stages=[execution.Stage.PROVISION],
            cluster_name='svc-replica-a',
            detach_setup=False,
            no_setup=False,
            clone_disk_from=None,
            skip_unnecessary_provisioning=False,
            _quiet_optimizer=False,
            _is_launched_by_jobs_controller=False,
            _is_launched_by_sky_serve_controller=True,
            _extra_launch_context=launch_context)

    assert result_handle is mock.sentinel.handle
    assert events == [
        'service-effect-enter', 'phase-v2-fenced-enter', 'phase-v2-fenced-exit',
        'service-effect-exit', 'pool-a-passive-wait', 'service-update',
        'phase-v2-fenced-enter', 'same-uid-v2-recovery', 'phase-v2-fenced-exit',
        'service-effect-enter', 'phase-v2-fenced-enter', 'pool-b-materialize',
        'phase-v2-fenced-exit', 'service-effect-exit',
        'same-context-ambient-blocked'
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
