"""Executor tests for immutable ordinary-paid placement authority."""
# pylint: disable=protected-access
import contextlib
import types
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import exceptions
from sky import execution
from sky import resources as resources_lib
from sky import task as task_lib
from sky.backends import cloud_vm_ray_backend
from sky.client import sdk
from sky.serve import constants
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import spot_placer
from sky.utils import common
from sky.utils import dag_utils

_REPLICA_RECORD_ID = '11111111-1111-4111-8111-111111111111'


class _Dag:

    def __init__(self, task):
        self.tasks = [task]

    def __len__(self):
        return len(self.tasks)


def _location(provider: str = 'aws') -> spot_placer.Location:
    cloud, region, zone, instance_type = {
        'aws': (clouds.AWS(), 'eu-south-2', 'eu-south-2b', 'g6.4xlarge'),
        'gcp': (clouds.GCP(), 'us-central1', 'us-central1-a', 'g2-standard-4'),
    }[provider]
    return spot_placer.Location(cloud=cloud,
                                region=region,
                                zone=zone,
                                accelerators={'L4': 1},
                                use_spot=True,
                                instance_type=instance_type)


def _pool_key(provider: str = 'aws') -> str:
    return paid_capacity.pool_key(
        _location(provider),
        workspace='default',
        num_nodes=1,
        aws_account_id=('123456789012' if provider == 'aws' else None),
        gcp_project_id=('boltz-spot-project' if provider == 'gcp' else None))


def _bound_paid_context(pool_key: str | None = None,
                        provider: str = 'aws') -> dict[str, object]:
    pool_key = _pool_key(provider) if pool_key is None else pool_key
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference=(
            f'paid-capacity:service-hash:{_REPLICA_RECORD_ID}:{pool_key}'),
        authorization_generation=0,
        authorization_payload={
            'claim': {
                'claimed_at': '2026-09-01T00:00:00+00:00',
                'pool_key': pool_key,
                'priority': 0,
                'service_hash': 'service-hash',
                'capacity_plan_generation': 1,
                'capacity_plan_sha256': 'a' * 64,
                'demand_feed_generation': 1,
                'demand_source_epoch': 'demand-source-epoch',
                'capacity_plan_accelerator': 'l4',
                'capacity_plan_units': 1,
            },
            'placement': {
                'cluster_name': 'svc-replica-5',
                'is_spot': True,
                'location': None,
                'planned_capacity': 1,
                'resources_override': None,
                'service_version': 1,
            },
        })
    return {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        ordinary_launch_binding.ASSOCIATION_ID_KEY: '22222222-2222-4222-8222-222222222222',
        ordinary_launch_binding.BOUND_REQUEST_ID_KEY: 'request-id',
        ordinary_launch_binding.REPLICA_ID_KEY: 5,
        ordinary_launch_binding.REPLICA_RECORD_ID_KEY: _REPLICA_RECORD_ID,
        ordinary_launch_binding.LAUNCH_GENERATION_KEY: 1,
        ordinary_launch_binding.INPUT_DIGEST_KEY: 'f' * 64,
        ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY:
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        ordinary_launch_binding.PROFILE_KIND_KEY: profile.kind.value,
        ordinary_launch_binding.PROFILE_VERSION_KEY: profile.version,
        ordinary_launch_binding.PROFILE_DIGEST_KEY: profile.digest,
        ordinary_launch_binding.CAPABILITY_COHORT_EPOCH_KEY:
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
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
    }


def _task(*, num_nodes: int = 1, provider: str = 'aws') -> task_lib.Task:
    location = _location(provider)
    task = task_lib.Task(num_nodes=num_nodes)
    task.set_resources(
        resources_lib.Resources(cloud=location.cloud,
                                region=location.region,
                                zone=location.zone,
                                instance_type=location.instance_type,
                                cpus=16,
                                memory=64,
                                accelerators={'L4': 1},
                                use_spot=True))
    task.best_resources = None
    return task


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
@pytest.mark.parametrize('cluster_exists', [False, True])
def test_bound_paid_exact_placement_bypasses_process_local_catalog(
        monkeypatch, cluster_exists, provider):
    """A PostgreSQL-admitted exact pool is the sole placement authority."""
    source_task = _task(provider=provider)
    context = _bound_paid_context(provider=provider)
    prepared = sdk.prepare_launch_request_for_server_controller(
        source_task,
        'svc-replica-5',
        workspace='default',
        extra_launch_context=context)
    task = dag_utils.load_dag_from_yaml_str(prepared.body.task).tasks[0]
    pinned = next(iter(task.resources))
    assert task.best_resources is None
    backend = backends.CloudVmRayBackend()
    backend.register_info = mock.MagicMock()
    handle = object()
    backend.provision = mock.MagicMock(return_value=(handle, False))
    context = prepared.body.extra_launch_context

    monkeypatch.setattr(execution.global_user_state, 'cluster_with_name_exists',
                        lambda _name: cluster_exists)
    monkeypatch.setattr(execution.container_image_consumers, 'derive',
                        lambda *_args, **_kwargs: mock.sentinel.image_consumer)
    provider_guard = mock.Mock(return_value=contextlib.nullcontext())
    monkeypatch.setattr(execution.ordinary_launch_request,
                        '_provider_effect_guard', provider_guard)
    monkeypatch.setattr(execution.provider_phase, 'provider_phase',
                        lambda _mode: contextlib.nullcontext())
    optimize = mock.Mock(
        side_effect=AssertionError('bound paid launch re-ran optimizer'))
    monkeypatch.setattr(execution.optimizer.Optimizer, 'optimize', optimize)

    _, result_handle = execution._execute_dag_under_provider_fence(
        _Dag(task),
        dryrun=False,
        stream_logs=False,
        handle=None,
        backend=backend,
        retry_until_up=False,
        optimize_target=common.OptimizeTarget.COST,
        stages=[execution.Stage.OPTIMIZE, execution.Stage.PROVISION],
        cluster_name='svc-replica-5',
        detach_setup=False,
        no_setup=True,
        clone_disk_from=None,
        skip_unnecessary_provisioning=False,
        _quiet_optimizer=False,
        _is_launched_by_jobs_controller=False,
        _is_launched_by_sky_serve_controller=True,
        _extra_launch_context=context,
        _reserved_fill_launch_fence=None,
        _provider_fence_stack=contextlib.ExitStack())

    assert result_handle is handle
    assert task.best_resources is pinned
    optimize.assert_not_called()
    provider_guard.assert_called_once_with(context)
    assert backend.register_info.call_args.kwargs['planner'] is None
    assert (
        backend.register_info.call_args.kwargs['exact_ordinary_paid_placement']
        is True)
    backend.provision.assert_called_once_with(
        task,
        pinned,
        dryrun=False,
        stream_logs=False,
        cluster_name='svc-replica-5',
        retry_until_up=False,
        skip_unnecessary_provisioning=False)


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
def test_bound_paid_provider_unavailability_never_reoptimizes(
        tmp_path, monkeypatch, provider):
    """Exact paid authority returns one failed candidate to PostgreSQL."""
    task = _task(provider=provider)
    pinned = next(iter(task.resources))
    task.best_resources = pinned
    provisioner = cloud_vm_ray_backend.RetryingVmProvisioner(
        log_dir=str(tmp_path),
        dag=_Dag(task),
        optimize_target=common.OptimizeTarget.COST,
        requested_features=set(),
        local_wheel_path=tmp_path / 'wheel',
        wheel_hash='',
        exact_ordinary_paid_placement=True,
        extra_launch_context={},
    )
    config = cloud_vm_ray_backend.RetryingVmProvisioner.ToProvisionConfig(
        cluster_name='svc-replica-5',
        resources=pinned,
        num_nodes=1,
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        prev_config_hash=None,
    )
    provider_error = exceptions.ResourcesUnavailableError(
        f'{provider} exact paid pool unavailable')
    provider_attempt = mock.Mock(side_effect=provider_error)
    monkeypatch.setattr(provisioner, '_retry_zones', provider_attempt)
    monkeypatch.setattr(type(pinned.cloud), 'get_active_user_identity',
                        lambda *_args, **_kwargs: ['provider-identity'])
    optimize = mock.Mock(
        side_effect=AssertionError('exact paid placement re-ran optimizer'))
    monkeypatch.setattr(cloud_vm_ray_backend.optimizer.Optimizer, 'optimize',
                        optimize)
    monkeypatch.setattr(cloud_vm_ray_backend.rich_utils, 'force_update_status',
                        lambda *_args, **_kwargs: None)

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        provisioner.provision_with_retries(
            task,
            config,
            dryrun=False,
            stream_logs=False,
            skip_unnecessary_provisioning=False,
        )

    assert exc_info.value is provider_error
    assert task.best_resources is pinned
    provider_attempt.assert_called_once()
    optimize.assert_not_called()


def test_exact_paid_authority_does_not_leak_between_backend_registrations():
    backend = backends.CloudVmRayBackend()

    backend.register_info(exact_ordinary_paid_placement=True)
    assert backend._exact_ordinary_paid_placement is True

    backend.register_info()
    assert backend._exact_ordinary_paid_placement is False


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
def test_bound_paid_finalizer_supports_exact_provider_pools(provider):
    task = _task(provider=provider)
    pinned = next(iter(task.resources))

    assert execution._finalize_bound_ordinary_paid_resources(
        task, _bound_paid_context(provider=provider))
    assert task.best_resources is pinned


@pytest.mark.parametrize(('provider', 'provider_scope'), [
    ('aws', '123456789012'),
    ('gcp', 'boltz-spot-project'),
])
def test_bound_paid_pool_key_is_shared_with_provider_scope(
        provider, provider_scope):
    context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        _bound_paid_context(provider=provider))

    assert ordinary_launch_binding._ordinary_paid_pool_key(  # pylint: disable=protected-access
        context) == _pool_key(provider)
    if provider == 'aws':
        assert (ordinary_launch_binding.ordinary_paid_aws_account_id(context) ==
                provider_scope)
    else:
        assert (ordinary_launch_binding.ordinary_paid_gcp_project_id(context) ==
                provider_scope)


@pytest.mark.parametrize(('task', 'match'), [
    (_task(num_nodes=2), 'node count'),
    (types.SimpleNamespace(resources=[], best_resources=None,
                           num_nodes=1), 'one exact resource'),
])
def test_bound_paid_exact_placement_rejects_malformed_task(task, match):
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match=match):
        execution._finalize_bound_ordinary_paid_resources(
            task, _bound_paid_context())


def test_bound_paid_exact_placement_rejects_pool_resource_drift():
    task = _task()
    candidate = next(iter(task.resources)).copy(zone='eu-south-2a')
    task.set_resources(candidate)
    task.best_resources = None

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='exact paid pool'):
        execution._finalize_bound_ordinary_paid_resources(
            task, _bound_paid_context())


def test_bound_paid_exact_placement_requires_launchable_resource():
    candidate = types.SimpleNamespace(cloud=clouds.AWS(),
                                      region='eu-south-2',
                                      zone='eu-south-2b',
                                      instance_type='g6.4xlarge',
                                      accelerators={'L4': 1},
                                      use_spot=True)
    candidate.assert_launchable = mock.Mock(
        side_effect=AssertionError('not launchable'))
    task = types.SimpleNamespace(resources=[candidate],
                                 best_resources=None,
                                 num_nodes=1)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='finalized launchable resource'):
        execution._finalize_bound_ordinary_paid_resources(
            task, _bound_paid_context())


def test_bound_paid_malformed_decoded_pool_is_binding_conflict(monkeypatch):
    identity = paid_capacity.pool_key_payload(_pool_key())
    assert identity is not None
    identity['region'] = None
    monkeypatch.setattr(ordinary_launch_binding.paid_capacity,
                        'pool_key_payload', lambda _key: identity)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='exact Spot pool'):
        ordinary_launch_binding.ordinary_paid_pool_identity(
            ordinary_launch_binding.parse_bound_non_pool_launch_context(
                _bound_paid_context()))
