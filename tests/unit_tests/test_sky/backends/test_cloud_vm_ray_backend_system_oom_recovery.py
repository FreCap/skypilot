"""CloudVmRayBackend system-OOM recovery authorization tests."""

import concurrent.futures
import json
import types
from unittest import mock

import pytest

import sky
from sky import clouds
from sky.backends import cloud_vm_ray_backend
from sky.backends import system_oom_recovery as backend_recovery
from sky.provision import common as provision_common
from sky.schemas.generated import jobsv1_pb2
from sky.serve import constants as serve_constants
from sky.serve import system_oom_recovery as serve_recovery
from sky.skylet import job_lib
from sky.skylet import system_oom_recovery as runtime_recovery
from sky.utils import message_utils

_IMAGE_DIGEST = 'sha256:' + 'a' * 64
_PINNED_IMAGE = f'example.invalid/model@{_IMAGE_DIGEST}'
_SERVICE_NAME = 'boltz-l4-fleet'
_SERVICE_HASH = 'service-hash-1'
_OWNER_IDENTITY = ('aws-user-id', '123456789012')


def _task(market_type: str = 'on_demand') -> sky.Task:
    task = sky.Task(run=_owned_spec().render(), envs={'MODEL': 'boltz'})
    task.set_resources(
        sky.Resources(cloud=clouds.AWS(),
                      instance_type='g6.xlarge',
                      region='us-east-1',
                      zone='us-east-1a',
                      memory='16',
                      use_spot=market_type == 'spot'))
    return task


def _owned_spec() -> runtime_recovery.OwnedContainerSpec:
    return runtime_recovery.OwnedContainerSpec(image=_PINNED_IMAGE)


def _authorization(task: sky.Task) -> dict[str, object]:
    spec = _owned_spec()
    resource, = task.resources
    market_type = 'spot' if resource.use_spot else 'on_demand'
    return {
        'authorization_version': 3,
        'profile_id': 'boltz-l4-v3',
        'workspace': 'default',
        'service_name': _SERVICE_NAME,
        'service_hash': _SERVICE_HASH,
        'task_sha256': serve_recovery.safety_profile_digest(task),
        'runtime_image_digest': _IMAGE_DIGEST,
        'runtime_profile_version': 2,
        'required_runtime_capability': runtime_recovery.CAPABILITY_V2,
        'owned_container_spec': spec.to_dict(),
        'owned_container_spec_sha256': serve_recovery._sha256_json(  # pylint: disable=protected-access
            spec.to_dict()),
        'execution_envelope_sha256': serve_recovery._sha256_json(  # pylint: disable=protected-access
            runtime_recovery.RecoveryExecutionEnvelope.standard().to_dict()),
        'resource_envelope': {
            'provider': 'aws',
            'allowed_aws_account_ids': ['123456789012'],
            'allowed_locations': [{
                'region': 'us-east-1',
                'availability_zones': ['us-east-1a'],
            }],
            'allowed_market_types': [market_type],
            'allowed_instance_types': ['g6.xlarge'],
            'max_host_memory_gib': 16,
            'num_nodes': 1,
            'dedicated': True,
            'require_new_create': True,
            'required_identity': [
                'aws_account_id', 'region', 'availability_zone',
                'ec2_instance_id'
            ],
        },
    }


def _context(*,
             include_contract: bool = True,
             market_type: str = 'on_demand') -> dict[str, object]:
    trusted = serve_recovery._authorization_v3_from_dict(  # pylint: disable=protected-access
        _authorization(_task(market_type)))
    requested = serve_recovery.RequestedRecoveryAuthorizationV3.from_authorization(
        trusted)
    intent = requested.to_intent_fields()
    intent.update({
        'service_hash': _SERVICE_HASH,
        'replica_id': 7,
        'launch_generation': 2,
        'launch_nonce': 'b' * 64,
    })
    context = serve_recovery.create_unbound_launch_context(
        intent,
        service_name=_SERVICE_NAME,
        service_version=4,
        controller_pid=123,
        controller_ip='10.0.0.2')
    context = serve_recovery.bind_launch_context(context, 'request-1')
    if not include_contract:
        context.pop(
            serve_constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY)
    return context


def _install_profile(monkeypatch, task: sky.Task) -> None:
    monkeypatch.setenv(
        serve_constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR,
        json.dumps({
            'version': 3,
            'profiles': [_authorization(task)],
        }))


def _cluster_info(
    *,
    provider_name: str = 'aws',
    head_instance_id: str = 'i-head',
    instance_ids: tuple[str, ...] = ('i-head',)
) -> provision_common.ClusterInfo:
    return provision_common.ClusterInfo(instances={
        instance_id: [
            provision_common.InstanceInfo(instance_id=instance_id,
                                          internal_ip='10.0.0.1',
                                          external_ip='1.2.3.4',
                                          tags={})
        ] for instance_id in instance_ids
    },
                                        head_instance_id=head_instance_id,
                                        provider_name=provider_name)


def _handle(*,
            cluster_name: str = 'replica-1',
            cluster_name_on_cloud: str = 'provider-cluster',
            provider=clouds.AWS(),
            launched_nodes: int = 1,
            num_ips_per_node: int = 1,
            cluster_info: provision_common.ClusterInfo | None = None):
    return types.SimpleNamespace(
        cluster_name=cluster_name,
        cluster_name_on_cloud=cluster_name_on_cloud,
        launched_nodes=launched_nodes,
        num_ips_per_node=num_ips_per_node,
        launched_resources=types.SimpleNamespace(cloud=provider,
                                                 instance_type='g6.xlarge',
                                                 region='us-east-1',
                                                 zone='us-east-1a'),
        cached_cluster_info=cluster_info or _cluster_info(),
        provision_runtime_metadata=types.SimpleNamespace(has_ray=True,
                                                         has_job_queue=True))


def _evidence(**overrides) -> backend_recovery.FreshProvisionEvidence:
    values = {
        'request_id': 'request-1',
        'workspace': 'default',
        'cluster_name': 'replica-1',
        'cluster_name_on_cloud': 'provider-cluster',
        'cluster_hash': 'cluster-generation-1',
        'provider_name': 'aws',
        'requested_node_count': 1,
        'head_instance_id': 'i-head',
        'created_instance_ids': ('i-head',),
        'aws_account_id': '123456789012',
        'provision_owner_identity': _OWNER_IDENTITY,
        'region': 'us-east-1',
        'availability_zone': 'us-east-1a',
        'instance_type': 'g6.xlarge',
        'market_type': 'on_demand',
        'catalog_memory_gib': 16.0,
        'service_name': _SERVICE_NAME,
        'service_hash': _SERVICE_HASH,
    }
    values.update(overrides)
    return backend_recovery.FreshProvisionEvidence(**values)


def _recovery_info() -> job_lib.JobSystemRecoveryInfo:
    return job_lib.JobSystemRecoveryInfo(
        capability=runtime_recovery.CAPABILITY_V2,
        phase=job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED,
        original_attempt_id='attempt-original',
        replacement_attempt_id='attempt-replacement',
        task_index=0,
        node_boot_id='boot-id',
        occurrence_count=1,
        armed_at=100.0,
        updated_at=104.0,
        event_id='event-id',
        reason='RAY_NODE_OOM',
        occurred_at=101.0,
        deadline_at=221.0)


def _backend(
    context: dict[str, object] | None = None
) -> cloud_vm_ray_backend.CloudVmRayBackend:
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    backend._workload_type = 'service'  # pylint: disable=protected-access
    backend._extra_launch_context = (  # pylint: disable=protected-access
        dict(context or _context()))
    return backend


def _provisioner() -> cloud_vm_ray_backend.RetryingVmProvisioner:
    provisioner = object.__new__(cloud_vm_ray_backend.RetryingVmProvisioner)
    provisioner._extra_launch_context = _context()  # pylint: disable=protected-access
    provisioner._workload_type = 'service'  # pylint: disable=protected-access
    provisioner._active_cluster_hash = (  # pylint: disable=protected-access
        'cluster-generation-1')
    provisioner._fresh_provision_evidence_lease = None  # pylint: disable=protected-access
    return provisioner


def _bind(
    backend: cloud_vm_ray_backend.CloudVmRayBackend,
    evidence: backend_recovery.FreshProvisionEvidence
) -> backend_recovery.FreshProvisionEvidenceLease:
    generation = backend._reset_fresh_provision_evidence()  # pylint: disable=protected-access
    lease = backend_recovery.FreshProvisionEvidenceLease(evidence)
    assert backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        lease, generation)
    return lease


def _decide(backend: cloud_vm_ray_backend.CloudVmRayBackend, handle,
            task: sky.Task):
    with backend._system_oom_recovery_submission_lock:  # pylint: disable=protected-access
        return backend._consume_system_oom_recovery_plan_no_lock(  # pylint: disable=protected-access
            handle, task, 7)


@pytest.fixture(autouse=True)
def _request_and_generation(monkeypatch):
    monkeypatch.setattr(cloud_vm_ray_backend.common_utils,
                        'get_current_request_id', lambda: 'request-1')
    monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                        'get_cluster_hash_for_name',
                        lambda _: 'cluster-generation-1')
    monkeypatch.setattr(
        cloud_vm_ray_backend.global_user_state, 'get_cluster_from_name',
        lambda *_args, **_kwargs: {
            'cluster_hash': 'cluster-generation-1',
            'owner': list(_OWNER_IDENTITY),
        })
    monkeypatch.setattr(clouds.AWS, 'get_vcpus_mem_from_instance_type',
                        lambda _self, _instance_type: (4.0, 16.0))


@pytest.mark.parametrize('market_type', ['on_demand', 'spot'])
def test_exact_profile_and_handle_produce_typed_launch_plan(
        monkeypatch, market_type):
    task = _task(market_type)
    _install_profile(monkeypatch, task)
    backend = _backend(_context(market_type=market_type))
    alias = _bind(backend, _evidence(market_type=market_type))

    plan = _decide(backend, _handle(), task)

    assert plan == runtime_recovery.RecoveryLaunchPlan.owned_container(
        _owned_spec())
    assert plan.capability == runtime_recovery.CAPABILITY_V2
    assert alias.take() is None


def test_provisioner_moves_exact_record_into_one_shot_lease():
    provisioner = _provisioner()
    record = provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone='us-east-1a',
        cluster_name='provider-cluster',
        head_instance_id='i-head',
        resumed_instance_ids=[],
        created_instance_ids=['i-head'],
        fresh_aws_instance_identity=(provision_common.AWSInstanceIdentity(
            '123456789012', 'us-east-1', 'us-east-1a', 'i-head', 'g6.xlarge',
            'on_demand')))

    provisioner._record_fresh_provision_evidence(  # pylint: disable=protected-access
        record,
        _handle(),
        requested_node_count=1,
        cluster_existed=False,
        dryrun=False,
        cloud_user_identity=list(_OWNER_IDENTITY))
    lease = provisioner.release_fresh_provision_evidence_lease()

    assert lease is not None
    evidence = lease.take()
    assert evidence is not None
    assert evidence.cluster_hash == 'cluster-generation-1'
    assert evidence.created_instance_ids == ('i-head',)
    assert provisioner.release_fresh_provision_evidence_lease() is None


def test_provisioner_repeated_record_invalidates_the_first_lease():
    provisioner = _provisioner()
    record = provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone='us-east-1a',
        cluster_name='provider-cluster',
        head_instance_id='i-head',
        resumed_instance_ids=[],
        created_instance_ids=['i-head'],
        fresh_aws_instance_identity=(provision_common.AWSInstanceIdentity(
            '123456789012', 'us-east-1', 'us-east-1a', 'i-head', 'g6.xlarge',
            'on_demand')))

    provisioner._record_fresh_provision_evidence(  # pylint: disable=protected-access
        record,
        _handle(),
        requested_node_count=1,
        cluster_existed=False,
        dryrun=False,
        cloud_user_identity=list(_OWNER_IDENTITY))
    first_alias = provisioner._fresh_provision_evidence_lease  # pylint: disable=protected-access
    assert first_alias is not None

    provisioner._record_fresh_provision_evidence(  # pylint: disable=protected-access
        record,
        _handle(),
        requested_node_count=1,
        cluster_existed=False,
        dryrun=False,
        cloud_user_identity=list(_OWNER_IDENTITY))

    assert first_alias.take() is None
    assert provisioner.release_fresh_provision_evidence_lease() is not None


def test_optional_evidence_failure_does_not_fail_provisioning(monkeypatch):
    provisioner = _provisioner()
    record = provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone='us-east-1a',
        cluster_name='provider-cluster',
        head_instance_id='i-head',
        resumed_instance_ids=[],
        created_instance_ids=['i-head'],
        fresh_aws_instance_identity=(provision_common.AWSInstanceIdentity(
            '123456789012', 'us-east-1', 'us-east-1a', 'i-head', 'g6.xlarge',
            'on_demand')))
    monkeypatch.setattr(
        backend_recovery.FreshProvisionEvidence, 'from_provision_record',
        mock.MagicMock(side_effect=RuntimeError('optional evidence failure')))

    provisioner._record_fresh_provision_evidence(  # pylint: disable=protected-access
        record,
        _handle(),
        requested_node_count=1,
        cluster_existed=False,
        dryrun=False,
        cloud_user_identity=list(_OWNER_IDENTITY))

    assert provisioner.release_fresh_provision_evidence_lease() is None


@pytest.mark.parametrize('failure_kind',
                         ['existing', 'resumed', 'partial', 'dryrun', 'legacy'])
def test_provisioner_does_not_create_ambiguous_evidence(failure_kind):
    provisioner = _provisioner()
    created_ids = [] if failure_kind == 'partial' else ['i-head']
    resumed_ids = ['i-head'] if failure_kind == 'resumed' else []
    record = provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone='us-east-1a',
        cluster_name='provider-cluster',
        head_instance_id='i-head',
        resumed_instance_ids=resumed_ids,
        created_instance_ids=created_ids,
        fresh_aws_instance_identity=(provision_common.AWSInstanceIdentity(
            '123456789012', 'us-east-1', 'us-east-1a', 'i-head', 'g6.xlarge',
            'on_demand')))
    if failure_kind == 'legacy':
        provisioner._workload_type = 'cluster'  # pylint: disable=protected-access

    provisioner._record_fresh_provision_evidence(  # pylint: disable=protected-access
        record,
        _handle(),
        requested_node_count=1,
        cluster_existed=failure_kind == 'existing',
        dryrun=failure_kind == 'dryrun',
        cloud_user_identity=list(_OWNER_IDENTITY))

    assert provisioner.release_fresh_provision_evidence_lease() is None


def test_first_submission_decision_does_not_assume_job_id_one(monkeypatch):
    task = _task()
    _install_profile(monkeypatch, task)
    backend = _backend()
    handle = _handle()
    handle.provision_runtime_metadata.run_started = False
    _bind(backend, _evidence())
    monkeypatch.setattr(backend, 'check_resources_fit_cluster',
                        mock.MagicMock(return_value=next(iter(task.resources))))
    monkeypatch.setattr(backend, '_add_job',
                        mock.MagicMock(return_value=(73, '/remote/logs')))
    execute_one = mock.MagicMock()
    monkeypatch.setattr(backend, '_execute_task_one_node', execute_one)
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'get_task_resources_str', lambda _: 'resources')

    assert backend._execute(handle, task) == 73  # pylint: disable=protected-access
    assert execute_one.call_args.args[2] == 73
    assert (execute_one.call_args.kwargs['recovery_plan'] ==
            runtime_recovery.RecoveryLaunchPlan.owned_container(_owned_spec()))


def test_concurrent_submission_decisions_share_one_consumption(monkeypatch):
    task = _task()
    _install_profile(monkeypatch, task)
    backend = _backend()
    handle = _handle()
    _bind(backend, _evidence())

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(lambda _: _decide(backend, handle, task), range(2)))

    assert results.count(
        runtime_recovery.RecoveryLaunchPlan.owned_container(_owned_spec())) == 1
    assert results.count(None) == 1


@pytest.mark.parametrize('failure_kind', [
    'absent_contract',
    'no_profile',
    'pool',
    'multi_node_task',
    'multi_node_handle',
    'multiple_futures',
    'kubernetes',
    'slurm',
    'managed_job',
    'no_ray',
    'no_job_queue',
])
def test_every_ineligible_submission_consumes_the_lease(monkeypatch,
                                                        failure_kind):
    task = _task()
    _install_profile(monkeypatch, task)
    context = _context(include_contract=failure_kind != 'absent_contract')
    backend = _backend(context)
    handle = _handle()
    if failure_kind == 'no_profile':
        monkeypatch.delenv(serve_constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR)
    elif failure_kind == 'pool':
        backend._workload_type = 'pool'  # pylint: disable=protected-access
    elif failure_kind == 'multi_node_task':
        task.num_nodes = 2
    elif failure_kind == 'multi_node_handle':
        handle.launched_nodes = 2
    elif failure_kind == 'multiple_futures':
        handle.num_ips_per_node = 2
    elif failure_kind == 'kubernetes':
        handle.launched_resources.cloud = clouds.Kubernetes()
    elif failure_kind == 'slurm':
        handle.launched_resources.cloud = clouds.Slurm()
    elif failure_kind == 'managed_job':
        task.managed_job_dag = mock.MagicMock()
    elif failure_kind == 'no_ray':
        handle.provision_runtime_metadata.has_ray = False
    elif failure_kind == 'no_job_queue':
        handle.provision_runtime_metadata.has_job_queue = False
    alias = _bind(backend, _evidence())

    assert _decide(backend, handle, task) is None
    assert alias.take() is None


def test_matcher_exception_consumes_lease_and_fails_closed(monkeypatch):
    task = _task()
    backend = _backend()
    alias = _bind(backend, _evidence())
    monkeypatch.setattr(serve_recovery, 'match_trusted_profile',
                        mock.MagicMock(side_effect=RuntimeError('malformed')))

    assert _decide(backend, _handle(), task) is None
    assert alias.take() is None


def test_missing_v3_evidence_records_bounded_evidence_loss(monkeypatch):
    backend = _backend()
    record = mock.MagicMock()
    monkeypatch.setattr(cloud_vm_ray_backend.system_oom_recovery_observability,
                        'record', record)

    assert _decide(backend, _handle(), _task()) is None
    record.assert_called_once_with('evidence_lost',
                                   provider='aws',
                                   market='unknown')


def test_actual_catalog_memory_above_envelope_is_ordinary(monkeypatch):
    task = _task()
    _install_profile(monkeypatch, task)
    backend = _backend()
    record = mock.MagicMock()
    monkeypatch.setattr(cloud_vm_ray_backend.system_oom_recovery_observability,
                        'record', record)
    monkeypatch.setattr(clouds.AWS, 'get_vcpus_mem_from_instance_type',
                        lambda _self, _instance_type: (4.0, 32.0))
    alias = _bind(backend, _evidence(catalog_memory_gib=32.0))

    assert _decide(backend, _handle(), task) is None
    assert alias.take() is None
    record.assert_called_once_with('authorization_v3_ordinary',
                                   provider='aws',
                                   market='on_demand')


@pytest.mark.parametrize('changed_evidence', [
    _evidence(request_id='other-request'),
    _evidence(workspace='other-workspace'),
    _evidence(cluster_name='other-cluster'),
    _evidence(cluster_name_on_cloud='other-provider-cluster'),
    _evidence(cluster_hash='other-generation'),
    _evidence(aws_account_id='999999999999',
              provision_owner_identity=('other-user', '999999999999')),
    _evidence(provision_owner_identity=('other-user', '123456789012')),
    _evidence(region='us-west-2'),
    _evidence(instance_type='g5.xlarge'),
    _evidence(head_instance_id='i-worker', created_instance_ids=('i-worker',)),
    _evidence(service_name='other-service'),
    _evidence(service_hash='other-service-hash'),
])
def test_every_evidence_identity_is_revalidated(monkeypatch, changed_evidence):
    task = _task()
    _install_profile(monkeypatch, task)
    backend = _backend()
    alias = _bind(backend, changed_evidence)

    assert _decide(backend, _handle(), task) is None
    assert alias.take() is None


def test_persisted_cluster_owner_must_match_provision_owner(monkeypatch):
    task = _task()
    _install_profile(monkeypatch, task)
    backend = _backend()
    alias = _bind(backend, _evidence())
    monkeypatch.setattr(
        cloud_vm_ray_backend.global_user_state, 'get_cluster_from_name',
        lambda *_args, **_kwargs: {
            'cluster_hash': 'cluster-generation-1',
            'owner': ['different-user', '123456789012'],
        })

    assert _decide(backend, _handle(), task) is None
    assert alias.take() is None


def test_full_provider_instance_inventory_is_revalidated(monkeypatch):
    task = _task()
    _install_profile(monkeypatch, task)
    backend = _backend()
    alias = _bind(backend, _evidence())
    rebound = _handle(cluster_info=_cluster_info(
        head_instance_id='i-head', instance_ids=('i-head', 'i-extra')))

    assert _decide(backend, rebound, task) is None
    assert alias.take() is None


def test_reset_register_and_stale_rebind_invalidate_all_aliases():
    backend = _backend()

    first_generation = backend._reset_fresh_provision_evidence()  # pylint: disable=protected-access
    first = backend_recovery.FreshProvisionEvidenceLease(_evidence())
    assert backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        first, first_generation)
    second_generation = backend._reset_fresh_provision_evidence()  # pylint: disable=protected-access
    assert first.take() is None

    stale = backend_recovery.FreshProvisionEvidenceLease(_evidence())
    assert not backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        stale, first_generation)
    assert stale.take() is None

    current = backend_recovery.FreshProvisionEvidenceLease(_evidence())
    assert backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        current, second_generation)
    duplicate = backend_recovery.FreshProvisionEvidenceLease(_evidence())
    assert not backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        duplicate, second_generation)
    assert duplicate.take() is None
    assert current.take() is None

    backend.register_info()
    assert current.take() is None


def test_same_lease_cannot_be_rebound_without_invalidating_it():
    backend = _backend()
    generation = backend._reset_fresh_provision_evidence()  # pylint: disable=protected-access
    lease = backend_recovery.FreshProvisionEvidenceLease(_evidence())
    assert backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        lease, generation)

    assert not backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
        lease, generation)
    assert lease.take() is None


def test_raw_evidence_is_not_accepted_as_a_lease():
    backend = _backend()
    generation = backend._reset_fresh_provision_evidence()  # pylint: disable=protected-access

    with pytest.raises(TypeError):
        backend._bind_fresh_provision_evidence(  # pylint: disable=protected-access
            _evidence(), generation)


def test_structured_job_status_round_trips_over_grpc():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=True)
    info = _recovery_info()
    client = mock.MagicMock()
    client.get_job_status.return_value = jobsv1_pb2.GetJobStatusResponse(
        job_statuses={7: jobsv1_pb2.JOB_STATUS_RUNNING},
        system_recovery_infos={7: info.to_protobuf()},
        system_recovery_detail_statuses={
            7: jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_PRESENT
        })

    with mock.patch.object(cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), mock.patch.object(
                               cloud_vm_ray_backend.backend_utils,
                               'invoke_skylet_with_retries',
                               side_effect=lambda callback: callback()):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert infos == {7: info}
    assert detail_statuses == {7: job_lib.JobSystemRecoveryDetailStatus.PRESENT}


def test_old_grpc_response_preserves_status_and_is_malformed():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=True)
    client = mock.MagicMock()
    client.get_job_status.return_value = jobsv1_pb2.GetJobStatusResponse(
        job_statuses={7: jobsv1_pb2.JOB_STATUS_RUNNING})

    with mock.patch.object(cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), mock.patch.object(
                               cloud_vm_ray_backend.backend_utils,
                               'invoke_skylet_with_retries',
                               side_effect=lambda callback: callback()):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert not infos
    assert detail_statuses == {
        7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_new_grpc_response_reports_positive_absence():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=True)
    client = mock.MagicMock()
    client.get_job_status.return_value = jobsv1_pb2.GetJobStatusResponse(
        job_statuses={7: jobsv1_pb2.JOB_STATUS_RUNNING},
        system_recovery_detail_statuses={
            7: jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_ABSENT
        })

    with mock.patch.object(cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), mock.patch.object(
                               cloud_vm_ray_backend.backend_utils,
                               'invoke_skylet_with_retries',
                               side_effect=lambda callback: callback()):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert not infos
    assert detail_statuses == {7: job_lib.JobSystemRecoveryDetailStatus.ABSENT}


@pytest.mark.parametrize('detail_status', [
    jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_UNSPECIFIED,
    99,
])
def test_zero_or_unknown_grpc_detail_status_is_malformed(detail_status):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=True)
    client = mock.MagicMock()
    client.get_job_status.return_value = jobsv1_pb2.GetJobStatusResponse(
        job_statuses={7: jobsv1_pb2.JOB_STATUS_RUNNING},
        system_recovery_detail_statuses={7: detail_status})

    with mock.patch.object(cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), mock.patch.object(
                               cloud_vm_ray_backend.backend_utils,
                               'invoke_skylet_with_retries',
                               side_effect=lambda callback: callback()):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert not infos
    assert detail_statuses == {
        7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_malformed_grpc_detail_does_not_hide_ordinary_status():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=True)
    client = mock.MagicMock()
    client.get_job_status.return_value = jobsv1_pb2.GetJobStatusResponse(
        job_statuses={7: jobsv1_pb2.JOB_STATUS_RUNNING},
        system_recovery_detail_statuses={
            7: jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_PRESENT
        },
        system_recovery_infos={
            7: jobsv1_pb2.JobSystemRecoveryInfo(
                capability=runtime_recovery.CAPABILITY_V2,
                phase=jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_UNSPECIFIED,
                original_attempt_id='attempt-original',
                node_boot_id='boot-id',
                armed_at=100.0,
                updated_at=100.0)
        })

    with mock.patch.object(cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), mock.patch.object(
                               cloud_vm_ray_backend.backend_utils,
                               'invoke_skylet_with_retries',
                               side_effect=lambda callback: callback()):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert not infos
    assert detail_statuses == {
        7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


@pytest.mark.parametrize('legacy_payload', [False, True])
def test_structured_job_status_round_trips_over_ssh(legacy_payload):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=False)
    info = _recovery_info()
    if legacy_payload:
        payload = message_utils.encode_payload({7: 'RUNNING'})
        expected_infos = {}
        expected_detail_statuses = {
            7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
        }
    else:
        payload = message_utils.encode_payload({
            'version': 1,
            'job_statuses': {
                7: 'RUNNING'
            },
            'system_recovery_infos': {
                7: info.to_dict()
            },
            'system_recovery_detail_statuses': {
                7: job_lib.JobSystemRecoveryDetailStatus.PRESENT.value
            },
        })
        expected_infos = {7: info}
        expected_detail_statuses = {
            7: job_lib.JobSystemRecoveryDetailStatus.PRESENT
        }

    with mock.patch.object(backend,
                           'run_on_head',
                           return_value=(0, payload, '')):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert infos == expected_infos
    assert detail_statuses == expected_detail_statuses


def test_structured_grpc_failure_falls_back_to_status_only_ssh():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=True)
    payload = message_utils.encode_payload({7: 'RUNNING'})

    with mock.patch.object(
            cloud_vm_ray_backend.backend_utils,
            'invoke_skylet_with_retries',
            side_effect=cloud_vm_ray_backend.exceptions.SkyletUnavailableError(
                'unavailable')), mock.patch.object(backend,
                                                   'run_on_head',
                                                   return_value=(0, payload,
                                                                 '')):
        statuses, infos, detail_statuses = (
            backend.get_job_status_with_system_recovery(handle, [7],
                                                        stream_logs=False))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert not infos
    assert detail_statuses == {
        7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_ordinary_job_status_does_not_depend_on_recovery_detail_path():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(is_grpc_enabled_with_flag=False)
    payload = message_utils.encode_payload({7: 'RUNNING'})
    detail_path = mock.MagicMock(side_effect=RuntimeError('detail failed'))

    with mock.patch.object(backend, 'get_job_status_with_system_recovery',
                           detail_path), mock.patch.object(
                               backend,
                               'run_on_head',
                               return_value=(0, payload, '')):
        statuses = backend.get_job_status(handle, [7], stream_logs=False)

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    detail_path.assert_not_called()
