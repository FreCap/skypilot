"""Failure, fencing, and recovery tests for independently deployed workers."""
# pylint: disable=protected-access

from __future__ import annotations

import base64
import contextlib
import json
import socket
from types import SimpleNamespace
from unittest import mock
import urllib.error
import urllib.request

import pytest

from sky.container_images import aws
from sky.container_images import canary_worker_service
from sky.container_images import catalog_state
from sky.container_images import copy_worker_service
from sky.container_images import demand_state
from sky.container_images import lifecycle_worker_service
from sky.container_images import models
from sky.container_images import topology_state
from sky.container_images import worker_health

_DIGEST = 'sha256:' + 'a' * 64
_CONFIG_DIGEST = 'sha256:' + 'b' * 64
_ARTIFACT_ID = '00000000-0000-4000-8000-000000000001'
_LOCATION_ID = '00000000-0000-4000-8000-000000000002'
_SHARD_ID = '00000000-0000-4000-8000-000000000003'
_REVISION_ID = '00000000-0000-4000-8000-000000000004'


def _cluster_demand(
        *,
        created_at: int,
        first_terminal_at: int | None = None) -> demand_state.DemandRecord:
    return demand_state.DemandRecord(
        id='00000000-0000-4000-8000-000000000005',
        authority_id='00000000-0000-4000-8000-000000000006',
        workspace='research',
        consumer_kind='cluster',
        consumer_owner='orphan-cluster:incarnation:owner-hash',
        request_id='request-id',
        consumer_generation=0,
        target_key='artifact:target',
        owner_epoch=1,
        retry_epoch=0,
        image_id=_ARTIFACT_ID,
        runtime_digest=_DIGEST,
        profile_revision_id=_REVISION_ID,
        target_fingerprint='f' * 64,
        location_id=_LOCATION_ID,
        placement={
            'consumer': {
                'workload_id': 'orphan-cluster',
                'request_id': 'request-id',
            }
        },
        pull_plan=None,
        state=models.ImageDemandState.WARMING,
        error_code=None,
        consumer_attached=False,
        first_terminal_observed_at=first_terminal_at,
        last_terminal_observed_at=first_terminal_at,
        terminal_observation_count=1 if first_terminal_at is not None else 0,
        terminal_at=None,
        expires_at=None,
        created_at=created_at,
        updated_at=created_at)


def test_terminal_unattached_cluster_is_reconciled_after_bounded_retention(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(
        created_at=current -
        lifecycle_worker_service._UNATTACHED_REQUEST_RETENTION_SECONDS,
        first_terminal_at=current - 3600)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock()
    reconcile = mock.Mock(return_value=False)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_not_called()
    reconcile.assert_called_once_with(demand, 'orphan-cluster', current)


def test_unattached_cluster_demand_is_not_released_early(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(
        created_at=current -
        lifecycle_worker_service._UNATTACHED_REQUEST_RETENTION_SECONDS + 1,
        first_terminal_at=current - 1)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock(return_value=True)
    reconcile = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_called_once_with(demand.id, now=current)
    reconcile.assert_not_called()


def test_old_unattached_cluster_without_terminal_request_proof_is_retained(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(
        created_at=current -
        lifecycle_worker_service._UNATTACHED_REQUEST_RETENTION_SECONDS)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock(return_value=True)
    reconcile = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_called_once_with(demand.id, now=current)
    reconcile.assert_not_called()


@pytest.mark.parametrize('binding', [
    ('cluster', 'orphan-cluster:incarnation:owner-hash'),
    None,
])
def test_current_or_indeterminate_cluster_binding_is_never_inferred_terminal(
        monkeypatch: pytest.MonkeyPatch,
        binding: tuple[str | None, str | None] | None) -> None:
    current = 200_000
    demand = _cluster_demand(created_at=current - 100_000)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers',
                        lambda _: {'orphan-cluster': binding})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock(return_value=True)
    reconcile = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_called_once_with(demand.id, now=current)
    reconcile.assert_not_called()


def test_known_absent_binding_does_not_mask_old_cluster_incarnation(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(created_at=current - 100_000,
                             first_terminal_at=current - 3600)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers',
                        lambda _: {'orphan-cluster': (None, None)})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock()
    reconcile = mock.Mock(return_value=False)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_not_called()
    reconcile.assert_called_once_with(demand, 'orphan-cluster', current)


def test_cluster_terminal_confirmation_uses_locked_reconciliation(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(created_at=current - 100_000,
                             first_terminal_at=current - 1)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    clear = mock.Mock()
    reconcile = mock.Mock(return_value=False)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', clear)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    clear.assert_not_called()
    reconcile.assert_called_once_with(demand, 'orphan-cluster', current)


@pytest.mark.parametrize(
    ('message', 'expected'),
    [('AccessDenied: arn:aws:iam::123:role/secret', 'CANARY_FAILED'),
     ('CANARY_TIMEOUT', 'CANARY_TIMEOUT')])
def test_canary_persists_only_closed_error_codes(
        monkeypatch: pytest.MonkeyPatch, message: str, expected: str) -> None:
    operation = mock.sentinel.operation
    monkeypatch.setattr(canary_worker_service, '_load_contract',
                        mock.Mock(side_effect=ValueError(message)))
    failed = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification, 'fail_canary',
                        failed)

    assert not canary_worker_service.run_canary(operation)
    failed.assert_called_once_with(operation, expected)


def _eks_node(uid: str, instance_id: str, *, selector_value: str = 'eks-node'):
    return SimpleNamespace(metadata=SimpleNamespace(
        uid=uid, labels={'skypilot.co/image-pull-role': selector_value}),
                           spec=SimpleNamespace(
                               provider_id=f'aws:///us-west-2a/{instance_id}',
                               unschedulable=False))


def test_eks_qualification_proves_every_selected_node_role(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    core = mock.Mock()
    core.list_node.return_value = SimpleNamespace(
        items=[_eks_node('node-a', 'i-a'),
               _eks_node('node-b', 'i-b')],
        metadata=SimpleNamespace(_continue=None))
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-a',
                'IamInstanceProfile': {
                    'Arn': 'arn:aws:iam::123:instance-profile/profile-a'
                },
            }, {
                'InstanceId': 'i-b',
                'IamInstanceProfile': {
                    'Arn': 'arn:aws:iam::123:instance-profile/profile-b'
                },
            }]
        }]
    }
    iam = mock.Mock()
    monkeypatch.setattr(
        canary_worker_service.aws, 'assumed_client',
        lambda _role, service, _region: ec2 if service == 'ec2' else iam)
    monkeypatch.setattr(canary_worker_service, '_instance_profile_role',
                        lambda _iam, _name: qualified.node_role)

    count, node_set_hash = canary_worker_service._qualified_eks_nodes(
        core, mock.sentinel.role, target, qualified)

    assert count == 2
    assert len(node_set_hash) == 64
    core.list_node.assert_called_once_with(
        label_selector='skypilot.co/image-pull-role=eks-node',
        limit=canary_worker_service._MAX_QUALIFIED_EKS_NODES + 1,
        _request_timeout=canary_worker_service.kubernetes.API_TIMEOUT)


def test_eks_qualification_rejects_heterogeneous_selected_node_roles(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    core = mock.Mock()
    core.list_node.return_value = SimpleNamespace(
        items=[_eks_node('node-a', 'i-a'),
               _eks_node('node-b', 'i-b')],
        metadata=SimpleNamespace(_continue=None))
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'InstanceId': instance_id,
                'IamInstanceProfile': {
                    'Arn': f'arn:aws:iam::123:instance-profile/{profile_name}'
                },
            } for instance_id, profile_name in [('i-a',
                                                 'qualified'), ('i-b', 'other')]
                         ]
        }]
    }
    monkeypatch.setattr(
        canary_worker_service.aws, 'assumed_client',
        lambda _role, service, _region: ec2
        if service == 'ec2' else mock.Mock())
    monkeypatch.setattr(
        canary_worker_service, '_instance_profile_role',
        lambda _iam, name: qualified.node_role
        if name == 'qualified' else 'arn:aws:iam::123:role/OtherNodeRole')

    with pytest.raises(ValueError,
                       match='QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED'):
        canary_worker_service._qualified_eks_nodes(core, mock.sentinel.role,
                                                   target, qualified)


def _artifact() -> catalog_state.ArtifactRecord:
    return catalog_state.ArtifactRecord(
        id=_ARTIFACT_ID,
        workspace='research',
        runtime_digest=_DIGEST,
        platform='linux/amd64',
        config_digest=_CONFIG_DIGEST,
        manifest_media_type='application/vnd.oci.image.manifest.v1+json',
        manifest_size_bytes=100,
        declared_size_bytes=1000,
        creator_user_hash='actor',
        producer_kind='external_oci',
        producer_spec_hash=None,
        builder_version=None,
        created_at=10,
        updated_at=11)


def _shard(
        profile: models.ManagedRegistryProfile) -> topology_state.ShardRecord:
    target = profile.target('aws-us-west-2')
    return topology_state.ShardRecord(
        id=_SHARD_ID,
        workspace='research',
        profile=profile.name,
        target_id=target.name,
        provider='aws',
        partition='aws',
        account=profile.registry_account,
        region=target.region,
        shard_generation=0,
        shard_index=0,
        physical_fingerprint='e' * 64,
        registry=target.registry,
        repository_name='skypilot/images/west/test/s00',
        repository_arn=(
            f'arn:aws:ecr:{target.region}:{profile.registry_account}:'
            'repository/skypilot/images/west/test/s00'),
        max_manifests=100,
        max_declared_bytes=10_000,
        reserved_manifests=1,
        reserved_declared_bytes=1000,
        observed_manifests=0,
        max_in_flight=4,
        in_flight=1,
        state=models.ImageShardState.READY,
        qualified_at=10,
        last_dispatch_at=11,
        inventory_epoch=0,
        inventory_cursor=None,
        inventory_started_at=None,
        inventory_completed_at=None,
        inventory_lease_token=None,
        inventory_lease_expires_at=None,
        created_at=10,
        updated_at=11)


def _revision(
    profile: models.ManagedRegistryProfile
) -> topology_state.ProfileRevisionRecord:
    return topology_state.ProfileRevisionRecord(
        id=_REVISION_ID,
        workspace='research',
        profile=profile.name,
        revision=profile.revision,
        desired_generation=1,
        state=models.ImageProfileState.ACTIVE,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        terraform_hash='c' * 64,
        physical_manifest_hash=profile.physical_manifest_hash,
        attestations={},
        attestations_hash='d' * 64,
        qualified_at=10,
        failed_code=None,
        canary_window_day=None,
        canary_reserved_microusd=0,
        max_daily_canary_microusd=5_000_000,
        created_at=10,
        updated_at=11)


def _copying_location(
        profile: models.ManagedRegistryProfile
) -> topology_state.LocationRecord:
    target = profile.target('aws-us-west-2')
    return topology_state.LocationRecord(
        id=_LOCATION_ID,
        workspace='research',
        image_id=_ARTIFACT_ID,
        shard_id=_SHARD_ID,
        target_fingerprint=target.target_fingerprint,
        physical_fingerprint='e' * 64,
        runtime_digest=_DIGEST,
        canonical=False,
        canonical_location_id='00000000-0000-4000-8000-000000000007',
        target_ref=(f'{target.registry}/skypilot/images/west/test/s00@'
                    f'{_DIGEST}'),
        state=models.ImageLocationState.COPYING,
        lease_kind='copy',
        lease_token='lease-token',
        lease_expires_at=1000,
        attempt_count=1,
        next_retry_at=None,
        error_code=None,
        last_verified_at=None,
        last_used_at=None,
        inventory_epoch_seen=None,
        reserved_declared_bytes=1000,
        created_at=10,
        updated_at=11)


class _OwnedHeartbeat(contextlib.AbstractContextManager['_OwnedHeartbeat']):
    """Deterministic lease-heartbeat double that remains owned."""
    cancel_event = mock.sentinel.cancel_event

    def __init__(self, *_: object) -> None:
        pass

    def __enter__(self) -> _OwnedHeartbeat:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def assert_owned(self) -> None:
        return None


def test_ambiguous_copy_is_verified_before_ready(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = _copying_location(profile)
    destination = mock.Mock()
    destination.copy_graph.return_value = aws.CopyOutcome.AMBIGUOUS
    destination.verify_graph.return_value = True
    graph = SimpleNamespace(runtime_digest=_DIGEST,
                            config=SimpleNamespace(digest=_CONFIG_DIGEST),
                            platform='linux/amd64')
    monkeypatch.setattr(copy_worker_service.catalog_state, 'get_artifact',
                        lambda *_: _artifact())
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_profile_revisions',
                        lambda _: [_revision(profile)])
    transition = mock.Mock(return_value=True)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'transition_location_to_verifying', transition)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'heartbeat_location', lambda *_: True)
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: destination)
    monkeypatch.setattr(copy_worker_service, '_graph_for_location',
                        lambda *_args: (graph, mock.sentinel.read_blob))
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', _OwnedHeartbeat)
    converge = mock.Mock()
    monkeypatch.setattr(copy_worker_service.transactions, 'converge_canonical',
                        converge)

    copied = copy_worker_service.copy_location(location,
                                               limiter=mock.Mock(),
                                               lease_seconds=30)
    assert copied, (destination.mock_calls, transition.mock_calls,
                    converge.mock_calls)
    destination.copy_graph.assert_called_once_with(graph,
                                                   mock.sentinel.read_blob,
                                                   mock.sentinel.cancel_event)
    transition.assert_called_once_with(location.id,
                                       location.lease_token,
                                       ambiguous=True)
    destination.verify_graph.assert_called_once_with(graph)
    converge.assert_called_once_with(location_id=location.id,
                                     lease_token=location.lease_token,
                                     ready=True,
                                     error_code=None,
                                     retry_at=None,
                                     terminal=False)


def test_lost_copy_lease_cannot_mark_location_ready(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = _copying_location(profile)
    destination = mock.Mock()
    destination.copy_graph.return_value = aws.CopyOutcome.WRITTEN
    graph = SimpleNamespace(runtime_digest=_DIGEST,
                            config=SimpleNamespace(digest=_CONFIG_DIGEST),
                            platform='linux/amd64')
    monkeypatch.setattr(copy_worker_service.catalog_state, 'get_artifact',
                        lambda *_: _artifact())
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_profile_revisions',
                        lambda _: [_revision(profile)])
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'transition_location_to_verifying',
                        mock.Mock(return_value=False))
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: destination)
    monkeypatch.setattr(copy_worker_service, '_graph_for_location',
                        lambda *_args: (graph, mock.sentinel.read_blob))
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', _OwnedHeartbeat)
    converge = mock.Mock(side_effect=topology_state.LocationLeaseLostError)
    monkeypatch.setattr(copy_worker_service.transactions, 'converge_canonical',
                        converge)

    assert not copy_worker_service.copy_location(
        location, limiter=mock.Mock(), lease_seconds=30)
    destination.verify_graph.assert_not_called()
    assert all(
        call.kwargs.get('ready') is False for call in converge.call_args_list)


def test_copy_maintenance_also_recovers_pending_publication_fanout(
        monkeypatch: pytest.MonkeyPatch) -> None:
    reconcile_fanout = mock.Mock(return_value=3)
    reconcile_profiles = mock.Mock()
    schedule_canaries = mock.Mock()
    monkeypatch.setattr(copy_worker_service.transactions,
                        'reconcile_pending_canonical_publications',
                        reconcile_fanout)
    monkeypatch.setattr(copy_worker_service, 'reconcile_qualification_profiles',
                        reconcile_profiles)
    monkeypatch.setattr(copy_worker_service.qualification,
                        'schedule_automatic_canaries', schedule_canaries)
    limiter = mock.Mock()

    assert copy_worker_service._qualification_maintenance(limiter)

    reconcile_fanout.assert_called_once_with()
    reconcile_profiles.assert_called_once_with(limiter)
    schedule_canaries.assert_called_once_with()


def _dockerconfig_binding() -> models.RegistryAccessBinding:
    return models.RegistryAccessBinding(
        id='source-secret',
        kind=(models.RegistryAccessBindingKind.KUBERNETES_DOCKERCONFIG_SECRET),
        purposes=('source_read',),
        reference={
            'namespace': 'image-sources',
            'name': 'ghcr',
            'key': '.dockerconfigjson',
        })


def test_source_secret_allowlist_blocks_ambient_rbac(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('SKYPILOT_IMAGE_SOURCE_SECRET_ALLOWLIST', '[]')
    core_api = mock.Mock()
    monkeypatch.setattr(copy_worker_service.kubernetes, 'core_api',
                        lambda: core_api)

    with pytest.raises(ValueError, match='AUTH_BINDING_UNAVAILABLE'):
        copy_worker_service._docker_config_credentials(_dockerconfig_binding(),
                                                       'ghcr.io')
    core_api.read_namespaced_secret.assert_not_called()


def test_source_secret_allowlist_reads_only_the_exact_secret(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        'SKYPILOT_IMAGE_SOURCE_SECRET_ALLOWLIST',
        json.dumps([{
            'namespace': 'image-sources',
            'name': 'ghcr'
        }]))
    payload = base64.b64encode(
        json.dumps({
            'auths': {
                'https://ghcr.io': {
                    'username': 'robot',
                    'password': 'token',
                }
            }
        }).encode()).decode()
    core_api = mock.Mock()
    core_api.read_namespaced_secret.return_value = SimpleNamespace(
        data={'.dockerconfigjson': payload})
    monkeypatch.setattr(copy_worker_service.kubernetes, 'core_api',
                        lambda: core_api)

    credentials = copy_worker_service._docker_config_credentials(
        _dockerconfig_binding(), 'ghcr.io')

    assert credentials.username == 'robot'
    assert credentials.password == 'token'
    core_api.read_namespaced_secret.assert_called_once_with(
        'ghcr',
        'image-sources',
        _request_timeout=copy_worker_service.kubernetes.API_TIMEOUT)


def test_worker_health_requires_heartbeat_and_detects_stalled_loop(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = {'value': 100.0}
    monkeypatch.setattr(worker_health.time, 'monotonic',
                        lambda: current['value'])
    health = worker_health.WorkerHealth('copy', liveness_deadline_seconds=30)
    assert health.snapshot().live
    assert not health.snapshot().ready
    health.registered()
    health.heartbeat(True)
    assert health.snapshot().ready
    current['value'] = 131.0
    assert not health.snapshot().live
    assert not health.snapshot().ready


def test_worker_health_http_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    current = {'value': 100.0}
    monkeypatch.setattr(worker_health.time, 'monotonic',
                        lambda: current['value'])
    health = worker_health.WorkerHealth('copy', liveness_deadline_seconds=30)
    health.registered()
    health.heartbeat(True)
    with socket.socket() as candidate:
        candidate.bind(('127.0.0.1', 0))
        port = candidate.getsockname()[1]
    health_server = worker_health.HealthServer(health, port)
    health_server.start()
    try:
        with urllib.request.urlopen(  # nosec B310
                f'http://127.0.0.1:{port}/ready', timeout=2) as response:
            assert response.status == 200
        metrics = urllib.request.urlopen(  # nosec B310
            f'http://127.0.0.1:{port}/metrics', timeout=2).read().decode()
        assert 'skypilot_image_worker_ready{kind="copy"} 1' in metrics
        current['value'] = 131.0
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(  # nosec B310
                f'http://127.0.0.1:{port}/live', timeout=2)
        assert error.value.code == 503
    finally:
        health_server.stop()
