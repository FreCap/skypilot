"""Failure, fencing, and recovery tests for independently deployed workers."""
# pylint: disable=protected-access

from __future__ import annotations

import base64
import contextlib
import dataclasses
import json
import socket
import threading
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
from sky.container_images import transactions
from sky.container_images import worker_health
from sky.container_images import worker_lease

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


def test_lifecycle_policy_refresh_keeps_last_valid_cutoffs(
        monkeypatch: pytest.MonkeyPatch) -> None:
    previous = {'research': 123, 'retained': None}
    reload_config = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.skypilot_config,
                        'safe_reload_config', reload_config)
    monkeypatch.setattr(
        lifecycle_worker_service.config, 'list_workspace_policies',
        mock.Mock(side_effect=ValueError('malformed workspace policy')))

    refreshed = lifecycle_worker_service._refresh_workspace_eviction_cutoffs(
        200, previous)
    startup = lifecycle_worker_service._refresh_workspace_eviction_cutoffs(
        200, None)

    assert refreshed is previous
    assert startup is None
    assert reload_config.call_count == 2


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
    profile: models.ManagedRegistryProfile,
    target_name: str = 'aws-us-west-2',
    shard_id: str = _SHARD_ID,
) -> topology_state.ShardRecord:
    target = profile.target(target_name)
    return topology_state.ShardRecord(
        id=shard_id,
        workspace='research',
        profile=profile.name,
        profile_revision_id=_REVISION_ID,
        target_id=target.name,
        provider='aws',
        partition='aws',
        account=profile.registry_account,
        region=target.region,
        shard_generation=0,
        shard_index=0,
        target_fingerprint=target.target_fingerprint,
        physical_fingerprint='e' * 64,
        eviction_enabled=(target.delete_authority is not None),
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


def _canonical_location(
    profile: models.ManagedRegistryProfile,) -> topology_state.LocationRecord:
    target = profile.canonical
    digest_ref = f'{target.registry}/skypilot/images/canonical/test/s00@{_DIGEST}'
    return topology_state.LocationRecord(
        id='00000000-0000-4000-8000-000000000007',
        workspace='research',
        image_id=_ARTIFACT_ID,
        shard_id='00000000-0000-4000-8000-000000000008',
        target_fingerprint=target.target_fingerprint,
        physical_fingerprint='f' * 64,
        runtime_digest=_DIGEST,
        canonical=True,
        canonical_location_id=None,
        target_ref=digest_ref,
        state=models.ImageLocationState.READY,
        lease_kind=None,
        lease_token=None,
        lease_expires_at=None,
        attempt_count=1,
        next_retry_at=None,
        error_code=None,
        last_verified_at=10,
        last_used_at=None,
        inventory_epoch_seen=None,
        reserved_declared_bytes=1000,
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


def _policy_profile(
    profile: models.ManagedRegistryProfile,) -> models.ManagedRegistryProfile:
    target = profile.target('aws-us-west-2')
    bindings = tuple(
        dataclasses.replace(binding,
                            authority=(
                                f'arn:aws:iam::{profile.registry_account}:'
                                'role/SkyPilotImageCopyV2')) if binding.id ==
        target.write_authority else binding
        for binding in profile.access_bindings)
    return dataclasses.replace(profile,
                               revision=profile.revision + 1,
                               access_bindings=bindings)


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


def test_shared_lease_heartbeat_fails_closed_on_synchronous_renewal() -> None:
    renew = mock.Mock(side_effect=(True, False))
    heartbeat = worker_lease.LeaseHeartbeat(renew, interval=60)

    with heartbeat:
        with pytest.raises(worker_lease.LeaseLostError):
            heartbeat.assert_owned()
        assert heartbeat.cancel_event.is_set()

    assert renew.call_count == 2


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
    canonical = _canonical_location(profile)
    monkeypatch.setattr(copy_worker_service.catalog_state, 'get_artifact',
                        lambda *_: _artifact())
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_location',
                        lambda _: canonical)
    monkeypatch.setattr(
        copy_worker_service.topology_state, 'get_shard',
        lambda shard_id: _shard(profile)
        if shard_id == location.shard_id else _shard(profile, profile.canonical.
                                                     name, canonical.shard_id))
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
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
    canonical = _canonical_location(profile)
    monkeypatch.setattr(copy_worker_service.catalog_state, 'get_artifact',
                        lambda *_: _artifact())
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_location',
                        lambda _: canonical)
    monkeypatch.setattr(
        copy_worker_service.topology_state, 'get_shard',
        lambda shard_id: _shard(profile)
        if shard_id == location.shard_id else _shard(profile, profile.canonical.
                                                     name, canonical.shard_id))
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
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


def test_copy_lease_loss_during_budget_wait_blocks_provider_call(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = _copying_location(profile)
    graph = SimpleNamespace(runtime_digest=_DIGEST,
                            config=SimpleNamespace(digest=_CONFIG_DIGEST),
                            platform='linux/amd64')
    events: list[str] = []
    lost = False

    class FencedHeartbeat:
        """Deterministic heartbeat that loses ownership during a wait."""

        def __init__(self, *_args: object) -> None:
            self.cancel_event = threading.Event()

        def __enter__(self):
            self.assert_owned()
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def assert_owned(self) -> None:
            events.append('lease')
            if lost:
                self.cancel_event.set()
                raise worker_lease.LeaseLostError(
                    'Container image work lease was lost.')

    class Destination:
        """Invokes the production hook immediately before fake provider I/O."""

        def __init__(self, hooks: aws.EcrCallHooks) -> None:
            self._hooks = hooks
            self.verify_graph = mock.Mock()

        def copy_graph(self, *_args: object) -> aws.CopyOutcome:
            self._hooks.before_call()
            events.append('provider')
            return aws.CopyOutcome.WRITTEN

    def repository_from_role(*_args: object, **kwargs: object) -> Destination:
        events.append('sts')
        return Destination(kwargs['hooks'])

    def lose_lease_during_budget(_shard: object) -> None:
        nonlocal lost
        events.append('budget')
        lost = True

    limiter = SimpleNamespace(before_call=lose_lease_during_budget,
                              record_throttle=lambda _shard: None)
    monkeypatch.setattr(copy_worker_service.catalog_state, 'get_artifact',
                        lambda *_args: _artifact())
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_shard',
                        lambda _shard_id: _shard(profile))
    monkeypatch.setattr(copy_worker_service, '_profile_for_location',
                        lambda *_args: profile)
    monkeypatch.setattr(
        copy_worker_service, '_graph_for_location',
        lambda *_args: events.append('source') or
        (graph, mock.sentinel.read_blob))
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', FencedHeartbeat)
    transition = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'transition_location_to_verifying', transition)
    converge = mock.Mock()
    monkeypatch.setattr(copy_worker_service.transactions, 'converge_canonical',
                        converge)

    assert not copy_worker_service.copy_location(
        location, limiter=limiter, lease_seconds=30)

    assert events == [
        'lease',
        'source',
        'lease',
        'sts',
        'lease',
        'lease',
        'budget',
        'lease',
    ]
    assert 'provider' not in events
    transition.assert_not_called()
    converge.assert_called_once()
    assert converge.call_args.kwargs['ready'] is False


def test_regional_source_credentials_and_ecr_reads_are_lease_fenced(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = _copying_location(profile)
    canonical = _canonical_location(profile)
    source_shard = _shard(profile, profile.canonical.name, canonical.shard_id)
    graph = mock.sentinel.graph
    events: list[str] = []
    heartbeat = SimpleNamespace(assert_owned=lambda: events.append('lease'))
    limiter = SimpleNamespace(
        before_call=lambda _shard: events.append('budget'),
        record_throttle=lambda _shard: None)

    def repository_from_role(*_args: object,
                             **kwargs: object) -> SimpleNamespace:
        events.append('sts')
        hooks = kwargs['hooks']

        def read_manifest(_digest: str) -> bytes:
            hooks.before_call()
            events.append('source-read')
            return b'{}'

        return SimpleNamespace(read_manifest=read_manifest,
                               read_blob=mock.sentinel.read_blob,
                               read_blob_bytes=mock.Mock())

    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_location',
                        lambda _location_id: canonical)
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_shard',
                        lambda _shard_id: source_shard)
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    monkeypatch.setattr(copy_worker_service.oci, 'build_content_graph',
                        lambda **_kwargs: graph)

    resolved, read_blob = copy_worker_service._graph_for_location(
        location, _artifact(), profile, limiter, heartbeat)

    assert resolved is graph
    assert read_blob is mock.sentinel.read_blob
    assert events == [
        'lease',
        'sts',
        'lease',
        'lease',
        'budget',
        'lease',
        'source-read',
    ]


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


def test_publication_release_limit_keeps_typed_error(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    publication = SimpleNamespace(
        id='publication-id',
        workspace='research',
        operation_id='operation-id',
        profile_revision_id=_REVISION_ID,
        inspection_lease_token='inspection-token',
        source_ref=f'ghcr.io/example/runtime@{_DIGEST}',
        source_root_digest=_DIGEST,
        requested_platform='linux/amd64',
        source_auth_binding_id=None,
        source_auth_fingerprint=None,
        attempt_count=1,
        created_at=10)
    graph = SimpleNamespace(runtime_digest=_DIGEST,
                            config=SimpleNamespace(digest=_CONFIG_DIGEST),
                            platform='linux/amd64',
                            source_root_media_type='application/vnd.oci.image.'
                            'manifest.v1+json',
                            runtime_media_type='application/vnd.oci.image.'
                            'manifest.v1+json',
                            raw_runtime_manifest=b'{}',
                            declared_size_bytes=2)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
    monkeypatch.setattr(copy_worker_service.catalog_state, 'get_operation',
                        lambda *_: SimpleNamespace(actor_hash='actor'))
    monkeypatch.setattr(copy_worker_service, '_source_reader',
                        lambda *_: mock.sentinel.reader)
    monkeypatch.setattr(copy_worker_service, '_inspection_graph',
                        lambda *_: graph)
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', _OwnedHeartbeat)
    monkeypatch.setattr(
        copy_worker_service.transactions, 'bind_inspected_publication',
        mock.Mock(side_effect=transactions.ImageLimitExceededError(
            'IMAGE_LIMIT_EXCEEDED')))
    fail = mock.Mock()
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'fail_publication_inspection', fail)

    assert not copy_worker_service.inspect_publication(publication)

    fail.assert_called_once_with('publication-id',
                                 'inspection-token',
                                 'IMAGE_LIMIT_EXCEEDED',
                                 retry_at=None,
                                 terminal=True)


def test_nonclaim_eviction_state_never_reaches_provider(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.EVICTING,
                                   lease_kind='DELETE')
    provider = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        provider)

    assert not lifecycle_worker_service.evict_location(location, mock.Mock())
    provider.assert_not_called()


def test_eviction_without_delete_authority_restores_without_provider_io(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    profile = dataclasses.replace(
        profile,
        targets=(dataclasses.replace(profile.targets[0],
                                     delete_authority=None),))
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.EVICTING,
                                   lease_kind='EVICT')
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
    complete = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', complete)

    assert not lifecycle_worker_service.evict_location(location, mock.Mock())

    complete.assert_called_once_with(location.id,
                                     location.lease_token,
                                     present=None,
                                     provider_not_called=True)


def test_eviction_resolution_failure_restores_without_provider_io(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.EVICTING,
                                   lease_kind='EVICT')
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(lifecycle_worker_service,
                        '_profile_target_for_location', lambda *_args: None)
    complete = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', complete)

    assert not lifecycle_worker_service.evict_location(location, mock.Mock())

    complete.assert_called_once_with(location.id,
                                     location.lease_token,
                                     present=None,
                                     provider_not_called=True)


def test_eviction_uses_shard_activated_revision(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    old_revision = dataclasses.replace(_revision(profile),
                                       state=models.ImageProfileState.RETIRED)
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.EVICTING,
                                   lease_kind='EVICT')
    repository = mock.Mock()
    roles: list[aws.AwsRoleBinding] = []
    call_order: list[str] = []
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'get_profile_revision', lambda _: old_revision)
    monkeypatch.setattr(lifecycle_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(lifecycle_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    begin_delete = mock.Mock(
        side_effect=lambda *_args: call_order.append('intent') or True)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'begin_eviction_delete', begin_delete)

    def repository_from_role(role: aws.AwsRoleBinding, *_args: object,
                             **_kwargs: object) -> mock.Mock:
        roles.append(role)
        hooks = _kwargs['hooks']

        def delete_outcome(_digest: str) -> aws.DeleteOutcome:
            hooks.before_call()
            call_order.append('delete')
            return aws.DeleteOutcome.ABSENT

        repository.delete_outcome.side_effect = delete_outcome
        return repository

    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', mock.Mock())

    limiter = mock.Mock()
    assert lifecycle_worker_service.evict_location(location, limiter)
    delete_authority = profile.targets[0].delete_authority
    assert delete_authority is not None
    assert roles[0].role_arn == profile.bindings[delete_authority].authority
    begin_delete.assert_called_once_with(location.id, location.lease_token)
    assert call_order == ['intent', 'delete']


def test_not_started_after_delete_intent_fails_closed(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.EVICTING,
                                   lease_kind='EVICT')
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
    monkeypatch.setattr(lifecycle_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(lifecycle_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'begin_eviction_delete', lambda *_args: True)

    def repository_from_role(*_args: object, **kwargs: object) -> mock.Mock:
        repository = mock.Mock()

        def delete_outcome(_digest: str) -> aws.DeleteOutcome:
            kwargs['hooks'].before_call()
            return aws.DeleteOutcome.NOT_STARTED

        repository.delete_outcome.side_effect = delete_outcome
        return repository

    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    complete = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', complete)

    assert not lifecycle_worker_service.evict_location(location, mock.Mock())
    complete.assert_called_once_with(location.id,
                                     location.lease_token,
                                     present=None,
                                     provider_not_called=False)


def test_copy_and_inventory_use_operational_revision_not_newer_candidate(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    candidate_profile = _policy_profile(profile)
    shard = _shard(profile)
    canonical = _canonical_location(profile)
    old_key = models.profile_attestation_key('terraform_shard',
                                             shard.physical_fingerprint)
    old_revision = dataclasses.replace(
        _revision(profile),
        attestations={
            old_key: {
                'status': 'READY',
                'physical_fingerprint': shard.physical_fingerprint,
            }
        })
    candidate_revision = dataclasses.replace(
        _revision(candidate_profile),
        id='new-revision',
        desired_generation=2,
        state=models.ImageProfileState.QUALIFYING,
        attestations={
            old_key: {
                'status': 'READY',
                'physical_fingerprint': shard.physical_fingerprint,
            }
        })
    monkeypatch.setattr(
        copy_worker_service.topology_state, 'list_profile_revisions',
        mock.Mock(
            side_effect=AssertionError('operational lookup must be exact')))
    monkeypatch.setattr(
        copy_worker_service.topology_state, 'get_profile_revision',
        lambda revision_id: old_revision
        if revision_id == old_revision.id else candidate_revision)
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_location',
                        lambda _: canonical)
    monkeypatch.setattr(
        copy_worker_service.topology_state, 'get_shard',
        lambda shard_id: shard if shard_id == _SHARD_ID else _shard(
            profile, profile.canonical.name, canonical.shard_id))

    resolved = copy_worker_service._profile_for_location(
        _copying_location(profile), shard)
    revision, inventory_profile = copy_worker_service._profile_for_shard(shard)

    assert resolved == profile
    assert revision.id == old_revision.id
    assert inventory_profile == profile


def test_bootstrap_inventory_uses_exact_qualifying_physical_revision(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    shard = dataclasses.replace(_shard(profile),
                                profile_revision_id=None,
                                state=models.ImageShardState.PENDING)
    key = models.profile_attestation_key('terraform_shard',
                                         shard.physical_fingerprint)
    candidate = dataclasses.replace(
        _revision(profile),
        state=models.ImageProfileState.QUALIFYING,
        attestations={
            key: {
                'status': 'READY',
                'physical_fingerprint': shard.physical_fingerprint,
            }
        })
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_profile_revisions',
                        lambda *_args, **_kwargs: [candidate])

    revision, inventory_profile = copy_worker_service._profile_for_shard(shard)

    assert revision.id == candidate.id
    assert inventory_profile == profile


@pytest.mark.parametrize('metadata_matches', [True, False])
def test_candidate_shard_probe_never_mutates_operational_state(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        metadata_matches: bool) -> None:
    candidate_profile = _policy_profile(profile)
    target = candidate_profile.targets[0]
    base_shard = dataclasses.replace(_shard(profile),
                                     inventory_epoch=3,
                                     inventory_started_at=80,
                                     inventory_completed_at=90)
    shards = [
        dataclasses.replace(base_shard,
                            id=f'shard-{index}',
                            shard_index=index,
                            physical_fingerprint=f'{index:064x}')
        for index in range(target.shard_count)
    ]
    shard = shards[0]
    expected_metadata = {
        'repository_arn': shard.repository_arn,
        'repository_uri': f'{shard.registry}/{shard.repository_name}',
        'tag_mutability': 'IMMUTABLE',
        'encryption_type': 'AES256',
        'kms_key': None,
        'scanning_mode': 'SCAN_ON_PUSH',
        'policy_hash': 'a' * 64,
        'ownership_tags_hash': 'b' * 64,
    }
    attestations = {
        models.profile_attestation_key('terraform_shard', item.physical_fingerprint):
            {
                'status': 'READY',
                'physical_fingerprint': item.physical_fingerprint,
                'live_attestation_key': models.profile_attestation_key(
                    'infrastructure_shard', item.physical_fingerprint),
                **expected_metadata,
                'terraform_applied_quota': 100,
                'max_manifests': 90,
                'reserved_headroom': 10,
            } for item in shards
    }
    candidate = dataclasses.replace(_revision(candidate_profile),
                                    id='candidate-revision',
                                    desired_generation=2,
                                    state=models.ImageProfileState.QUALIFYING,
                                    attestations=attestations)
    repository = mock.Mock()
    repository.repository_metadata.return_value = (expected_metadata
                                                   if metadata_matches else {
                                                       **expected_metadata,
                                                       'policy_hash': 'c' * 64,
                                                   })
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_target_shards', lambda *_args: shards)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: repository)
    monkeypatch.setattr(copy_worker_service.aws,
                        'applied_ecr_images_per_repository_quota',
                        lambda *_args: 100)
    record = mock.Mock(return_value=candidate)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_candidate_shard_attestation', record)
    drift = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'mark_shard_drifted', drift)

    reconciled = copy_worker_service._reconcile_candidate_shard_attestation(
        candidate, candidate_profile, target, limiter=mock.Mock(), now=100)

    assert reconciled is metadata_matches
    assert record.call_count == (target.shard_count if metadata_matches else 0)
    drift.assert_not_called()
    if metadata_matches:
        assert record.call_args.kwargs['profile_revision_id'] == candidate.id
        assert record.call_args.kwargs[
            'expected_operational_revision_id'] == _REVISION_ID


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
