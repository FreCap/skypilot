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
from sky.container_images import budgets
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


def _canary_operation(
    *,
    operation_id: str = '00000000-0000-4000-8000-000000000009',
) -> catalog_state.OperationRecord:
    return catalog_state.OperationRecord(
        id=operation_id,
        authority_id='00000000-0000-4000-8000-000000000010',
        scope='research',
        actor_hash='a' * 64,
        kind='PROFILE_CANARY',
        idempotency_key='canary-idempotency-key',
        request_hash='b' * 64,
        state=models.ImageOperationState.RUNNING,
        result_kind='profile_revision',
        result_id=_REVISION_ID,
        result=None,
        error_code=None,
        lease_token='canary-lease-token',
        lease_expires_at=10**12,
        child_launch_id=None,
        teardown_deadline=10**12,
        created_at=10,
        updated_at=11,
        terminal_expires_at=None)


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
    reconcile.assert_called_once_with(demand, 'orphan-cluster', now=current)


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
    preserve = mock.Mock(return_value=True)
    reconcile = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_terminal_confirmation', preserve)
    monkeypatch.setattr(lifecycle_worker_service, '_reconcile_cluster_terminal',
                        reconcile)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_not_called()
    preserve.assert_called_once_with(demand.id, now=current)
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
    reconcile.assert_called_once_with(demand, 'orphan-cluster', now=current)


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
    reconcile.assert_called_once_with(demand, 'orphan-cluster', now=current)


def test_lifecycle_policy_refresh_keeps_last_valid_retentions(
        monkeypatch: pytest.MonkeyPatch) -> None:
    previous = {'research': 123, 'retained': None}
    reload_config = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.skypilot_config,
                        'safe_reload_config', reload_config)
    monkeypatch.setattr(
        lifecycle_worker_service.config, 'list_workspace_policies',
        mock.Mock(side_effect=ValueError('malformed workspace policy')))

    refreshed = (lifecycle_worker_service.
                 _refresh_workspace_eviction_retentions(previous))
    startup = lifecycle_worker_service._refresh_workspace_eviction_retentions(
        None)

    assert refreshed is previous
    assert startup is None
    assert reload_config.call_count == 2


@pytest.mark.parametrize('invalid_workspaces', [
    None,
    {
        'research': {
            'container_images': None,
        },
    },
])
def test_lifecycle_policy_refresh_rejects_null_and_keeps_last_valid_map(
        monkeypatch: pytest.MonkeyPatch, invalid_workspaces: object) -> None:
    previous = {'research': None, 'other': 123}
    reload_config = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.skypilot_config,
                        'safe_reload_config', reload_config)
    monkeypatch.setattr(lifecycle_worker_service.config.skypilot_config,
                        'get_nested',
                        lambda *_args, **_kwargs: invalid_workspaces)

    refreshed = (lifecycle_worker_service.
                 _refresh_workspace_eviction_retentions(previous))

    assert refreshed is previous
    reload_config.assert_called_once_with()


def test_lifecycle_policy_refresh_defaults_only_a_missing_policy_key(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lifecycle_worker_service.skypilot_config,
                        'safe_reload_config', mock.Mock())
    monkeypatch.setattr(lifecycle_worker_service.config.skypilot_config,
                        'get_nested',
                        lambda *_args, **_kwargs: {'research': {}})

    refreshed = lifecycle_worker_service._refresh_workspace_eviction_retentions(
        {'research': None})

    assert refreshed == {'research': 8 * 7 * 24 * 60 * 60}


def test_provider_budget_limiter_uses_only_monotonic_local_expiry(
        monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = budgets.ProviderBudgetLimiter('copy-worker')
    monkeypatch.setattr(limiter, '_budget',
                        lambda _: SimpleNamespace(id='provider-budget'))
    acquire = mock.Mock(return_value=topology_state.ProviderGrant(
        budget_id='provider-budget', tokens=2, valid_for_seconds=1))
    monkeypatch.setattr(topology_state, 'acquire_provider_grant', acquire)
    monkeypatch.setattr(budgets.time, 'monotonic', lambda: 100.0)
    monkeypatch.setattr(
        budgets.time, 'time',
        mock.Mock(side_effect=AssertionError('wall clock is not local expiry')))

    limiter.before_call(SimpleNamespace())
    limiter.before_call(SimpleNamespace())

    acquire.assert_called_once_with('copy-worker',
                                    'provider-budget',
                                    requested_calls=64)


@pytest.mark.parametrize(
    ('message', 'expected'),
    [('AccessDenied: arn:aws:iam::123:role/secret', 'CANARY_FAILED'),
     ('CANARY_TIMEOUT', 'CANARY_TIMEOUT')])
def test_canary_persists_only_closed_error_codes(
        monkeypatch: pytest.MonkeyPatch, message: str, expected: str) -> None:
    operation = _canary_operation()
    monkeypatch.setattr(canary_worker_service, '_load_contract',
                        mock.Mock(side_effect=ValueError(message)))
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    failed = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', failed)

    assert not canary_worker_service.run_canary(operation)
    failed.assert_called_once_with(operation, expected, teardown_verified=True)


def test_database_expired_canary_terminalizes_as_timeout(
        monkeypatch: pytest.MonkeyPatch) -> None:
    operation = dataclasses.replace(_canary_operation(), teardown_deadline=1)
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        ({
            'backend': 'aws_vm'
        }, mock.sentinel.revision, mock.sentinel.profile, mock.sentinel.target,
         mock.sentinel.binding, _DIGEST, mock.sentinel.ref))
    run_ec2 = mock.Mock(side_effect=ValueError('CANARY_TIMEOUT'))
    monkeypatch.setattr(canary_worker_service, '_run_ec2_canary', run_ec2)
    failed = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', failed)

    assert not canary_worker_service.run_canary(operation)
    run_ec2.assert_called_once()
    failed.assert_called_once_with(operation,
                                   'CANARY_TIMEOUT',
                                   teardown_verified=True)


def test_unverified_canary_teardown_remains_reclaimable(
        monkeypatch: pytest.MonkeyPatch) -> None:
    operation = _canary_operation()
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        ({
            'backend': 'aws_vm'
        }, mock.sentinel.revision, mock.sentinel.profile, mock.sentinel.target,
         mock.sentinel.binding, _DIGEST, mock.sentinel.ref))
    monkeypatch.setattr(
        canary_worker_service, '_run_ec2_canary',
        mock.Mock(side_effect=ValueError('CANARY_TEARDOWN_FAILED')))
    fail = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)

    assert not canary_worker_service.run_canary(operation)
    fail.assert_not_called()


def test_persisted_canary_child_survives_contract_reload_failure(
        monkeypatch: pytest.MonkeyPatch) -> None:
    operation = dataclasses.replace(_canary_operation(),
                                    child_launch_id='ec2:us-west-2:child')
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract',
        mock.Mock(side_effect=ValueError('QUALIFICATION_FAILED')))
    fail = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)

    assert not canary_worker_service.run_canary(operation)
    fail.assert_not_called()


@pytest.mark.parametrize('backend', ['aws_vm', 'aws_eks'])
def test_initial_canary_client_failure_terminalizes_without_provider_child(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        backend: str) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding(backend)
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    runtime_id = (target.region if backend == 'aws_vm' else
                  binding.qualified_clusters[0].context)
    payload = {
        'backend': backend,
        'nonce': '3' * 32,
        'runtime_id': runtime_id,
        'timeout_seconds': 900,
    }
    reference = f'{target.registry}/qualification@{_DIGEST}'
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        (payload, _revision(profile), profile, target, binding, _DIGEST,
         reference))
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    if backend == 'aws_vm':
        monkeypatch.setattr(
            canary_worker_service, '_assumed_client',
            mock.Mock(side_effect=RuntimeError('EC2 client unavailable')))
    else:
        monkeypatch.setattr(
            canary_worker_service, '_kubernetes_core',
            mock.Mock(
                side_effect=RuntimeError('Kubernetes client unavailable')))
    failed = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', failed)

    assert not canary_worker_service.run_canary(operation)
    failed.assert_called_once_with(operation,
                                   'CANARY_FAILED',
                                   teardown_verified=True)


def _eks_node(uid: str, instance_id: str, *, selector_value: str = 'eks-node'):
    return SimpleNamespace(metadata=SimpleNamespace(
        uid=uid,
        labels={
            'kubernetes.io/arch': 'amd64',
            'skypilot.co/image-pull-role': selector_value,
        }),
                           spec=SimpleNamespace(
                               provider_id=f'aws:///us-west-2a/{instance_id}',
                               unschedulable=False))


def _api_error(status: int) -> RuntimeError:
    error = RuntimeError(f'Kubernetes API status {status}')
    error.status = status  # type: ignore[attr-defined]
    return error


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
        lambda _role, service, _region, **_kwargs: ec2
        if service == 'ec2' else iam)
    monkeypatch.setattr(canary_worker_service, '_instance_profile_role',
                        lambda _iam, _name: qualified.node_role)
    heartbeat = _OwnedHeartbeat()
    fenced_core = canary_worker_service._FencedClient(core, heartbeat)

    count, node_set_hash = canary_worker_service._qualified_eks_nodes(
        fenced_core, mock.sentinel.role, target, qualified, heartbeat)

    assert count == 2
    assert len(node_set_hash) == 64
    core.list_node.assert_called_once_with(
        label_selector=('kubernetes.io/arch=amd64,'
                        'skypilot.co/image-pull-role=eks-node'),
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
        lambda _role, service, _region, **_kwargs: ec2
        if service == 'ec2' else mock.Mock())
    monkeypatch.setattr(
        canary_worker_service, '_instance_profile_role',
        lambda _iam, name: qualified.node_role
        if name == 'qualified' else 'arn:aws:iam::123:role/OtherNodeRole')

    with pytest.raises(ValueError,
                       match='QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED'):
        heartbeat = _OwnedHeartbeat()
        fenced_core = canary_worker_service._FencedClient(core, heartbeat)
        canary_worker_service._qualified_eks_nodes(fenced_core,
                                                   mock.sentinel.role, target,
                                                   qualified, heartbeat)


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
        inventory_finalizing=False,
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


def test_canary_provider_call_rechecks_lease_after_response() -> None:
    provider = mock.Mock()
    provider.describe_instances.return_value = {'Reservations': []}
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = (
        None, worker_lease.LeaseLostError('canary lease lost'))
    client = canary_worker_service._FencedClient(provider, heartbeat)

    with pytest.raises(worker_lease.LeaseLostError, match='canary lease lost'):
        client.describe_instances()

    provider.describe_instances.assert_called_once_with()
    assert heartbeat.assert_owned.call_count == 2


def test_canary_provider_call_rejects_stale_owner_before_request() -> None:
    provider = mock.Mock()
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = worker_lease.LeaseLostError(
        'canary lease lost')
    client = canary_worker_service._FencedClient(provider, heartbeat)

    with pytest.raises(worker_lease.LeaseLostError, match='canary lease lost'):
        client.describe_instances()

    provider.describe_instances.assert_not_called()


@pytest.mark.parametrize('method_name',
                         ['run_instances', 'create_namespaced_pod'])
def test_canary_create_rechecks_deadline_after_ownership_renewal(
        monkeypatch: pytest.MonkeyPatch, method_name: str) -> None:
    current = [99]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    provider = mock.Mock()
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = lambda: current.__setitem__(0, 100)
    started = mock.Mock()
    client = canary_worker_service._FencedClient(provider, heartbeat)

    with pytest.raises(ValueError, match='CANARY_TIMEOUT'):
        client.call_before_deadline(method_name, 100, started)

    started.assert_not_called()
    getattr(provider, method_name).assert_not_called()
    heartbeat.assert_owned.assert_called_once_with()


def test_expired_database_authorization_blocks_ec2_create_with_slow_host_clock(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {'Reservations': []}
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Roles': [{
                'Arn': binding.principals[0]
            }]
        }
    }
    clients = {'ec2': ec2, 'iam': iam}
    monkeypatch.setattr(
        canary_worker_service.aws, 'assumed_client',
        lambda _role, service, _region, **_kwargs: clients[service])
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    authorize = mock.Mock(return_value=None)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch', authorize)
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: -1_000_000.0)

    with pytest.raises(ValueError, match='CANARY_TIMEOUT'):
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    authorize.assert_called_once()
    ec2.run_instances.assert_not_called()


def test_expired_database_authorization_blocks_eks_create_with_slow_host_clock(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_eks')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    qualified = binding.qualified_clusters[0]
    payload = {
        'backend': 'aws_eks',
        'nonce': '2' * 32,
        'runtime_id': qualified.context,
    }
    core = mock.Mock()
    monkeypatch.setattr(canary_worker_service.kubernetes, 'core_api',
                        lambda _context: core)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    assumed_client = mock.Mock(side_effect=AssertionError(
        'provider preflight must not run after DB authorization expires'))
    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client',
                        assumed_client)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    authorize = mock.Mock(return_value=None)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch', authorize)
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: -1_000_000.0)

    with pytest.raises(ValueError, match='CANARY_TIMEOUT'):
        canary_worker_service._run_eks_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    authorize.assert_called_once()
    assumed_client.assert_not_called()
    core.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(('returned_id', 'tags'), [
    ('i-other', [{
        'Key': 'SkyPilotCanaryOperation',
        'Value': 'operation',
    }, {
        'Key': 'SkyPilotCatalog',
        'Value': 'catalog',
    }, {
        'Key': 'SkyPilotProfile',
        'Value': 'profile',
    }]),
    ('i-primary', [{
        'Key': 'SkyPilotCanaryOperation',
        'Value': 'operation',
    }, {
        'Key': 'SkyPilotCatalog',
        'Value': 'other-catalog',
    }, {
        'Key': 'SkyPilotProfile',
        'Value': 'profile',
    }]),
    ('i-primary', [{
        'Key': 'SkyPilotCanaryOperation',
        'Value': 'operation',
    }, {
        'Key': 'SkyPilotCanaryOperation',
        'Value': 'operation',
    }, {
        'Key': 'SkyPilotCatalog',
        'Value': 'catalog',
    }, {
        'Key': 'SkyPilotProfile',
        'Value': 'profile',
    }]),
    ('i-primary', None),
])
def test_exact_ec2_canary_read_rejects_id_and_tag_splices(
        returned_id: str, tags: list[dict[str, str]] | None) -> None:
    ec2 = mock.Mock()
    instance = {'InstanceId': returned_id}
    if tags is not None:
        instance['Tags'] = tags
    ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [instance]
        }]
    }

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        canary_worker_service._exact_canary_instance(
            ec2, 'i-primary', {
                'SkyPilotCanaryOperation': 'operation',
                'SkyPilotCatalog': 'catalog',
                'SkyPilotProfile': 'profile',
            })

    ec2.describe_instances.assert_called_once_with(InstanceIds=['i-primary'])


@pytest.mark.parametrize(('architecture', 'image_id_case', 'expected_error'),
                         (('x86_64', 'expected', None),
                          ('arm64', 'expected', 'QUALIFICATION_FAILED'),
                          (None, 'expected', 'QUALIFICATION_FAILED'),
                          ('x86_64', 'other', 'QUALIFICATION_FAILED'),
                          ('x86_64', None, 'QUALIFICATION_FAILED')))
def test_ec2_canary_observes_exact_host_and_uses_fenced_clients(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        architecture: str | None, image_id_case: str | None,
        expected_error: str | None) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    assert binding.instance_profile is not None
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }
    expected_tags = [{
        'Key': 'SkyPilotCanaryOperation',
        'Value': operation.id,
    }, {
        'Key': 'SkyPilotCatalog',
        'Value': 'catalog',
    }, {
        'Key': 'SkyPilotProfile',
        'Value': profile.name,
    }]
    tagged_instance = {'InstanceId': 'i-canary'}
    instance = {
        'InstanceId': 'i-canary',
        'Tags': expected_tags,
        'IamInstanceProfile': {
            'Arn': models.ec2_instance_profile_arn(binding),
        },
        'State': {
            'Name': 'stopped'
        },
    }
    if image_id_case == 'expected':
        instance['ImageId'] = dict(binding.qualified_node_images)[target.region]
    elif image_id_case == 'other':
        instance['ImageId'] = 'ami-other'
    if architecture is not None:
        instance['Architecture'] = architecture
    terminated_instance = {
        **instance,
        'State': {
            'Name': 'terminated'
        },
    }
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = ({
        'Reservations': []
    }, {
        'Reservations': [{
            'Instances': [tagged_instance]
        }]
    }, {
        'Reservations': [{
            'Instances': [instance]
        }]
    }, {
        'Reservations': [{
            'Instances': [tagged_instance]
        }]
    }, {
        'Reservations': [{
            'Instances': [tagged_instance]
        }]
    }, {
        'Reservations': [{
            'Instances': [terminated_instance]
        }]
    })
    ec2.run_instances.side_effect = (
        TimeoutError('ambiguous EC2 launch response'), {
            'Instances': [{
                'InstanceId': 'i-canary'
            }]
        })
    marker = f'SKYPILOT_IMAGE_CANARY_SUCCESS:{payload["nonce"]}'
    ec2.get_console_output.return_value = {
        'Output': base64.b64encode(marker.encode()).decode()
    }
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Roles': [{
                'Arn': binding.principals[0]
            }]
        }
    }
    provider_fences: list[object] = []

    def assumed_client(_role: object,
                       service: str,
                       _region: str,
                       *,
                       provider_fence: object = None) -> object:
        provider_fences.append(provider_fence)
        return ec2 if service == 'ec2' else iam

    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client',
                        assumed_client)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch',
                        lambda *_args, **_kwargs: 1000)
    heartbeat = _OwnedHeartbeat()

    if expected_error is None:
        evidence = canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}', heartbeat)
        assert evidence['child_instance_id'] == 'i-canary'
        assert evidence['host_image_id'] == dict(
            binding.qualified_node_images)[target.region]
        assert evidence['instance_architecture'] == 'x86_64'
    else:
        with pytest.raises(ValueError, match=expected_error):
            canary_worker_service._run_ec2_canary(
                operation, payload, _revision(profile), profile, target,
                binding, _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
                heartbeat)
    assert ec2.run_instances.call_args.kwargs['ClientToken'] == (
        canary_worker_service._ec2_client_token(operation.id))
    assert len(ec2.run_instances.call_args.kwargs['ClientToken']) == 64
    assert ec2.run_instances.call_count == 2
    assert ec2.run_instances.call_args_list[0].kwargs == (
        ec2.run_instances.call_args_list[1].kwargs)
    assert mock.call(
        InstanceIds=['i-canary']) in (ec2.describe_instances.call_args_list)
    launch = ec2.run_instances.call_args.kwargs
    assert launch['InstanceInitiatedShutdownBehavior'] == 'terminate'
    assert launch['SecurityGroupIds'] == list(
        dict(binding.canary_security_groups)[target.region])
    assert {item['ResourceType'] for item in launch['TagSpecifications']
           } == {'instance', 'volume', 'network-interface'}
    for specification in launch['TagSpecifications']:
        assert {
            tag['Key']: tag['Value'] for tag in specification['Tags']
        } == {
            'SkyPilotCanaryOperation': operation.id,
            'SkyPilotCatalog': 'catalog',
            'SkyPilotProfile': profile.name,
        }
    assert provider_fences == [heartbeat.assert_owned, heartbeat.assert_owned]


@pytest.mark.parametrize('invalid_instance_id', [None, 7],
                         ids=['missing-id', 'non-string-id'])
def test_ec2_canary_malformed_launch_identity_settles_and_terminates_late_child(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        invalid_instance_id: object) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    assert binding.instance_profile is not None
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }

    def response(*instances: dict[str, object]) -> dict[str, object]:
        if not instances:
            return {'Reservations': []}
        return {'Reservations': [{'Instances': list(instances)}]}

    late = {
        'InstanceId': 'i-late',
        'State': {
            'Name': 'running',
        },
    }
    terminated = {
        'InstanceId': 'i-late',
        'State': {
            'Name': 'terminated',
        },
    }
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        response(),  # Initial operation-tag discovery.
        response(),  # Immediate discovery in finally.
        response(),  # First settling attempt still observes tag propagation.
        response(late),  # The paid child becomes visible later.
        response(terminated),  # Exact ID-scoped termination proof.
    )
    ec2.run_instances.return_value = {
        'Instances': [{
            'InstanceId': invalid_instance_id,
        }]
    }
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Roles': [{
                'Arn': binding.principals[0],
            }]
        }
    }
    clients = {'ec2': ec2, 'iam': iam}
    monkeypatch.setattr(
        canary_worker_service.aws, 'assumed_client',
        lambda _role, service, _region, **_kwargs: clients[service])
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch',
                        lambda *_args, **_kwargs: 1000)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    ec2.run_instances.assert_called_once()
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-late'])
    assert mock.call(
        InstanceIds=['i-late']) in ec2.describe_instances.call_args_list
    ec2.get_console_output.assert_not_called()


def test_ec2_canary_malformed_discovered_identity_uses_settling_teardown(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }

    def response(*instances: dict[str, object]) -> dict[str, object]:
        if not instances:
            return {'Reservations': []}
        return {'Reservations': [{'Instances': list(instances)}]}

    malformed = {'State': {'Name': 'running'}}
    late = {'InstanceId': 'i-late', 'State': {'Name': 'running'}}
    terminated = {
        'InstanceId': 'i-late',
        'State': {
            'Name': 'terminated',
        },
    }
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        response(malformed),
        response(),
        response(),
        response(late),
        response(terminated),
    )
    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client',
                        lambda *_args, **_kwargs: ec2)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    ec2.run_instances.assert_not_called()
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-late'])


def test_ec2_teardown_retains_late_child_after_partial_termination(
        monkeypatch: pytest.MonkeyPatch) -> None:

    def response(*instances: dict[str, object]) -> dict[str, object]:
        return {'Reservations': [{'Instances': list(instances)}]}

    known_running = {
        'InstanceId': 'i-known',
        'State': {
            'Name': 'running',
        },
    }
    known_stopping = {
        'InstanceId': 'i-known',
        'State': {
            'Name': 'shutting-down',
        },
    }
    known_terminated = {
        'InstanceId': 'i-known',
        'State': {
            'Name': 'terminated',
        },
    }
    late_running = {
        'InstanceId': 'i-late',
        'State': {
            'Name': 'running',
        },
    }
    late_terminated = {
        'InstanceId': 'i-late',
        'State': {
            'Name': 'terminated',
        },
    }
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        response(known_running),
        response(known_stopping),
        response(known_terminated, late_running),
        response(known_terminated, late_terminated),
    )
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    assert canary_worker_service._terminate_ec2_instances(ec2,
                                                          'operation',
                                                          ['i-known'],
                                                          settle_absence=False)

    assert ec2.terminate_instances.call_args_list == [
        mock.call(InstanceIds=['i-known']),
        mock.call(InstanceIds=['i-late']),
    ]


@pytest.mark.parametrize('persisted_child', [False, True],
                         ids=['fresh-launch', 'replay'])
def test_ec2_canary_rejects_mismatched_tagged_child_and_tears_down_all(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        persisted_child: bool) -> None:
    operation = _canary_operation()
    if persisted_child:
        operation = dataclasses.replace(
            operation, child_launch_id=f'ec2:us-west-2:{operation.id}')
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    assert binding.instance_profile is not None
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }
    primary = {'InstanceId': 'i-primary'}
    unexpected = {
        'InstanceId': 'i-other',
        'ImageId': dict(binding.qualified_node_images)[target.region],
        'Architecture': 'x86_64',
        'IamInstanceProfile': {
            'Arn': models.ec2_instance_profile_arn(binding),
        },
        'State': {
            'Name': 'stopped'
        },
    }
    terminated = [{
        'InstanceId': instance_id,
        'State': {
            'Name': 'terminated'
        },
    } for instance_id in ('i-primary', 'i-other')]

    def response(*instances: dict[str, object]) -> dict[str, object]:
        if not instances:
            return {'Reservations': []}
        return {'Reservations': [{'Instances': list(instances)}]}

    discovery = response(primary) if persisted_child else response()
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        discovery,
        response(unexpected),
        response(primary),
        response(primary, unexpected),
        response(*terminated),
    )
    ec2.run_instances.return_value = {'Instances': [primary]}
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Roles': [{
                'Arn': binding.principals[0]
            }]
        }
    }
    clients = {'ec2': ec2, 'iam': iam}
    monkeypatch.setattr(
        canary_worker_service.aws, 'assumed_client',
        lambda _role, service, _region, **_kwargs: clients[service])
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch',
                        lambda *_args, **_kwargs: 1000)

    with pytest.raises(ValueError, match='CANARY_DUPLICATE_CHILD'):
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    assert ec2.run_instances.call_count == (0 if persisted_child else 1)
    ec2.get_console_output.assert_not_called()
    ec2.terminate_instances.assert_called_once_with(
        InstanceIds=['i-other', 'i-primary'])


def test_eks_canary_fences_clients_and_verifies_teardown(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    payload = {
        'backend': 'aws_eks',
        'nonce': '2' * 32,
        'runtime_id': qualified.context,
    }
    reference = f'{target.registry}/qualification@{_DIGEST}'
    node = _eks_node('node-a', 'i-a')
    pod = SimpleNamespace(metadata=SimpleNamespace(labels={
        'skypilot.co/image-canary-operation': operation.id,
    }),
                          spec=SimpleNamespace(
                              containers=[SimpleNamespace(image=reference)],
                              node_name='node-a'),
                          status=SimpleNamespace(phase='Succeeded'))
    core = mock.Mock()
    core.api_client = SimpleNamespace(configuration=SimpleNamespace(
        host='https://eks.example'))
    core.list_node.return_value = SimpleNamespace(
        items=[node], metadata=SimpleNamespace(_continue=None))
    core.read_namespaced_pod.side_effect = (pod, _api_error(404))
    core.read_namespaced_pod_log.return_value = payload['nonce']
    core.read_node.return_value = node
    eks = mock.Mock()
    eks.describe_cluster.return_value = {
        'cluster': {
            'arn': qualified.cluster_arn,
            'endpoint': 'https://eks.example',
        }
    }
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-a',
                'IamInstanceProfile': {
                    'Arn': ('arn:aws:iam::210987654321:'
                            'instance-profile/EksNodeProfile'),
                },
            }]
        }]
    }
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Roles': [{
                'Arn': qualified.node_role,
            }]
        }
    }
    clients = {'eks': eks, 'ec2': ec2, 'iam': iam}
    provider_fences: list[object] = []

    def assumed_client(_role: object,
                       service: str,
                       _region: str,
                       *,
                       provider_fence: object = None) -> object:
        provider_fences.append(provider_fence)
        return clients[service]

    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client',
                        assumed_client)
    monkeypatch.setattr(canary_worker_service.kubernetes, 'core_api',
                        lambda _context: core)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch',
                        lambda *_args, **_kwargs: 1000)
    heartbeat = _OwnedHeartbeat()

    evidence = canary_worker_service._run_eks_canary(operation, payload,
                                                     _revision(profile),
                                                     profile, target, binding,
                                                     _DIGEST, reference,
                                                     heartbeat)

    assert evidence['node_uid'] == 'node-a'
    assert evidence['teardown_verified'] is True
    assert provider_fences == [
        heartbeat.assert_owned, heartbeat.assert_owned, heartbeat.assert_owned
    ]
    core.delete_namespaced_pod.assert_called_once()


def test_eks_teardown_catches_late_ambiguous_create(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    monkeypatch.setattr(
        canary_worker_service.time, 'sleep',
        lambda seconds: current.__setitem__(0, current[0] + seconds))
    monkeypatch.setattr(canary_worker_service, '_EKS_TEARDOWN_POLL_SECONDS', 1)
    monkeypatch.setattr(canary_worker_service, '_EKS_ABSENCE_SETTLE_SECONDS', 2)
    core = mock.Mock()
    core.delete_namespaced_pod.side_effect = (_api_error(404), None)
    core.read_namespaced_pod.side_effect = (_api_error(404),
                                            mock.sentinel.late_pod,
                                            _api_error(404), _api_error(404),
                                            _api_error(404))

    assert canary_worker_service._delete_eks_pod(core,
                                                 'canary-pod',
                                                 'canary-namespace',
                                                 settle_absence=True)
    assert core.delete_namespaced_pod.call_count == 2


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
                                     retry_delay_seconds=None,
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


def test_inventory_lease_loss_during_budget_wait_blocks_provider_call(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    shard = dataclasses.replace(_shard(profile),
                                inventory_started_at=10,
                                inventory_lease_token='inventory-token',
                                inventory_lease_expires_at=1000)
    revision = _revision(profile)
    events: list[str] = []
    lost = False

    class FencedHeartbeat:
        """Loses inventory ownership during the provider-budget wait."""

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
                raise worker_lease.LeaseLostError('inventory lease lost')

    class Repository:

        def __init__(self, hooks: aws.EcrCallHooks) -> None:
            self._hooks = hooks

        def inventory_page(self, **_kwargs: object):
            self._hooks.before_call()
            events.append('provider')
            return (), None

    def repository_from_role(*_args: object, **kwargs: object) -> Repository:
        events.append('sts')
        return Repository(kwargs['hooks'])

    def lose_during_budget(_shard: object) -> None:
        nonlocal lost
        events.append('budget')
        lost = True

    limiter = SimpleNamespace(before_call=lose_during_budget,
                              record_throttle=lambda _shard: None)
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', FencedHeartbeat)
    monkeypatch.setattr(copy_worker_service, '_profile_for_shard',
                        lambda _shard: (revision, profile))
    monkeypatch.setattr(copy_worker_service, '_expected_shard_attestation',
                        lambda *_args: ('live-key', {}))
    monkeypatch.setattr(copy_worker_service, '_matching_shard_metadata',
                        lambda *_args, **_kwargs: ({}, 100, 10))
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    abandon = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'abandon_inventory_claim', abandon)
    record_page = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_inventory_page', record_page)

    assert not copy_worker_service.reconcile_inventory(
        shard, limiter=limiter, lease_seconds=30)

    assert events == [
        'lease',
        'lease',
        'sts',
        'lease',
        'lease',
        'budget',
        'lease',
    ]
    assert 'provider' not in events
    record_page.assert_not_called()
    abandon.assert_called_once_with(shard.id,
                                    'inventory-token',
                                    shard.inventory_epoch,
                                    invalid_cursor=False)


def test_inventory_final_evidence_and_lease_release_are_one_operation(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    token = 'inventory-token'
    shard = dataclasses.replace(_shard(profile),
                                inventory_started_at=10,
                                inventory_lease_token=token,
                                inventory_lease_expires_at=1000)
    completed = dataclasses.replace(shard,
                                    state=models.ImageShardState.READY,
                                    inventory_epoch=3,
                                    inventory_completed_at=20,
                                    inventory_finalizing=True)
    revision = _revision(profile)
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.READY,
                                   lease_kind=None,
                                   lease_token=None,
                                   lease_expires_at=None)
    repository = mock.Mock()
    repository.inventory_page.return_value = ((), None)
    repository.exact_manifest_exists.return_value = False
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', _OwnedHeartbeat)
    monkeypatch.setattr(copy_worker_service, '_profile_for_shard',
                        lambda _shard: (revision, profile))
    monkeypatch.setattr(copy_worker_service, '_expected_shard_attestation',
                        lambda *_args: ('live-key', {}))
    monkeypatch.setattr(copy_worker_service, '_matching_shard_metadata',
                        lambda *_args, **_kwargs: ({}, 100, 10))
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: repository)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_inventory_page', lambda *_args: completed)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_inventory_missing_candidates',
                        lambda *_args, **_kwargs: [location])
    confirm = mock.Mock(return_value=location)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'complete_inventory_confirmation', confirm)
    finalize = mock.Mock(return_value=revision)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_inventory_attestation_and_release', finalize)

    assert copy_worker_service.reconcile_inventory(shard,
                                                   limiter=mock.Mock(),
                                                   lease_seconds=30)

    confirm.assert_called_once_with(location.id,
                                    shard.id,
                                    completed.inventory_epoch,
                                    token,
                                    present=False)
    assert finalize.call_args.kwargs['inventory_lease_token'] == token
    assert finalize.call_args.kwargs[
        'expected_inventory_epoch'] == completed.inventory_epoch
    assert finalize.call_args.kwargs['evidence'][
        'inventory_completed_at'] == completed.inventory_completed_at


def test_inventory_finalization_resume_skips_provider_listing(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    token = 'successor-token'
    shard = dataclasses.replace(_shard(profile),
                                inventory_epoch=3,
                                inventory_started_at=10,
                                inventory_completed_at=20,
                                inventory_finalizing=True,
                                inventory_lease_token=token,
                                inventory_lease_expires_at=1000)
    revision = _revision(profile)
    repository = mock.Mock()
    monkeypatch.setattr(copy_worker_service, '_LeaseHeartbeat', _OwnedHeartbeat)
    monkeypatch.setattr(copy_worker_service, '_profile_for_shard',
                        lambda _shard: (revision, profile))
    monkeypatch.setattr(copy_worker_service, '_expected_shard_attestation',
                        lambda *_args: ('live-key', {}))
    monkeypatch.setattr(copy_worker_service, '_matching_shard_metadata',
                        lambda *_args, **_kwargs: ({}, 100, 10))
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: repository)
    candidates = mock.Mock(return_value=[])
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_inventory_missing_candidates', candidates)
    finalize = mock.Mock(return_value=revision)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_inventory_attestation_and_release', finalize)

    assert copy_worker_service.reconcile_inventory(shard,
                                                   limiter=mock.Mock(),
                                                   lease_seconds=30)

    repository.inventory_page.assert_not_called()
    candidates.assert_called_once_with(shard.id,
                                       shard.inventory_epoch,
                                       limit=100)
    finalize.assert_called_once()


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
    mark_readback = mock.Mock(
        side_effect=lambda *_args: call_order.append('concluded') or True)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'mark_eviction_readback', mark_readback)

    def repository_from_role(role: aws.AwsRoleBinding, *_args: object,
                             **_kwargs: object) -> mock.Mock:
        roles.append(role)
        hooks = _kwargs['hooks']

        def delete_request_outcome(_digest: str) -> aws.DeleteRequestOutcome:
            hooks.before_call()
            call_order.append('delete')
            return aws.DeleteRequestOutcome.CONCLUDED

        def exact_manifest_exists(_digest: str) -> bool:
            hooks.before_call()
            call_order.append('readback')
            return False

        repository.delete_request_outcome.side_effect = (delete_request_outcome)
        repository.exact_manifest_exists.side_effect = exact_manifest_exists
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
    mark_readback.assert_called_once_with(location.id, location.lease_token)
    assert call_order == ['intent', 'delete', 'concluded', 'readback']


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
    cancel_delete = mock.Mock(return_value=True)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'cancel_eviction_delete', cancel_delete)

    def repository_from_role(*_args: object, **kwargs: object) -> mock.Mock:
        repository = mock.Mock()

        def delete_request_outcome(_digest: str) -> aws.DeleteRequestOutcome:
            kwargs['hooks'].before_call()
            return aws.DeleteRequestOutcome.NOT_STARTED

        repository.delete_request_outcome.side_effect = (delete_request_outcome)
        return repository

    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    complete = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', complete)

    assert not lifecycle_worker_service.evict_location(location, mock.Mock())
    cancel_delete.assert_called_once_with(location.id, location.lease_token)
    complete.assert_called_once_with(location.id,
                                     location.lease_token,
                                     present=None,
                                     provider_not_called=True)


def test_concluded_delete_readback_failure_remains_retryable(
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
    mark_readback = mock.Mock(return_value=True)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'mark_eviction_readback', mark_readback)
    readback_calls = 0

    def repository_from_role(*_args: object, **kwargs: object) -> mock.Mock:
        repository = mock.Mock()

        def delete_request_outcome(_digest: str) -> aws.DeleteRequestOutcome:
            kwargs['hooks'].before_call()
            return aws.DeleteRequestOutcome.CONCLUDED

        def exact_manifest_exists(_digest: str) -> bool:
            nonlocal readback_calls
            readback_calls += 1
            kwargs['hooks'].before_call()
            raise budgets.ProviderBudgetUnavailableError('retry readback')

        repository.delete_request_outcome.side_effect = (delete_request_outcome)
        repository.exact_manifest_exists.side_effect = exact_manifest_exists
        return repository

    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    complete = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', complete)

    assert not lifecycle_worker_service.evict_location(location, mock.Mock())

    mark_readback.assert_called_once_with(location.id, location.lease_token)
    assert readback_calls == 3
    complete.assert_not_called()


def test_reclaimed_readback_never_repeats_delete(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    location = dataclasses.replace(_copying_location(profile),
                                   state=models.ImageLocationState.EVICTING,
                                   lease_kind='READBACK')
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        lambda _: _shard(profile))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'get_profile_revision', lambda _: _revision(profile))
    monkeypatch.setattr(lifecycle_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(lifecycle_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    repository = mock.Mock()

    def repository_from_role(*_args: object, **kwargs: object) -> mock.Mock:

        def exact_manifest_exists(_digest: str) -> bool:
            kwargs['hooks'].before_call()
            return False

        repository.exact_manifest_exists.side_effect = exact_manifest_exists
        return repository

    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    complete = mock.Mock(return_value=location)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'complete_eviction', complete)
    begin_delete = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'begin_eviction_delete', begin_delete)

    assert lifecycle_worker_service.evict_location(location, mock.Mock())

    repository.delete_request_outcome.assert_not_called()
    begin_delete.assert_not_called()
    complete.assert_called_once_with(location.id,
                                     location.lease_token,
                                     present=False)


def test_lifecycle_lease_loss_before_sts_blocks_credential_acquisition(
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

    class LosingHeartbeat:
        """Loses lifecycle ownership immediately before credential STS."""

        def __init__(self, *_args: object) -> None:
            self.calls = 0

        def __enter__(self):
            self.assert_owned()
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def assert_owned(self) -> None:
            self.calls += 1
            if self.calls > 2:
                raise worker_lease.LeaseLostError('lease lost before STS')

    monkeypatch.setattr(lifecycle_worker_service, '_LeaseHeartbeat',
                        LosingHeartbeat)
    sts = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.aws.aws_adaptor, 'client',
                        lambda _service: sts)

    with pytest.raises(worker_lease.LeaseLostError,
                       match='lease lost before STS'):
        lifecycle_worker_service.evict_location(location, mock.Mock())

    sts.assume_role.assert_not_called()


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


@pytest.mark.parametrize(('worker_module', 'service_name'),
                         ((copy_worker_service, 'CopyWorkerService'),
                          (lifecycle_worker_service, 'LifecycleWorkerService'),
                          (canary_worker_service, 'CanaryWorkerService')),
                         ids=('copy', 'lifecycle', 'canary'))
def test_worker_main_verifies_all_databases_before_advertising_health(
        monkeypatch: pytest.MonkeyPatch, worker_module,
        service_name: str) -> None:
    calls: list[str] = []
    health = mock.Mock()
    health_server = mock.Mock()
    service = mock.Mock()
    monkeypatch.setattr(worker_module.database_migrations,
                        'initialize_central_databases',
                        lambda: calls.append('databases'))
    monkeypatch.setattr(
        worker_module.worker_health, 'WorkerHealth',
        mock.Mock(side_effect=lambda *_args, **_kwargs:
                  (calls.append('health') or health)))
    monkeypatch.setattr(worker_module.worker_health, 'HealthServer',
                        mock.Mock(return_value=health_server))
    monkeypatch.setattr(worker_module, service_name,
                        mock.Mock(return_value=service))
    monkeypatch.setattr(worker_module.signal, 'signal', mock.Mock())

    worker_module.main()

    assert calls == ['databases', 'health']
    health_server.start.assert_called_once_with()
    service.run_forever.assert_called_once_with()
    health_server.stop.assert_called_once_with()


@pytest.mark.parametrize(
    'worker_module',
    (copy_worker_service, lifecycle_worker_service, canary_worker_service),
    ids=('copy', 'lifecycle', 'canary'))
def test_worker_main_fails_before_health_when_central_schema_is_stale(
        monkeypatch: pytest.MonkeyPatch, worker_module) -> None:
    health_factory = mock.Mock()
    monkeypatch.setattr(
        worker_module.database_migrations, 'initialize_central_databases',
        mock.Mock(side_effect=RuntimeError('Serve schema is stale')))
    monkeypatch.setattr(worker_module.worker_health, 'WorkerHealth',
                        health_factory)

    with pytest.raises(RuntimeError, match='Serve schema is stale'):
        worker_module.main()

    health_factory.assert_not_called()
