"""Failure, fencing, and recovery tests for independently deployed workers."""
# pylint: disable=protected-access

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.container_images import aws
from sky.container_images import canary_worker_service
from sky.container_images import catalog_state
from sky.container_images import copy_worker_service
from sky.container_images import demand_state
from sky.container_images import lifecycle_worker_service
from sky.container_images import models
from sky.container_images import topology_state

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
        consumer_owner='orphan-cluster',
        consumer_generation=0,
        target_key='artifact:target',
        owner_epoch=1,
        retry_epoch=0,
        image_id=_ARTIFACT_ID,
        runtime_digest=_DIGEST,
        profile_revision_id=_REVISION_ID,
        target_fingerprint='f' * 64,
        location_id=_LOCATION_ID,
        placement={'consumer': {
            'request_id': 'request-id'
        }},
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


def test_unattached_cluster_demand_is_observed_after_bounded_retention(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(
        created_at=current -
        lifecycle_worker_service._UNATTACHED_REQUEST_RETENTION_SECONDS)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_status_fields', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock()
    observe = mock.Mock(return_value=False)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'observe_consumer_terminal', observe)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_not_called()
    observe.assert_called_once_with(demand.id,
                                    demand.workspace,
                                    authoritative=True,
                                    now=current)


def test_unattached_cluster_demand_is_not_released_early(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(
        created_at=current -
        lifecycle_worker_service._UNATTACHED_REQUEST_RETENTION_SECONDS + 1)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_status_fields', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock(return_value=True)
    observe = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'observe_consumer_terminal', observe)

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 0
    defer.assert_called_once_with(demand.id, now=current)
    observe.assert_not_called()


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

    def __enter__(self) -> '_OwnedHeartbeat':
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
