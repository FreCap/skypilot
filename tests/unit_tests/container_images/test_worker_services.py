"""Failure, fencing, and recovery tests for independently deployed workers."""
# pylint: disable=protected-access

from __future__ import annotations

import base64
import contextlib
import dataclasses
import json
import pathlib
import socket
import threading
import types
from types import SimpleNamespace
from typing import Any
from unittest import mock
import urllib.error
import urllib.request

import pytest

from sky import exceptions
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
from sky.provision import docker_utils

_DIGEST = 'sha256:' + 'a' * 64
_CONFIG_DIGEST = 'sha256:' + 'b' * 64
_ARTIFACT_ID = '00000000-0000-4000-8000-000000000001'
_LOCATION_ID = '00000000-0000-4000-8000-000000000002'
_SHARD_ID = '00000000-0000-4000-8000-000000000003'
_REVISION_ID = '00000000-0000-4000-8000-000000000004'


def test_canary_credential_helper_can_remove_baked_docker_auth() -> None:
    server = '123456789012.dkr.ecr.us-west-2.amazonaws.com'
    ordinary = docker_utils.credential_helper_config_cmd(server)
    canary = docker_utils.credential_helper_config_cmd(server,
                                                       clear_cached_auth=True)

    assert 'a=c.get("auths")' not in ordinary
    assert 'a=c.get("auths")' in canary
    assert 'a.pop(' in canary
    assert server in canary


@pytest.fixture(autouse=True)
def _persist_canary_profile_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        canary_worker_service.qualification,
        'record_canary_ec2_instance_profile',
        lambda *_args, **_kwargs: True,
    )


@pytest.mark.parametrize(
    ('output', 'expected'),
    (
        ('boot log\nSKYPILOT_IMAGE_CANARY_SUCCESS:nonce\n', True),
        (base64.b64encode(
            b'boot log\nSKYPILOT_IMAGE_CANARY_SUCCESS:nonce\n').decode(), True),
        ('boot log without marker', False),
        ('not valid base64 \u2026', False),
        (None, False),
    ),
)
def test_ec2_console_marker_supports_sdk_response_shapes(
        output: object, expected: bool) -> None:
    assert canary_worker_service._console_has_marker(
        output, 'SKYPILOT_IMAGE_CANARY_SUCCESS:nonce') is expected


def _console_clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    clock = [100.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: clock[0])
    monkeypatch.setattr(
        canary_worker_service.time, 'sleep',
        lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(canary_worker_service, '_EC2_CONSOLE_SETTLE_SECONDS',
                        10)
    monkeypatch.setattr(canary_worker_service, '_EC2_CONSOLE_POLL_SECONDS', 5)
    return clock


def test_ec2_console_marker_waits_for_delayed_terminal_buffer(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _console_clock(monkeypatch)
    ec2 = mock.Mock()
    marker = 'SKYPILOT_IMAGE_CANARY_SUCCESS:nonce'
    ec2.get_console_output.side_effect = ({'Output': ''}, {'Output': marker})
    heartbeat = SimpleNamespace(assert_owned=mock.Mock())

    assert canary_worker_service._wait_for_console_marker(
        ec2, 'i-canary', marker, 1000.0, heartbeat)

    assert ec2.get_console_output.call_args_list == [
        mock.call(InstanceId='i-canary', Latest=True),
        mock.call(InstanceId='i-canary', Latest=True),
    ]


def test_ec2_console_marker_exhausts_bounded_settle_window(
        monkeypatch: pytest.MonkeyPatch) -> None:
    clock = _console_clock(monkeypatch)
    ec2 = mock.Mock()
    ec2.get_console_output.return_value = {'Output': 'partial boot log'}
    heartbeat = SimpleNamespace(assert_owned=mock.Mock())

    assert not canary_worker_service._wait_for_console_marker(
        ec2, 'i-canary', 'SKYPILOT_IMAGE_CANARY_SUCCESS:nonce', 1000.0,
        heartbeat)

    assert clock == [110.0]
    assert ec2.get_console_output.call_count == 2


def test_ec2_console_marker_preserves_original_canary_deadline(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _console_clock(monkeypatch)
    ec2 = mock.Mock()
    ec2.get_console_output.return_value = {'Output': ''}
    heartbeat = SimpleNamespace(assert_owned=mock.Mock())

    with pytest.raises(ValueError, match='CANARY_TIMEOUT'):
        canary_worker_service._wait_for_console_marker(
            ec2, 'i-canary', 'SKYPILOT_IMAGE_CANARY_SUCCESS:nonce', 105.0,
            heartbeat)

    ec2.get_console_output.assert_called_once_with(InstanceId='i-canary',
                                                   Latest=True)


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


def test_canary_contract_rejects_deleted_copy_before_launch_but_allows_cleanup(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.targets[0]
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    runtime_id = canary_worker_service.qualification.runtime_ids(
        target, 'aws_vm', binding)[0]
    repository_name = f'{target.repository_prefix}/qualification'
    revision = dataclasses.replace(
        _revision(profile),
        attestations={
            models.profile_attestation_key('terraform_target', target.name): {
                'status': 'READY',
                'target_fingerprint': target.target_fingerprint,
                'registry': target.registry,
                'repository_name': repository_name,
                'repository_arn': 'qualification-repository-arn',
            },
            models.profile_attestation_key('copy', target.name): {
                'status': 'READY',
                'observed_at': 100,
                'target_fingerprint': target.target_fingerprint,
                'runtime_digest': _DIGEST,
                'platform': profile.qualification.canary_platform,
            },
            models.profile_attestation_key('lifecycle', target.name): {
                'status': 'READY',
                'observed_at': 101,
                'target_fingerprint': target.target_fingerprint,
                'runtime_digest': _DIGEST,
                'exact_absence': True,
            },
        })
    payload = {
        'profile_revision_id': revision.id,
        'desired_generation': revision.desired_generation,
        'config_hash': revision.config_hash,
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'backend': 'aws_vm',
        'binding_id': binding.id,
        'binding_fingerprint': binding.fingerprint,
        'runtime_id': runtime_id,
        'nonce': '1' * 32,
        'worst_case_microusd': 10_000,
        'timeout_seconds': 900,
    }
    operation = dataclasses.replace(_canary_operation(), result=payload)
    monkeypatch.setattr(canary_worker_service.topology_state,
                        'get_profile_revision', lambda _revision_id: revision)

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        canary_worker_service._load_contract(operation)

    persisted = dataclasses.replace(
        operation, child_launch_id=f'ec2:{target.region}:{operation.id}')
    loaded = canary_worker_service._load_contract(persisted)

    assert loaded[1].id == revision.id
    assert loaded[5] == _DIGEST


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


def test_terminal_reconciliation_stops_between_independent_demands(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    first = dataclasses.replace(_cluster_demand(created_at=current - 100_000),
                                id='demand-one',
                                consumer_kind='unknown',
                                placement={'consumer': {}})
    second = dataclasses.replace(first, id='demand-two')
    stop = threading.Event()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [first, second])
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    defer = mock.Mock(side_effect=lambda *_args, **_kwargs: stop.set())
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'defer_consumer_reconciliation', defer)

    assert lifecycle_worker_service._reconcile_terminal_consumers(
        current, should_stop=stop.is_set) == 0

    defer.assert_called_once_with(first.id, now=current)


def test_terminal_reconciliation_stops_between_bulk_queries(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = 200_000
    demand = _cluster_demand(created_at=current - 100_000)
    stop = threading.Event()
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'list_consumer_reconciliation_candidates',
                        lambda **_: [demand])
    monkeypatch.setattr(
        lifecycle_worker_service.global_user_state,
        'get_cluster_image_consumers',
        mock.Mock(side_effect=lambda _names: (stop.set() or {})))
    service_states = mock.Mock()
    job_states = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', service_states)
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', job_states)

    assert lifecycle_worker_service._reconcile_terminal_consumers(
        current, should_stop=stop.is_set) == 0

    service_states.assert_not_called()
    job_states.assert_not_called()


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
        mock.Mock(side_effect=canary_worker_service._CanaryTeardownFailed()))
    fail = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)

    assert not canary_worker_service.run_canary(operation)
    fail.assert_not_called()


def test_provider_teardown_code_collision_terminalizes_as_failure(
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
    provider_error = ValueError('CANARY_TEARDOWN_FAILED')
    monkeypatch.setattr(canary_worker_service, '_run_ec2_canary',
                        mock.Mock(side_effect=provider_error))
    fail = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)

    assert not canary_worker_service.run_canary(operation)
    fail.assert_called_once_with(operation,
                                 'CANARY_FAILED',
                                 teardown_verified=True)


def test_unstarted_canary_drain_does_not_load_or_terminalize(
        monkeypatch: pytest.MonkeyPatch) -> None:
    operation = _canary_operation()
    drain_event = threading.Event()
    drain_event.set()
    load_contract = mock.Mock()
    fail = mock.Mock()
    release = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service, '_load_contract', load_contract)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'release_drained_canary', release)

    assert not canary_worker_service.run_canary(operation,
                                                drain_event=drain_event)

    load_contract.assert_not_called()
    fail.assert_not_called()
    release.assert_called_once_with(operation, teardown_verified=True)


def test_canary_drain_is_forwarded_without_terminalizing(
        monkeypatch: pytest.MonkeyPatch) -> None:
    operation = _canary_operation()
    drain_event = threading.Event()
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        ({
            'backend': 'aws_vm'
        }, mock.sentinel.revision, mock.sentinel.profile, mock.sentinel.target,
         mock.sentinel.binding, _DIGEST, mock.sentinel.reference))
    run_ec2 = mock.Mock(
        side_effect=canary_worker_service._CanaryDrainRequested())
    fail = mock.Mock()
    release = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service, '_run_ec2_canary', run_ec2)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'release_drained_canary', release)

    assert not canary_worker_service.run_canary(operation,
                                                drain_event=drain_event)

    assert run_ec2.call_args.kwargs['drain_event'] is drain_event
    fail.assert_not_called()
    release.assert_called_once_with(operation, teardown_verified=True)


@pytest.mark.parametrize('backend', ['aws_vm', 'aws_eks'])
def test_canary_drain_during_verified_teardown_releases_without_terminalizing(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        backend: str) -> None:
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding(backend)
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    runtime_id = (target.region if backend == 'aws_vm' else
                  binding.qualified_clusters[0].context)
    operation = dataclasses.replace(_canary_operation(),
                                    child_launch_id=f'{backend}:persisted')
    payload = {
        'backend': backend,
        'nonce': '5' * 32,
        'runtime_id': runtime_id,
        'timeout_seconds': 900,
    }
    drain_event = threading.Event()
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        (payload, _revision(profile), profile, target, binding, _DIGEST,
         f'{target.registry}/qualification@{_DIGEST}'))
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    teardown = mock.Mock(
        side_effect=lambda *_args, **_kwargs: (drain_event.set() or True))
    if backend == 'aws_vm':
        monkeypatch.setattr(canary_worker_service, '_assumed_client',
                            lambda *_args, **_kwargs: mock.Mock())
        monkeypatch.setattr(
            canary_worker_service, '_tagged_instances',
            mock.Mock(side_effect=(RuntimeError('provider read failed'), [])))
        monkeypatch.setattr(canary_worker_service, '_terminate_ec2_instances',
                            teardown)
    else:
        core = mock.Mock()
        eks = mock.Mock()
        eks.describe_cluster.side_effect = RuntimeError('provider read failed')
        monkeypatch.setattr(canary_worker_service, '_kubernetes_core',
                            lambda *_args, **_kwargs: core)
        monkeypatch.setattr(canary_worker_service, '_assumed_client',
                            lambda *_args, **_kwargs: eks)
        monkeypatch.setattr(canary_worker_service,
                            '_authorized_launch_deadline',
                            lambda *_args, **_kwargs: 1000.0)
        monkeypatch.setattr(canary_worker_service, '_delete_eks_pod', teardown)
    complete = mock.Mock()
    fail = mock.Mock()
    release = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification, 'complete_canary',
                        complete)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'release_drained_canary', release)

    assert not canary_worker_service.run_canary(operation,
                                                drain_event=drain_event)

    teardown.assert_called_once()
    complete.assert_not_called()
    fail.assert_not_called()
    release.assert_called_once_with(operation, teardown_verified=True)


@pytest.mark.parametrize(
    'error', [ValueError('CANARY_PULL_FAILED'),
              RuntimeError('boom')])
def test_canary_drain_after_verified_helper_error_releases_without_failure(
        monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    operation = _canary_operation()
    drain_event = threading.Event()
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        ({
            'backend': 'aws_vm'
        }, mock.sentinel.revision, mock.sentinel.profile, mock.sentinel.target,
         mock.sentinel.binding, _DIGEST, mock.sentinel.reference))

    def finish_then_fail(*_args: object, **_kwargs: object) -> None:
        drain_event.set()
        raise error

    monkeypatch.setattr(canary_worker_service, '_run_ec2_canary',
                        finish_then_fail)
    fail = mock.Mock()
    release = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', fail)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'release_drained_canary', release)

    assert not canary_worker_service.run_canary(operation,
                                                drain_event=drain_event)

    fail.assert_not_called()
    release.assert_called_once_with(operation, teardown_verified=True)


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


def test_ec2_drain_of_persisted_child_runs_uncancelled_teardown(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    operation = dataclasses.replace(
        _canary_operation(),
        child_launch_id=f'ec2:{target.region}:{_canary_operation().id}')
    payload = {
        'backend': 'aws_vm',
        'nonce': '3' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }
    drain_event = threading.Event()
    drain_event.set()
    heartbeat = _OwnedHeartbeat()
    ec2 = mock.Mock()
    acquisitions = []
    tagged_instances = mock.Mock(return_value=[])
    terminate = mock.Mock(return_value=True)

    def assumed_client(*_args, **kwargs):
        acquisitions.append(kwargs)
        return ec2

    monkeypatch.setattr(canary_worker_service, '_assumed_client',
                        assumed_client)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service, '_tagged_instances',
                        tagged_instances)
    monkeypatch.setattr(canary_worker_service, '_terminate_ec2_instances',
                        terminate)

    with pytest.raises(canary_worker_service._CanaryDrainRequested):
        canary_worker_service._run_ec2_canary(
            operation,
            payload,
            _revision(profile),
            profile,
            target,
            binding,
            _DIGEST,
            f'{target.registry}/qualification@{_DIGEST}',
            heartbeat,
            drain_event=drain_event)

    ec2.run_instances.assert_not_called()
    assert len(acquisitions) == 1
    assert acquisitions[0]['cleanup_deadline'] == (
        tagged_instances.call_args.kwargs['cleanup_deadline'])
    assert 'drain_event' not in acquisitions[0]
    tagged_instances.assert_called_once()
    assert tagged_instances.call_args.args == (ec2, operation.id)
    assert isinstance(tagged_instances.call_args.kwargs['cleanup_deadline'],
                      float)
    terminate.assert_called_once()


def test_eks_drain_of_persisted_child_runs_uncancelled_teardown(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_eks')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    qualified = binding.qualified_clusters[0]
    operation = dataclasses.replace(_canary_operation(),
                                    child_launch_id='eks:persisted-child')
    payload = {
        'backend': 'aws_eks',
        'nonce': '4' * 32,
        'runtime_id': qualified.context,
        'timeout_seconds': 900,
    }
    drain_event = threading.Event()
    drain_event.set()
    heartbeat = _OwnedHeartbeat()
    cleanup_core = mock.Mock()
    acquisitions = []

    def kubernetes_core(*_args, **kwargs):
        acquisitions.append(kwargs)
        if kwargs.get('drain_event') is drain_event:
            raise canary_worker_service._CanaryDrainRequested()
        assert kwargs.get('cleanup_deadline') is not None
        return cleanup_core

    delete_pod = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service, '_kubernetes_core',
                        kubernetes_core)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service, '_delete_eks_pod', delete_pod)

    with pytest.raises(canary_worker_service._CanaryDrainRequested):
        canary_worker_service._run_eks_canary(
            operation,
            payload,
            _revision(profile),
            profile,
            target,
            binding,
            _DIGEST,
            f'{target.registry}/qualification@{_DIGEST}',
            heartbeat,
            drain_event=drain_event)

    assert len(acquisitions) == 2
    assert acquisitions[0]['drain_event'] is drain_event
    assert 'cleanup_deadline' not in acquisitions[0]
    assert acquisitions[1]['cleanup_deadline'] == (
        delete_pod.call_args.kwargs['cleanup_deadline'])
    cleanup_core.create_namespaced_pod.assert_not_called()
    delete_pod.assert_called_once()


def test_eks_dynamic_credential_refresh_observes_drain_before_raw_call(
        monkeypatch: pytest.MonkeyPatch) -> None:
    drain_event = threading.Event()
    initial = SimpleNamespace(api_client=mock.Mock(), list_node=mock.Mock())
    replacement = SimpleNamespace(api_client=mock.Mock(), list_node=mock.Mock())

    def build_isolated(*_args, **_kwargs):
        drain_event.set()
        return canary_worker_service.kubernetes._BoundedCoreApiResult(
            replacement, None, None, None)

    monkeypatch.setattr(canary_worker_service.kubernetes, '_bounded_core_api',
                        lambda *_args, **_kwargs: (initial, None))
    monkeypatch.setattr(canary_worker_service.kubernetes,
                        '_bounded_core_api_isolated', build_isolated)
    core = canary_worker_service.kubernetes.ProviderFencedCoreApi(
        'bounded-context',
        exec_credential_timeout_seconds=2,
        provider_fence=lambda: None)
    monkeypatch.setattr(core, '_should_refresh', lambda: True)
    fenced = canary_worker_service._FencedClient(core, _OwnedHeartbeat())

    with pytest.raises(canary_worker_service._CanaryDrainRequested):
        canary_worker_service._ordinary_provider_call(fenced, 'list_node',
                                                      drain_event)

    initial.list_node.assert_not_called()
    replacement.list_node.assert_not_called()
    initial.api_client.close.assert_called_once_with()
    replacement.api_client.close.assert_called_once_with()


def _installed_eks_fenced_client(
    marker: str,
    heartbeat: Any,
    *,
    host: str = 'https://eks.example'
) -> tuple[Any, Any, SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    configuration = SimpleNamespace(host=host,
                                    api_key={'authorization': marker},
                                    api_key_prefix={'authorization': 'Bearer'},
                                    username=marker,
                                    password=marker,
                                    cert_file=marker,
                                    key_file=marker,
                                    refresh_api_key_hook=None)
    api_client = SimpleNamespace(configuration=configuration,
                                 default_headers={'Authorization': marker},
                                 cookie=marker,
                                 close=mock.Mock())
    raw_core = SimpleNamespace(api_client=api_client, list_node=mock.Mock())
    core = object.__new__(
        canary_worker_service.kubernetes.ProviderFencedCoreApi)
    core._context = 'bounded-context'
    core._exec_credential_timeout_seconds = 2
    core._refresh_lock = threading.Lock()
    core._client = raw_core
    core._credential_refresh_deadline = None
    core._last_refresh_monotonic = 0.0
    fenced = canary_worker_service._FencedClient(core, heartbeat)
    return fenced, core, configuration, api_client, raw_core


@pytest.mark.parametrize('failure_kind', ('lease', 'drain', 'deadline'))
def test_eks_initial_canary_fence_scrubs_installed_credential_before_escape(
        monkeypatch: pytest.MonkeyPatch, failure_kind: str) -> None:
    marker = 'INITIAL_CANARY_FENCE_EXEC_TOKEN'
    heartbeat = mock.Mock(spec=['assert_owned'])
    fenced, core, configuration, api_client, raw_core = (
        _installed_eks_fenced_client(marker, heartbeat))
    drain_event = None
    expected_error: type[BaseException]
    if failure_kind == 'lease':
        heartbeat.assert_owned.side_effect = worker_lease.LeaseLostError(
            'canary lease lost')
        expected_error = worker_lease.LeaseLostError
    elif failure_kind == 'drain':
        drain_event = threading.Event()
        drain_event.set()
        expected_error = canary_worker_service._CanaryDrainRequested
    else:
        monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                            lambda: 2.0)
        expected_error = canary_worker_service._CanaryCleanupDeadlineExceeded

    with pytest.raises(expected_error) as exc_info:
        if failure_kind == 'deadline':
            fenced.call_before_cleanup_deadline('list_node', 1.0)
        else:
            canary_worker_service._ordinary_provider_call(
                fenced, 'list_node', drain_event)

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in str(error)
    _assert_canary_traceback_value_free(error, marker)
    raw_core.list_node.assert_not_called()
    assert core._client is None
    assert configuration.api_key == {}
    assert configuration.api_key_prefix == {}
    assert configuration.username is None
    assert configuration.password is None
    assert configuration.cert_file is None
    assert configuration.key_file is None
    assert api_client.default_headers == {}
    assert api_client.cookie is None
    api_client.close.assert_called_once_with()


def test_eks_outer_validation_failure_scrubs_installed_credential_before_escape(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    marker = 'OUTER_EKS_VALIDATION_EXEC_TOKEN'
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    operation = _canary_operation()
    payload = {
        'backend': 'aws_eks',
        'nonce': '2' * 32,
        'runtime_id': qualified.context,
        'timeout_seconds': 900,
    }
    heartbeat = _OwnedHeartbeat()
    fenced, core, configuration, api_client, _ = (_installed_eks_fenced_client(
        marker, heartbeat, host='https://wrong.example'))
    eks = mock.Mock()
    eks.describe_cluster.return_value = {
        'cluster': {
            'arn': qualified.cluster_arn,
            'endpoint': 'https://right.example',
        }
    }
    monkeypatch.setattr(canary_worker_service, '_canary_role',
                        lambda *_args, **_kwargs: mock.sentinel.role)
    monkeypatch.setattr(canary_worker_service, '_attach_canary_child',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(canary_worker_service, '_kubernetes_core',
                        lambda *_args, **_kwargs: fenced)
    monkeypatch.setattr(
        canary_worker_service, '_authorized_launch_deadline',
        lambda *_args, **_kwargs: canary_worker_service.time.monotonic() + 30)
    monkeypatch.setattr(canary_worker_service, '_assumed_client',
                        lambda *_args, **_kwargs: eks)

    with pytest.raises(ValueError,
                       match='CANARY_PRINCIPAL_UNVERIFIED') as exc_info:
        canary_worker_service._run_eks_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}', heartbeat)

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in str(error)
    _assert_canary_traceback_value_free(error, marker)
    assert fenced._client is None
    assert core._client is None
    assert configuration.api_key == {}
    assert configuration.api_key_prefix == {}
    assert api_client.default_headers == {}
    assert api_client.cookie is None
    api_client.close.assert_called_once_with()


def test_eks_success_scrubs_installed_credential_before_return(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    marker = 'OUTER_EKS_SUCCESS_EXEC_TOKEN'
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    operation = _canary_operation()
    payload = {
        'backend': 'aws_eks',
        'nonce': '2' * 32,
        'runtime_id': qualified.context,
        'timeout_seconds': 900,
    }
    reference = f'{target.registry}/qualification@{_DIGEST}'
    heartbeat = _OwnedHeartbeat()
    fenced, core, configuration, api_client, raw_core = (
        _installed_eks_fenced_client(marker, heartbeat))
    monkeypatch.setattr(core, '_should_refresh', lambda: False)
    node = _eks_node('node-a', 'i-a')
    pod = SimpleNamespace(metadata=SimpleNamespace(labels={
        'skypilot.co/image-canary-operation': operation.id,
    }),
                          spec=SimpleNamespace(
                              containers=[SimpleNamespace(image=reference)],
                              node_name='node-a'),
                          status=SimpleNamespace(phase='Succeeded'))
    raw_core.create_namespaced_pod = mock.Mock()
    raw_core.read_namespaced_pod = mock.Mock(return_value=pod)
    raw_core.read_namespaced_pod_log = mock.Mock(return_value=payload['nonce'])
    raw_core.read_node = mock.Mock(return_value=node)
    eks = mock.Mock()
    eks.describe_cluster.return_value = {
        'cluster': {
            'arn': qualified.cluster_arn,
            'endpoint': 'https://eks.example',
        }
    }
    monkeypatch.setattr(canary_worker_service, '_canary_role',
                        lambda *_args, **_kwargs: mock.sentinel.role)
    monkeypatch.setattr(canary_worker_service, '_attach_canary_child',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(canary_worker_service, '_kubernetes_core',
                        lambda *_args, **_kwargs: fenced)
    monkeypatch.setattr(
        canary_worker_service, '_authorized_launch_deadline',
        lambda *_args, **_kwargs: canary_worker_service.time.monotonic() + 30)
    monkeypatch.setattr(canary_worker_service, '_assumed_client',
                        lambda *_args, **_kwargs: eks)
    monkeypatch.setattr(canary_worker_service, '_qualified_eks_nodes',
                        lambda *_args, **_kwargs: (1, 'f' * 64))
    monkeypatch.setattr(canary_worker_service, '_delete_eks_pod',
                        lambda *_args, **_kwargs: True)

    evidence = canary_worker_service._run_eks_canary(operation, payload,
                                                     _revision(profile),
                                                     profile, target, binding,
                                                     _DIGEST, reference,
                                                     heartbeat)

    assert evidence['teardown_verified']
    assert fenced._client is None
    assert core._client is None
    assert configuration.api_key == {}
    assert configuration.api_key_prefix == {}
    assert api_client.default_headers == {}
    assert api_client.cookie is None
    api_client.close.assert_called_once_with()
    raw_core.create_namespaced_pod.assert_called_once()
    raw_core.read_namespaced_pod.assert_called_once()
    raw_core.read_namespaced_pod_log.assert_called_once()
    raw_core.read_node.assert_called_once()


@pytest.mark.parametrize(
    ('winner', 'expected_type', 'expected_message'),
    [
        ('provider', RuntimeError, 'LOSING_EKS_PROVIDER_SECRET'),
        ('provider-code-collision', ValueError, 'CANARY_TEARDOWN_FAILED'),
        ('teardown', ValueError, 'CANARY_TEARDOWN_FAILED'),
        ('lease', worker_lease.LeaseLostError, 'cleanup lease lost'),
        ('drain', canary_worker_service._CanaryDrainRequested, ''),
    ],
)
def test_eks_cleanup_precedence_handles_provider_state_after_scrub(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        winner: str, expected_type: type[Exception],
        expected_message: str) -> None:
    marker = 'LOSING_EKS_PROVIDER_SECRET'
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    operation = _canary_operation()
    payload = {
        'backend': 'aws_eks',
        'nonce': '2' * 32,
        'runtime_id': qualified.context,
        'timeout_seconds': 900,
    }
    reference = f'{target.registry}/qualification@{_DIGEST}'
    heartbeat = _OwnedHeartbeat()
    drain_event = threading.Event()
    fenced, core, configuration, api_client, raw_core = (
        _installed_eks_fenced_client('INSTALLED_EXEC_TOKEN', heartbeat))
    monkeypatch.setattr(core, '_should_refresh', lambda: False)
    if winner == 'provider-code-collision':
        provider_error = ValueError('CANARY_TEARDOWN_FAILED')
    else:
        provider_error = RuntimeError(marker)
    provider_error.response = {  # type: ignore[attr-defined]
        'headers': {
            'Authorization': f'Bearer {marker}',
        },
        'body': marker,
    }
    raw_core.create_namespaced_pod = mock.Mock()
    raw_core.read_namespaced_pod = mock.Mock(side_effect=provider_error)
    eks = mock.Mock()
    eks.describe_cluster.return_value = {
        'cluster': {
            'arn': qualified.cluster_arn,
            'endpoint': 'https://eks.example',
        }
    }
    monkeypatch.setattr(canary_worker_service, '_canary_role',
                        lambda *_args, **_kwargs: mock.sentinel.role)
    monkeypatch.setattr(canary_worker_service, '_attach_canary_child',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(canary_worker_service, '_kubernetes_core',
                        lambda *_args, **_kwargs: fenced)
    monkeypatch.setattr(
        canary_worker_service, '_authorized_launch_deadline',
        lambda *_args, **_kwargs: canary_worker_service.time.monotonic() + 30)
    monkeypatch.setattr(canary_worker_service, '_assumed_client',
                        lambda *_args, **_kwargs: eks)
    monkeypatch.setattr(canary_worker_service, '_qualified_eks_nodes',
                        lambda *_args, **_kwargs: (1, 'f' * 64))

    def cleanup(*_args, **_kwargs):
        if winner in ('provider', 'provider-code-collision'):
            return True
        if winner == 'teardown':
            return False
        if winner == 'lease':
            raise worker_lease.LeaseLostError('cleanup lease lost')
        assert winner == 'drain'
        drain_event.set()
        return True

    monkeypatch.setattr(canary_worker_service, '_delete_eks_pod', cleanup)

    with pytest.raises(expected_type, match=expected_message) as exc_info:
        canary_worker_service._run_eks_canary(operation,
                                              payload,
                                              _revision(profile),
                                              profile,
                                              target,
                                              binding,
                                              _DIGEST,
                                              reference,
                                              heartbeat,
                                              drain_event=drain_event)

    error = exc_info.value
    rendered = json.dumps(exceptions.serialize_exception(error),
                          default=str,
                          sort_keys=True)
    assert error.__cause__ is None
    assert error.__context__ is None
    if winner in ('provider', 'provider-code-collision'):
        assert error is provider_error
        assert marker in rendered
    else:
        assert marker not in rendered
        _assert_canary_traceback_value_free(error, marker)
    if winner == 'teardown':
        assert isinstance(error, canary_worker_service._CanaryTeardownFailed)
    assert fenced._client is None
    assert core._client is None
    assert configuration.api_key == {}
    assert configuration.api_key_prefix == {}
    assert api_client.default_headers == {}
    assert api_client.cookie is None
    api_client.close.assert_called_once_with()
    raw_core.create_namespaced_pod.assert_called_once()
    raw_core.read_namespaced_pod.assert_called_once()


def test_ec2_teardown_failure_drops_losing_provider_state(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    marker = 'LOSING_EC2_PROVIDER_SECRET'
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    operation = _canary_operation()
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }
    heartbeat = _OwnedHeartbeat()
    runtime_ec2 = mock.Mock()
    cleanup_ec2 = mock.Mock()
    iam = mock.Mock()
    clients = iter((runtime_ec2, iam, cleanup_ec2))
    provider_error = RuntimeError(marker)
    provider_error.response = {  # type: ignore[attr-defined]
        'headers': {
            'Authorization': f'Bearer {marker}',
        },
        'body': marker,
    }

    def launch(_method_name: str, _deadline: float, on_start, **_kwargs):
        on_start()
        return {'Instances': [{'InstanceId': 'i-canary'}]}

    runtime_ec2.call_before_deadline.side_effect = launch
    tagged_instances = mock.Mock(side_effect=([], provider_error, []))
    terminate = mock.Mock(return_value=False)
    monkeypatch.setattr(canary_worker_service, '_canary_role',
                        lambda *_args, **_kwargs: mock.sentinel.role)
    monkeypatch.setattr(canary_worker_service, '_attach_canary_child',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(canary_worker_service, '_assumed_client',
                        lambda *_args, **_kwargs: next(clients))
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(
        canary_worker_service, '_authorized_launch_deadline',
        lambda *_args, **_kwargs: canary_worker_service.time.monotonic() + 30)
    monkeypatch.setattr(
        canary_worker_service, '_instance_profile_identity',
        lambda *_args, **_kwargs:
        (models.ec2_instance_profile_arn(binding), binding.principals[0]))
    monkeypatch.setattr(canary_worker_service, '_tagged_instances',
                        tagged_instances)
    monkeypatch.setattr(canary_worker_service, '_terminate_ec2_instances',
                        terminate)

    with pytest.raises(ValueError, match='CANARY_TEARDOWN_FAILED') as exc_info:
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}', heartbeat)

    error = exc_info.value
    rendered = json.dumps(exceptions.serialize_exception(error),
                          default=str,
                          sort_keys=True)
    assert isinstance(error, canary_worker_service._CanaryTeardownFailed)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in rendered
    _assert_canary_traceback_value_free(error, marker)
    runtime_ec2.call_before_deadline.assert_called_once()
    assert tagged_instances.call_count == 3
    terminate.assert_called_once()


def test_eks_cleanup_refresh_observes_shared_deadline_before_raw_delete(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    initial = SimpleNamespace(api_client=mock.Mock(),
                              delete_namespaced_pod=mock.Mock())
    replacement = SimpleNamespace(api_client=mock.Mock(),
                                  delete_namespaced_pod=mock.Mock())

    def build_isolated(*_args, **_kwargs):
        current[0] = 61.0
        return canary_worker_service.kubernetes._BoundedCoreApiResult(
            replacement, None, None, None)

    monkeypatch.setattr(canary_worker_service.kubernetes, '_bounded_core_api',
                        lambda *_args, **_kwargs: (initial, None))
    monkeypatch.setattr(canary_worker_service.kubernetes,
                        '_bounded_core_api_isolated', build_isolated)
    core = canary_worker_service.kubernetes.ProviderFencedCoreApi(
        'bounded-context',
        exec_credential_timeout_seconds=2,
        provider_fence=lambda: None)
    monkeypatch.setattr(core, '_should_refresh', lambda: True)
    fenced = canary_worker_service._FencedClient(core, _OwnedHeartbeat())

    assert not canary_worker_service._delete_eks_pod(fenced,
                                                     'canary-pod',
                                                     'default',
                                                     settle_absence=False,
                                                     cleanup_deadline=60.0)

    initial.delete_namespaced_pod.assert_not_called()
    replacement.delete_namespaced_pod.assert_not_called()
    initial.api_client.close.assert_called_once_with()
    replacement.api_client.close.assert_called_once_with()


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
                        lambda _iam, _name, **_kwargs: qualified.node_role)
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
        lambda _iam, name, **_kwargs: qualified.node_role
        if name == 'qualified' else 'arn:aws:iam::123:role/OtherNodeRole')

    with pytest.raises(ValueError,
                       match='QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED'):
        heartbeat = _OwnedHeartbeat()
        fenced_core = canary_worker_service._FencedClient(core, heartbeat)
        canary_worker_service._qualified_eks_nodes(fenced_core,
                                                   mock.sentinel.role, target,
                                                   qualified, heartbeat)


def test_eks_qualification_rechecks_drain_after_node_response_parsing(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.target('aws-us-west-2')
    binding = profile.bindings['aws-eks-pullers']
    qualified = binding.qualified_clusters[0]
    drain_event = threading.Event()

    class DrainOnMetadata:

        def __init__(self) -> None:
            self.items = [_eks_node('node-a', 'i-a')]

        @property
        def metadata(self) -> object:
            drain_event.set()
            return SimpleNamespace(_continue=None)

    core = mock.Mock()
    core.list_node.return_value = DrainOnMetadata()
    assumed_client = mock.Mock()
    monkeypatch.setattr(canary_worker_service, '_assumed_client',
                        assumed_client)

    with pytest.raises(canary_worker_service._CanaryDrainRequested):
        canary_worker_service._qualified_eks_nodes(core,
                                                   mock.sentinel.role,
                                                   target,
                                                   qualified,
                                                   _OwnedHeartbeat(),
                                                   drain_event=drain_event)

    assumed_client.assert_not_called()


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


def _canary_object_graph_contains(root: object, marker: str) -> bool:
    seen: set[int] = set()
    pending = [root]
    while pending:
        if len(seen) >= 50000:
            raise AssertionError('Canary object-graph traversal exceeded.')
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, str):
            if marker in current:
                return True
            continue
        if isinstance(current, bytes):
            if marker.encode() in current:
                return True
            continue
        if current is None or isinstance(current, (bool, int, float, complex)):
            continue
        if isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
            continue
        if isinstance(current, (list, tuple, set, frozenset)):
            pending.extend(current)
            continue
        if isinstance(current, BaseException):
            pending.extend(current.args)
            pending.extend((current.__cause__, current.__context__))
            pending.extend(vars(current).values())
            continue
        if isinstance(current, types.MethodType):
            pending.append(current.__self__)
            continue
        if isinstance(current, types.FunctionType):
            pending.extend(current.__defaults__ or ())
            pending.extend((current.__kwdefaults__ or {}).values())
            for cell in current.__closure__ or ():
                try:
                    pending.append(cell.cell_contents)
                except ValueError:
                    pass
            continue
        if isinstance(current, (types.ModuleType, type)):
            continue
        try:
            pending.extend(vars(current).values())
        except TypeError:
            continue
    return False


def _assert_canary_traceback_value_free(error: BaseException,
                                        marker: str) -> None:
    traceback = error.__traceback__
    while traceback is not None:
        frame = traceback.tb_frame
        if frame.f_globals.get('__name__') == canary_worker_service.__name__:
            for value in frame.f_locals.values():
                assert not _canary_object_graph_contains(value, marker)
        traceback = traceback.tb_next


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


@pytest.mark.parametrize('provider_fails', (False, True),
                         ids=('response', 'failure'))
def test_canary_control_error_drops_losing_provider_state(
        provider_fails: bool) -> None:
    marker = 'LOSING_PROVIDER_SECRET'

    class _Provider:

        def describe_instances(self):
            if provider_fails:
                raise RuntimeError(marker)
            return {'secret': marker}

    provider = _Provider()
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = (
        None, worker_lease.LeaseLostError('canary lease lost'))
    client = canary_worker_service._FencedClient(provider, heartbeat)

    with pytest.raises(worker_lease.LeaseLostError) as exc_info:
        client.describe_instances()

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in str(error)
    _assert_canary_traceback_value_free(error, marker)


def test_canary_provider_call_rejects_stale_owner_before_request() -> None:
    provider = mock.Mock()
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = worker_lease.LeaseLostError(
        'canary lease lost')
    client = canary_worker_service._FencedClient(provider, heartbeat)

    with pytest.raises(worker_lease.LeaseLostError, match='canary lease lost'):
        client.describe_instances()

    provider.describe_instances.assert_not_called()


def test_canary_provider_call_rechecks_drain_after_ownership_renewal() -> None:
    drain_event = threading.Event()
    provider = mock.Mock()
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = drain_event.set
    client = canary_worker_service._FencedClient(provider, heartbeat)

    with pytest.raises(canary_worker_service._CanaryDrainRequested):
        canary_worker_service._ordinary_provider_call(client,
                                                      'describe_instances',
                                                      drain_event)

    provider.describe_instances.assert_not_called()


def test_canary_client_acquisition_rechecks_drain_inside_provider_fence(
        monkeypatch: pytest.MonkeyPatch) -> None:
    drain_event = threading.Event()
    raw_started = mock.Mock()

    def acquire(_role: object, _service: str, _region: str, *,
                provider_fence) -> object:
        drain_event.set()
        provider_fence()
        raw_started()
        return mock.sentinel.client

    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client', acquire)

    with pytest.raises(canary_worker_service._CanaryDrainRequested):
        canary_worker_service._assumed_client(mock.sentinel.role,
                                              'ec2',
                                              'us-west-2',
                                              _OwnedHeartbeat(),
                                              drain_event=drain_event)

    raw_started.assert_not_called()


def test_canary_client_acquisition_drain_drops_losing_credential_error(
        monkeypatch: pytest.MonkeyPatch) -> None:
    marker = 'LOSING_CREDENTIAL_SECRET'
    drain_event = threading.Event()

    def acquire(_role: object, _service: str, _region: str, *,
                provider_fence) -> object:
        del provider_fence
        drain_event.set()
        raise RuntimeError(marker)

    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client', acquire)

    with pytest.raises(canary_worker_service._CanaryDrainRequested) as exc_info:
        canary_worker_service._assumed_client(mock.sentinel.role,
                                              'ec2',
                                              'us-west-2',
                                              _OwnedHeartbeat(),
                                              drain_event=drain_event)

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_canary_traceback_value_free(error, marker)


def test_canary_cleanup_client_acquisition_rechecks_shared_deadline(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [99.0]
    raw_started = mock.Mock()
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])

    def acquire(_role: object, _service: str, _region: str, *,
                provider_fence) -> object:
        current[0] = 100.0
        provider_fence()
        raw_started()
        return mock.sentinel.client

    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client', acquire)

    with pytest.raises(canary_worker_service._CanaryCleanupDeadlineExceeded):
        canary_worker_service._assumed_client(mock.sentinel.role,
                                              'ec2',
                                              'us-west-2',
                                              _OwnedHeartbeat(),
                                              cleanup_deadline=100.0)

    raw_started.assert_not_called()


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
            'Arn': models.ec2_instance_profile_arn(binding),
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
    monkeypatch.setattr(canary_worker_service.kubernetes,
                        'provider_fenced_core_api',
                        lambda _context, **_kwargs: core)
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


@pytest.mark.parametrize(
    ('architecture', 'image_id_case', 'profile_case', 'expected_error'),
    (('x86_64', 'expected', 'launch_response', None),
     ('x86_64', 'expected', 'expected', None),
     ('x86_64', 'expected', 'delayed', None),
     ('x86_64', 'expected', 'terminal_missing_after_match', None),
     ('x86_64', 'expected', 'missing', None),
     ('x86_64', 'expected', 'missing_without_marker',
      'QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED'),
     ('x86_64', 'expected', 'iam_path_mismatch',
      'QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED'),
     ('x86_64', 'expected', 'conflicting',
      'QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED'),
     ('arm64', 'expected', 'expected', 'QUALIFICATION_FAILED'),
     (None, 'expected', 'expected', 'QUALIFICATION_FAILED'),
     ('x86_64', 'other', 'expected', 'QUALIFICATION_FAILED'),
     ('x86_64', None, 'expected', 'QUALIFICATION_FAILED')))
@pytest.mark.parametrize('use_spot', [True, False, None])
def test_ec2_canary_observes_exact_host_and_uses_fenced_clients(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        architecture: str | None, image_id_case: str | None, profile_case: str,
        expected_error: str | None, use_spot: bool | None) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=use_spot)
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
    if profile_case in ('missing', 'missing_without_marker',
                        'iam_path_mismatch'):
        instance.pop('IamInstanceProfile')
    elif profile_case == 'conflicting':
        instance['IamInstanceProfile'] = {
            'Arn': 'arn:aws:iam::210987654321:instance-profile/Other',
        }
    terminated_instance = {
        **instance,
        'State': {
            'Name': 'terminated'
        },
    }
    poll_observations = [instance]
    if profile_case == 'delayed':
        delayed_instance = {
            **instance,
            'State': {
                'Name': 'running'
            },
        }
        delayed_instance.pop('IamInstanceProfile')
        poll_observations.insert(0, delayed_instance)
    elif profile_case == 'terminal_missing_after_match':
        running_instance = {
            **instance,
            'State': {
                'Name': 'running'
            },
        }
        final_instance = {
            **instance,
            'State': {
                'Name': 'terminated'
            },
        }
        final_instance.pop('IamInstanceProfile')
        poll_observations = [running_instance, final_instance]

    def response(*instances: dict[str, object]) -> dict[str, object]:
        if not instances:
            return {'Reservations': []}
        return {'Reservations': [{'Instances': list(instances)}]}

    ec2 = mock.Mock()
    describe_responses = [response()]
    for observation in poll_observations:
        describe_responses.extend(
            (response(tagged_instance), response(observation)))
    describe_responses.extend(
        (response(tagged_instance), response(tagged_instance),
         response(terminated_instance)))
    ec2.describe_instances.side_effect = describe_responses
    launched_instance = {'InstanceId': 'i-canary'}
    if profile_case == 'launch_response':
        launched_instance['IamInstanceProfile'] = {
            'Arn': models.ec2_instance_profile_arn(binding),
        }
    ec2.run_instances.side_effect = (
        TimeoutError('ambiguous EC2 launch response'), {
            'Instances': [launched_instance],
        })
    spot_request = {
        'SpotInstanceRequestId': 'sir-canary',
        'InstanceId': 'i-canary',
        'State': 'active',
    }
    cancelled_spot_request = {
        **spot_request,
        'State': 'cancelled',
    }
    if use_spot:
        ec2.describe_spot_instance_requests.side_effect = ({
            'SpotInstanceRequests': [spot_request],
        }, {
            'SpotInstanceRequests': [spot_request],
        }, {
            'SpotInstanceRequests': [cancelled_spot_request],
        })
    marker = f'SKYPILOT_IMAGE_CANARY_SUCCESS:{payload["nonce"]}'
    ec2.get_console_output.return_value = {'Output': marker}
    if profile_case == 'missing_without_marker':
        ec2.get_console_output.return_value = {'Output': ''}
        monkeypatch.setattr(canary_worker_service,
                            '_EC2_CONSOLE_SETTLE_SECONDS', 0)
    iam = mock.Mock()
    iam_profile_arn = models.ec2_instance_profile_arn(binding)
    if profile_case == 'iam_path_mismatch':
        iam_profile_arn = iam_profile_arn.replace(
            'instance-profile/', 'instance-profile/qualified-path/')
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Arn': iam_profile_arn,
            'Roles': [{
                'Arn': binding.principals[0]
            }]
        }
    }
    provider_fences: list[object] = []
    acquired_services: list[str] = []

    def assumed_client(_role: object,
                       service: str,
                       _region: str,
                       *,
                       provider_fence: object = None) -> object:
        acquired_services.append(service)
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
    record_profile = mock.Mock(return_value=True)
    monkeypatch.setattr(
        canary_worker_service.qualification,
        'record_canary_ec2_instance_profile',
        record_profile,
    )
    monkeypatch.setattr(canary_worker_service, '_POLL_SECONDS', 0)
    heartbeat = _OwnedHeartbeat()

    if expected_error is None:
        evidence = canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}', heartbeat)
        assert evidence['child_instance_id'] == 'i-canary'
        assert evidence['host_image_id'] == dict(
            binding.qualified_node_images)[target.region]
        assert evidence['instance_architecture'] == 'x86_64'
        assert evidence['instance_profile_arn'] == (
            models.ec2_instance_profile_arn(binding))
    else:
        with pytest.raises(ValueError, match=expected_error):
            canary_worker_service._run_ec2_canary(
                operation, payload, _revision(profile), profile, target,
                binding, _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
                heartbeat)
    if profile_case == 'missing':
        record_profile.assert_called_once_with(
            operation.id,
            operation.lease_token,
            f'ec2:{target.region}:{operation.id}',
            models.ec2_instance_profile_arn(binding),
        )
    elif profile_case == 'missing_without_marker':
        record_profile.assert_not_called()
    elif profile_case == 'iam_path_mismatch':
        record_profile.assert_not_called()
    elif profile_case == 'launch_response':
        assert record_profile.call_args_list == [
            mock.call(
                operation.id,
                operation.lease_token,
                f'ec2:{target.region}:{operation.id}',
                models.ec2_instance_profile_arn(binding),
            )
        ]
    assert ec2.run_instances.call_args.kwargs['ClientToken'] == (
        canary_worker_service._ec2_client_token(operation.id))
    assert len(ec2.run_instances.call_args.kwargs['ClientToken']) == 64
    assert ec2.run_instances.call_count == 2
    assert ec2.run_instances.call_args_list[0].kwargs == (
        ec2.run_instances.call_args_list[1].kwargs)
    assert mock.call(
        InstanceIds=['i-canary']) in (ec2.describe_instances.call_args_list)
    launch = ec2.run_instances.call_args.kwargs
    assert docker_utils.credential_helper_config_cmd(
        target.registry, clear_cached_auth=True) in launch['UserData']
    assert 'AWS_SHARED_CREDENTIALS_FILE=/dev/null' in launch['UserData']
    assert 'AWS_CONFIG_FILE=/dev/null' in launch['UserData']
    assert '-u AWS_WEB_IDENTITY_TOKEN_FILE' in launch['UserData']
    assert '-u AWS_CONTAINER_CREDENTIALS_FULL_URI' in launch['UserData']
    assert 'AWS_EC2_METADATA_DISABLED=false' in launch['UserData']
    assert 'AWS_ECR_DISABLE_CACHE=true' in launch['UserData']
    assert 'AWS_SDK_LOAD_CONFIG=false' in launch['UserData']
    assert "trap 'shutdown -h now' EXIT" in launch['UserData']
    assert launch['InstanceInitiatedShutdownBehavior'] == 'terminate'
    if use_spot:
        assert launch['InstanceMarketOptions'] == {
            'MarketType': 'spot',
            'SpotOptions': {
                'SpotInstanceType': 'one-time',
                'InstanceInterruptionBehavior': 'terminate',
            },
        }
        assert ec2.cancel_spot_instance_requests.call_args_list == [
            mock.call(SpotInstanceRequestIds=['sir-canary'])
        ]
    else:
        assert 'InstanceMarketOptions' not in launch
        ec2.describe_spot_instance_requests.assert_not_called()
        ec2.cancel_spot_instance_requests.assert_not_called()
    assert launch['SecurityGroupIds'] == list(
        dict(binding.canary_security_groups)[target.region])
    expected_resource_types = {'instance', 'volume', 'network-interface'}
    if use_spot:
        expected_resource_types.add('spot-instances-request')
    assert {item['ResourceType'] for item in launch['TagSpecifications']
           } == expected_resource_types
    for specification in launch['TagSpecifications']:
        assert {
            tag['Key']: tag['Value'] for tag in specification['Tags']
        } == {
            'SkyPilotCanaryOperation': operation.id,
            'SkyPilotCatalog': 'catalog',
            'SkyPilotProfile': profile.name,
        }
    assert "trap 'shutdown -h now' EXIT" in launch['UserData']
    assert acquired_services == ['ec2', 'iam', 'ec2']
    assert len(provider_fences) == 3
    assert all(callable(fence) for fence in provider_fences)


def test_ec2_canary_replay_restores_durable_profile_latch(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
    expected_profile_arn = models.ec2_instance_profile_arn(binding)
    operation = _canary_operation()
    child_id = f'ec2:{target.region}:{operation.id}'
    operation = dataclasses.replace(
        operation,
        child_launch_id=child_id,
        canary_child_evidence={
            'backend': 'aws_vm',
            'instance_profile_arn': expected_profile_arn,
        },
    )
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
    tagged = {'InstanceId': 'i-canary'}
    terminal = {
        'InstanceId': 'i-canary',
        'Tags': expected_tags,
        'ImageId': dict(binding.qualified_node_images)[target.region],
        'Architecture': 'x86_64',
        'State': {
            'Name': 'terminated',
        },
    }

    def response(*instances: dict[str, object]) -> dict[str, object]:
        return {
            'Reservations': ([{
                'Instances': list(instances)
            }] if instances else [])
        }

    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        response(tagged),
        response(tagged),
        response(terminal),
        response(tagged),
        response(tagged),
        response(terminal),
    )
    ec2.get_console_output.return_value = {
        'Output': f'SKYPILOT_IMAGE_CANARY_SUCCESS:{payload["nonce"]}',
    }
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Arn': expected_profile_arn,
            'Roles': [{
                'Arn': binding.principals[0],
            }],
        },
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
    record_profile = mock.Mock(return_value=True)
    monkeypatch.setattr(
        canary_worker_service.qualification,
        'record_canary_ec2_instance_profile',
        record_profile,
    )
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 1)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    evidence = canary_worker_service._run_ec2_canary(
        operation, payload, _revision(profile), profile, target,
        binding, _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
        _OwnedHeartbeat())

    assert evidence['instance_profile_arn'] == expected_profile_arn
    assert evidence['teardown_verified'] is True
    record_profile.assert_not_called()
    ec2.run_instances.assert_not_called()
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-canary'])


def test_ec2_spot_ambiguous_launch_cancels_request_and_terminates_racing_child(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=True)
    assert binding.instance_profile is not None
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }
    empty_instances = {'Reservations': []}
    terminated_instance = {
        'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-racing',
                'State': {
                    'Name': 'terminated',
                },
            }]
        }]
    }
    ec2 = mock.Mock()
    ec2.run_instances.side_effect = (
        TimeoutError('first accepted response lost'),
        TimeoutError('second accepted response lost'),
    )
    ec2.describe_instances.side_effect = (
        empty_instances,  # Initial operation-tag discovery.
        empty_instances,  # Immediate cleanup discovery.
        empty_instances,  # First bounded cleanup observation.
        empty_instances,  # The request-to-instance edge is not tagged yet.
        terminated_instance,  # Exact racing-instance termination proof.
    )
    open_request = {
        'SpotInstanceRequestId': 'sir-racing',
        'State': 'open',
    }
    cancelled_request = {
        'SpotInstanceRequestId': 'sir-racing',
        'State': 'cancelled',
    }
    cancelled_with_instance = {
        'SpotInstanceRequestId': 'sir-racing',
        'InstanceId': 'i-racing',
        'State': 'cancelled',
    }
    ec2.describe_spot_instance_requests.side_effect = ({
        'SpotInstanceRequests': [open_request],
    }, {
        'SpotInstanceRequests': [open_request],
    }, {
        'SpotInstanceRequests': [cancelled_request],
    }, {
        'SpotInstanceRequests': [cancelled_with_instance],
    }, {
        'SpotInstanceRequests': [cancelled_with_instance],
    })
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Arn': models.ec2_instance_profile_arn(binding),
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

    with pytest.raises(TimeoutError, match='second accepted response lost'):
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    launch = ec2.run_instances.call_args.kwargs
    assert {
        specification['ResourceType']
        for specification in launch['TagSpecifications']
    } == {
        'instance',
        'network-interface',
        'spot-instances-request',
        'volume',
    }
    ec2.cancel_spot_instance_requests.assert_called_once_with(
        SpotInstanceRequestIds=['sir-racing'])
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-racing'])
    assert ec2.describe_spot_instance_requests.call_count == 5


def test_ec2_spot_terminal_request_without_child_consumes_full_settle_window(
        monkeypatch: pytest.MonkeyPatch) -> None:
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {'Reservations': []}
    open_request = {
        'SpotInstanceRequestId': 'sir-no-child',
        'State': 'open',
    }
    cancelled_request = {
        'SpotInstanceRequestId': 'sir-no-child',
        'State': 'cancelled',
    }
    ec2.describe_spot_instance_requests.side_effect = (
        {
            'SpotInstanceRequests': [open_request],
        },
        {
            'SpotInstanceRequests': [open_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
    )
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 3)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    assert canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', [], settle_absence=True, spot_request_expected=True)

    assert ec2.describe_instances.call_count == 3
    assert ec2.describe_spot_instance_requests.call_count == 7
    ec2.cancel_spot_instance_requests.assert_called_once_with(
        SpotInstanceRequestIds=['sir-no-child'])
    ec2.terminate_instances.assert_not_called()


def test_ec2_spot_late_terminal_transition_preserves_custody(
        monkeypatch: pytest.MonkeyPatch) -> None:
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {'Reservations': []}
    active_request = {
        'SpotInstanceRequestId': 'sir-late-terminal',
        'State': 'active',
    }
    cancelled_request = {
        'SpotInstanceRequestId': 'sir-late-terminal',
        'State': 'cancelled',
    }
    ec2.describe_spot_instance_requests.side_effect = (
        {
            'SpotInstanceRequests': [active_request],
        },
        {
            'SpotInstanceRequests': [active_request],
        },
        {
            'SpotInstanceRequests': [active_request],
        },
        {
            'SpotInstanceRequests': [active_request],
        },
        {
            'SpotInstanceRequests': [active_request],
        },
        {
            'SpotInstanceRequests': [active_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
    )
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 3)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    assert not canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', [], settle_absence=True, spot_request_expected=True)

    assert ec2.describe_instances.call_count == 3
    assert ec2.describe_spot_instance_requests.call_count == 8
    assert ec2.cancel_spot_instance_requests.call_count == 2
    ec2.terminate_instances.assert_not_called()


def test_ec2_spot_terminal_transition_restarts_settle_window(
        monkeypatch: pytest.MonkeyPatch) -> None:
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {'Reservations': []}
    open_request = {
        'SpotInstanceRequestId': 'sir-terminal-transition',
        'State': 'open',
    }
    cancelled_request = {
        'SpotInstanceRequestId': 'sir-terminal-transition',
        'State': 'cancelled',
    }
    closed_request = {
        'SpotInstanceRequestId': 'sir-terminal-transition',
        'State': 'closed',
    }
    ec2.describe_spot_instance_requests.side_effect = (
        {
            'SpotInstanceRequests': [open_request],
        },
        {
            'SpotInstanceRequests': [open_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [cancelled_request],
        },
        {
            'SpotInstanceRequests': [closed_request],
        },
        {
            'SpotInstanceRequests': [closed_request],
        },
        {
            'SpotInstanceRequests': [closed_request],
        },
    )
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 3)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    assert not canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', [], settle_absence=True, spot_request_expected=True)

    assert ec2.describe_instances.call_count == 3
    assert ec2.describe_spot_instance_requests.call_count == 7
    ec2.cancel_spot_instance_requests.assert_called_once_with(
        SpotInstanceRequestIds=['sir-terminal-transition'])
    ec2.terminate_instances.assert_not_called()


def test_ec2_spot_cleanup_preserves_custody_until_request_is_terminal(
        monkeypatch: pytest.MonkeyPatch) -> None:
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {'Reservations': []}
    active_request = {
        'SpotInstanceRequestId': 'sir-active',
        'State': 'active',
    }
    ec2.describe_spot_instance_requests.return_value = {
        'SpotInstanceRequests': [active_request],
    }
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 1)

    assert not canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', [], settle_absence=True, spot_request_expected=True)

    ec2.cancel_spot_instance_requests.assert_called_once_with(
        SpotInstanceRequestIds=['sir-active'])
    ec2.terminate_instances.assert_not_called()


def test_tagged_instance_discovery_reads_every_page(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = ({
        'Reservations': [],
        'NextToken': 'next-page',
    }, {
        'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-second-page',
            }]
        }],
    })

    instances = canary_worker_service._tagged_instances(ec2,
                                                        'operation',
                                                        cleanup_deadline=300.0)

    assert [instance['InstanceId'] for instance in instances
           ] == ['i-second-page']
    filters = [{
        'Name': 'tag:SkyPilotCanaryOperation',
        'Values': ['operation'],
    }, {
        'Name': 'instance-state-name',
        'Values': [
            'pending', 'running', 'stopping', 'stopped', 'shutting-down',
            'terminated'
        ],
    }]
    assert ec2.describe_instances.call_args_list == [
        mock.call(Filters=filters, MaxResults=1000),
        mock.call(Filters=filters, MaxResults=1000, NextToken='next-page'),
    ]


def test_ec2_spot_cleanup_terminates_second_page_instance(
        monkeypatch: pytest.MonkeyPatch) -> None:
    ec2 = mock.Mock()
    running_instance = {
        'InstanceId': 'i-second-page',
        'SpotInstanceRequestId': 'sir-second-page',
        'State': {
            'Name': 'running',
        },
    }
    terminated_instance = {
        **running_instance,
        'State': {
            'Name': 'terminated',
        },
    }

    def describe_instances(**kwargs: object) -> dict[str, object]:
        if 'InstanceIds' in kwargs:
            return {
                'Reservations': [{
                    'Instances': [terminated_instance],
                }]
            }
        if kwargs.get('NextToken') == 'next-page':
            return {
                'Reservations': [{
                    'Instances': [running_instance],
                }]
            }
        return {
            'Reservations': [],
            'NextToken': 'next-page',
        }

    ec2.describe_instances.side_effect = describe_instances
    closed_request = {
        'SpotInstanceRequestId': 'sir-second-page',
        'State': 'closed',
    }
    ec2.describe_spot_instance_requests.return_value = {
        'SpotInstanceRequests': [closed_request],
    }
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 1)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    assert canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', [], settle_absence=True, spot_request_expected=True)

    assert any(
        call.kwargs.get('NextToken') == 'next-page'
        for call in ec2.describe_instances.call_args_list)
    ec2.terminate_instances.assert_called_once_with(
        InstanceIds=['i-second-page'])


@pytest.mark.parametrize(
    ('next_token', 'expected_calls'),
    [
        ('', 1),
        (7, 1),
        ('same-page', 2),
    ],
    ids=['empty-token', 'non-string-token', 'cyclic-token'],
)
def test_tagged_instance_discovery_rejects_invalid_pagination(
        monkeypatch: pytest.MonkeyPatch, next_token: object,
        expected_calls: int) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {
        'Reservations': [],
        'NextToken': next_token,
    }

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        canary_worker_service._tagged_instances(ec2,
                                                'operation',
                                                cleanup_deadline=300.0)

    assert ec2.describe_instances.call_count == expected_calls


def test_tagged_spot_request_discovery_reads_every_page(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    ec2 = mock.Mock()
    ec2.describe_spot_instance_requests.side_effect = ({
        'SpotInstanceRequests': [{
            'SpotInstanceRequestId': 'sir-one',
        }],
        'NextToken': 'next-page',
    }, {
        'SpotInstanceRequests': [{
            'SpotInstanceRequestId': 'sir-two',
        }],
    })

    requests = canary_worker_service._tagged_spot_requests(
        ec2, 'operation', cleanup_deadline=300.0)

    assert [request['SpotInstanceRequestId'] for request in requests
           ] == ['sir-one', 'sir-two']
    assert ec2.describe_spot_instance_requests.call_args_list == [
        mock.call(Filters=[{
            'Name': 'tag:SkyPilotCanaryOperation',
            'Values': ['operation'],
        }],
                  MaxResults=1000),
        mock.call(Filters=[{
            'Name': 'tag:SkyPilotCanaryOperation',
            'Values': ['operation'],
        }],
                  MaxResults=1000,
                  NextToken='next-page'),
    ]


@pytest.mark.parametrize('invalid_instance_id', [None, 7],
                         ids=['missing-id', 'non-string-id'])
def test_ec2_canary_malformed_launch_identity_settles_and_terminates_late_child(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        invalid_instance_id: object) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
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
            'Arn': models.ec2_instance_profile_arn(binding),
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
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 2)
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
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
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
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 2)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        canary_worker_service._run_ec2_canary(
            operation, payload, _revision(profile), profile, target, binding,
            _DIGEST, f'{target.registry}/qualification@{_DIGEST}',
            _OwnedHeartbeat())

    ec2.run_instances.assert_not_called()
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-late'])


def test_ec2_canary_persistent_malformed_discovery_preserves_custody(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    operation = _canary_operation()
    operation = dataclasses.replace(
        operation,
        child_launch_id=f'ec2:us-west-2:{operation.id}',
    )
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }

    malformed = {'State': {'Name': 'running'}}
    response = {'Reservations': [{'Instances': [malformed]}]}
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = [response] * 4
    monkeypatch.setattr(canary_worker_service.aws, 'assumed_client',
                        lambda *_args, **_kwargs: ec2)
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        (payload, _revision(profile), profile, target, binding, _DIGEST,
         f'{target.registry}/qualification@{_DIGEST}'))
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 2)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)
    failed = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', failed)

    assert not canary_worker_service.run_canary(operation)

    assert ec2.describe_instances.call_count == 4
    ec2.run_instances.assert_not_called()
    ec2.terminate_instances.assert_not_called()
    failed.assert_not_called()
    assert operation.child_launch_id is not None


@pytest.mark.parametrize('malformed', [
    {
        'State': {
            'Name': 'running'
        }
    },
    {
        'InstanceId': 7,
        'State': {
            'Name': 'running'
        }
    },
    {
        'InstanceId': '',
        'State': {
            'Name': 'running'
        }
    },
],
                         ids=['missing-id', 'non-string-id', 'empty-id'])
def test_ec2_canary_terminal_child_plus_malformed_discovery_preserves_custody(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        malformed: dict[str, object]) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
    payload = {
        'backend': 'aws_vm',
        'nonce': '1' * 32,
        'runtime_id': target.region,
        'timeout_seconds': 900,
    }

    def response(*instances: dict[str, object]) -> dict[str, object]:
        return {'Reservations': [{'Instances': list(instances)}]}

    known_running = {
        'InstanceId': 'i-known',
        'State': {
            'Name': 'running',
        },
    }
    known_terminated = {
        'InstanceId': 'i-known',
        'State': {
            'Name': 'terminated',
        },
    }
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        response(known_running),
        response(known_terminated, malformed),
        response(known_terminated, malformed),
        response(known_terminated),
    )
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Arn': models.ec2_instance_profile_arn(binding),
            'Roles': [{
                'Arn': 'arn:aws:iam::123456789012:role/wrong-role',
            }]
        }
    }
    clients = {'ec2': ec2, 'iam': iam}
    monkeypatch.setattr(
        canary_worker_service.aws, 'assumed_client',
        lambda _role, service, _region, **_kwargs: clients[service])
    monkeypatch.setattr(canary_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    attached = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'attach_canary_child', attached)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'authorize_canary_launch',
                        lambda *_args, **_kwargs: 1000)
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        (payload, _revision(profile), profile, target, binding, _DIGEST,
         f'{target.registry}/qualification@{_DIGEST}'))
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 1)
    failed = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', failed)

    assert not canary_worker_service.run_canary(operation)

    assert ec2.describe_instances.call_count == 4
    ec2.run_instances.assert_not_called()
    ec2.terminate_instances.assert_not_called()
    failed.assert_not_called()
    attached.assert_called_once_with(
        operation.id,
        operation.lease_token,
        f'ec2:us-west-2:{operation.id}',
    )


def test_ec2_teardown_resolves_prior_malformed_inventory_after_clean_window(
        monkeypatch: pytest.MonkeyPatch) -> None:
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = [
        {
            'Reservations': []
        },
        {
            'Reservations': []
        },
    ]
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 2)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)

    assert canary_worker_service._terminate_ec2_instances(
        ec2,
        'operation', [],
        settle_absence=False,
        initial_unidentified_child_observed=True)

    assert ec2.describe_instances.call_count == 2
    ec2.terminate_instances.assert_not_called()


def test_ec2_teardown_stops_at_wall_clock_bound_after_slow_provider_call(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_SECONDS', 300)
    ec2 = mock.Mock()

    def slow_running_inventory(**_kwargs: object) -> dict[str, object]:
        current[0] = 301.0
        return {
            'Reservations': [{
                'Instances': [{
                    'InstanceId': 'i-known',
                    'State': {
                        'Name': 'running'
                    },
                }]
            }]
        }

    ec2.describe_instances.side_effect = slow_running_inventory

    assert not canary_worker_service._terminate_ec2_instances(
        ec2,
        'operation', [],
        settle_absence=True,
        initial_unidentified_child_observed=True)

    ec2.describe_instances.assert_called_once()
    ec2.terminate_instances.assert_not_called()


def test_ec2_spot_production_settle_budget_includes_provider_latency(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Model cleanup-client acquisition and preliminary discovery having already
    # consumed 90 seconds of the absolute pod-shutdown budget.
    current = [90.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    monkeypatch.setattr(
        canary_worker_service.time, 'sleep',
        lambda seconds: current.__setitem__(0, current[0] + seconds))
    ec2 = mock.Mock()
    closed_request = {
        'SpotInstanceRequestId': 'sir-no-child',
        'State': 'closed',
    }

    def describe_instances(**_kwargs: object) -> dict[str, object]:
        current[0] += 0.05
        return {'Reservations': []}

    def describe_spot_requests(**_kwargs: object) -> dict[str, object]:
        current[0] += 0.05
        return {'SpotInstanceRequests': [closed_request]}

    ec2.describe_instances.side_effect = describe_instances
    ec2.describe_spot_instance_requests.side_effect = describe_spot_requests

    assert canary_worker_service._terminate_ec2_instances(
        ec2,
        'operation', [],
        settle_absence=True,
        spot_request_expected=True,
        cleanup_deadline=float(canary_worker_service._EC2_TEARDOWN_SECONDS))

    assert ec2.describe_instances.call_count == (
        canary_worker_service._EC2_TEARDOWN_ATTEMPTS)
    assert ec2.describe_spot_instance_requests.call_count == (
        2 * canary_worker_service._EC2_TEARDOWN_ATTEMPTS)
    assert current[0] < canary_worker_service._EC2_TEARDOWN_SECONDS
    ec2.terminate_instances.assert_not_called()


def test_ec2_cleanup_rechecks_deadline_after_lease_heartbeat(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [299.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    provider = mock.Mock()
    heartbeat = mock.Mock(spec=['assert_owned'])
    heartbeat.assert_owned.side_effect = lambda: current.__setitem__(0, 301.0)
    ec2 = canary_worker_service._FencedClient(provider, heartbeat)

    assert not canary_worker_service._terminate_ec2_instances(
        ec2,
        'operation', [],
        settle_absence=True,
        initial_unidentified_child_observed=True,
        cleanup_deadline=300.0)

    provider.describe_instances.assert_not_called()
    provider.terminate_instances.assert_not_called()


def test_ec2_teardown_starts_no_state_read_after_slow_termination(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_SECONDS', 300)
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-known',
                'State': {
                    'Name': 'running'
                },
            }]
        }]
    }
    ec2.terminate_instances.side_effect = lambda **_kwargs: current.__setitem__(
        0, 301.0)

    assert not canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', ['i-known'], settle_absence=False)

    ec2.describe_instances.assert_called_once()
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-known'])


def test_ec2_teardown_rejects_state_read_that_crosses_deadline(
        monkeypatch: pytest.MonkeyPatch) -> None:
    current = [0.0]
    monkeypatch.setattr(canary_worker_service.time, 'monotonic',
                        lambda: current[0])
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_SECONDS', 300)
    ec2 = mock.Mock()
    terminated = {
        'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-known',
                'State': {
                    'Name': 'terminated'
                },
            }]
        }]
    }

    def describe_instances(**kwargs: object) -> dict[str, object]:
        if 'InstanceIds' in kwargs:
            current[0] = 301.0
        return terminated

    ec2.describe_instances.side_effect = describe_instances

    assert not canary_worker_service._terminate_ec2_instances(
        ec2, 'operation', ['i-known'], settle_absence=False)

    assert ec2.describe_instances.call_count == 2
    ec2.terminate_instances.assert_not_called()


@pytest.mark.parametrize(
    ('late_state', 'terminalizes'), [
        ('terminated', True),
        ('shutting-down', False),
    ],
    ids=['late-child-proved-terminal', 'late-child-remains-reclaimable'])
def test_ec2_ambiguous_settling_consumes_full_window_before_terminalizing(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        late_state: str, terminalizes: bool) -> None:
    operation = _canary_operation()
    target = profile.target('aws-us-west-2')
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
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

    first_running = {
        'InstanceId': 'i-first',
        'State': {
            'Name': 'running',
        },
    }
    first_terminated = {
        'InstanceId': 'i-first',
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
    late_final = {
        'InstanceId': 'i-late',
        'State': {
            'Name': late_state,
        },
    }
    ec2 = mock.Mock()
    ec2.describe_instances.side_effect = (
        response(),  # Initial operation-tag discovery.
        response(),  # Immediate discovery in finally.
        response(),  # First bounded settling attempt.
        response(first_running),  # First child becomes visible.
        response(first_terminated),  # First child termination proof.
        response(first_terminated, late_running),  # Later child next poll.
        response(first_terminated, late_final),  # Complete final-state proof.
    )
    ec2.run_instances.return_value = {
        'Instances': [{
            'InstanceId': None,
        }]
    }
    iam = mock.Mock()
    iam.get_instance_profile.return_value = {
        'InstanceProfile': {
            'Arn': models.ec2_instance_profile_arn(binding),
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
    monkeypatch.setattr(canary_worker_service, '_LeaseHeartbeat',
                        _OwnedHeartbeat)
    monkeypatch.setattr(
        canary_worker_service, '_load_contract', lambda _:
        (payload, _revision(profile), profile, target, binding, _DIGEST,
         f'{target.registry}/qualification@{_DIGEST}'))
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 3)
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_POLL_SECONDS', 0)
    failed = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification,
                        'fail_owned_canary', failed)

    assert not canary_worker_service.run_canary(operation)

    assert ec2.terminate_instances.call_args_list == [
        mock.call(InstanceIds=['i-first']),
        mock.call(InstanceIds=['i-late']),
    ]
    assert ec2.describe_instances.call_count == 7
    if terminalizes:
        failed.assert_called_once_with(operation,
                                       'QUALIFICATION_FAILED',
                                       teardown_verified=True)
    else:
        failed.assert_not_called()


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
    binding = dataclasses.replace(profile.bindings[binding_id],
                                  canary_use_spot=False)
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
            'Arn': models.ec2_instance_profile_arn(binding),
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
    monkeypatch.setattr(canary_worker_service, '_EC2_TEARDOWN_ATTEMPTS', 1)

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
            'Arn': ('arn:aws:iam::210987654321:'
                    'instance-profile/EksNodeProfile'),
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
    monkeypatch.setattr(canary_worker_service.kubernetes,
                        'provider_fenced_core_api',
                        lambda _context, **_kwargs: core)
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
    assert len(provider_fences) == 3
    assert all(callable(fence) for fence in provider_fences)
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

    reconcile_fanout.assert_called_once_with(should_stop=None)
    reconcile_profiles.assert_called_once_with(limiter, should_stop=None)
    schedule_canaries.assert_called_once_with(should_stop=None)


def test_copy_qualification_page_stops_between_targets(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    stop = threading.Event()
    revision = _revision(profile)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    reconcile = mock.Mock(
        side_effect=lambda *_args, **_kwargs: (stop.set() or True))
    monkeypatch.setattr(copy_worker_service, 'reconcile_qualification_copy',
                        reconcile)

    assert copy_worker_service.reconcile_qualification_profiles(
        mock.sentinel.limiter, should_stop=stop.is_set) == 1

    reconcile.assert_called_once()
    assert reconcile.call_args.args == (revision, profile.canonical)


def test_copy_qualification_stops_after_metadata_before_database_write(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    stop = threading.Event()
    revision = _revision(profile)
    target = profile.canonical
    destination = mock.Mock()
    destination.repository_metadata.side_effect = (lambda: stop.set() or {
        'repository_arn': 'unused'
    })
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_target_shard',
                        lambda *_args: _shard(profile, target.name))
    monkeypatch.setattr(copy_worker_service.qualification,
                        'qualification_repository', lambda *_args:
                        ('qualification', 'qualification-arn'))
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: destination)
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    record = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_profile_attestation', record)

    with pytest.raises(copy_worker_service._QualificationDrainRequested):
        copy_worker_service.reconcile_qualification_copy(
            revision, target, limiter=mock.Mock(), should_stop=stop.is_set)

    record.assert_not_called()


def test_copy_qualification_transfer_receives_live_stop_event(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    stop = threading.Event()
    revision = _revision(profile)
    target = profile.canonical
    repository_name = 'qualification'
    repository_arn = 'qualification-arn'
    destination = mock.Mock()
    destination.repository_metadata.return_value = {
        'repository_arn': repository_arn,
        'repository_uri': f'{target.registry}/{repository_name}',
        'tag_mutability': 'IMMUTABLE',
        'encryption_type': 'AES256',
        'kms_key': None,
    }
    graph = SimpleNamespace(runtime_digest=_DIGEST, platform='linux/amd64')

    def copy_graph(_graph, _read_blob, cancel_event):
        assert not cancel_event.is_set()
        stop.set()
        assert cancel_event.is_set()
        return aws.CopyOutcome.WRITTEN

    destination.copy_graph.side_effect = copy_graph
    monkeypatch.setattr(copy_worker_service.topology_state, 'get_target_shard',
                        lambda *_args: _shard(profile, target.name))
    monkeypatch.setattr(copy_worker_service.qualification,
                        'qualification_repository', lambda *_args:
                        (repository_name, repository_arn))
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        lambda *_args, **_kwargs: destination)
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    record = mock.Mock(return_value=revision)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_profile_attestation', record)
    monkeypatch.setattr(copy_worker_service, '_qualification_database_epoch',
                        lambda **_kwargs: 100)
    monkeypatch.setattr(copy_worker_service, '_qualification_copy_needed',
                        lambda *_args: True)
    reader = SimpleNamespace(read_blob=mock.sentinel.read_blob)
    monkeypatch.setattr(copy_worker_service.providers, 'RegistryV2Source',
                        lambda *_args, **_kwargs: reader)
    monkeypatch.setattr(copy_worker_service, '_inspection_graph',
                        lambda *_args, **_kwargs: graph)

    with pytest.raises(copy_worker_service._QualificationDrainRequested):
        copy_worker_service.reconcile_qualification_copy(
            revision, target, limiter=mock.Mock(), should_stop=stop.is_set)

    destination.verify_graph.assert_not_called()
    assert record.call_count == 1


def test_candidate_shard_probe_stops_after_first_provider_proof(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
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
    revision = dataclasses.replace(_revision(candidate_profile),
                                   state=models.ImageProfileState.QUALIFYING)
    stop = threading.Event()
    from_role = mock.Mock(return_value=mock.sentinel.repository)
    matching = mock.Mock(
        side_effect=lambda *_args, **_kwargs: stop.set() or ({}, 100, 10))
    record = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_target_shards', lambda *_args: shards)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'get_profile_revision',
                        lambda *_args: _revision(profile))
    monkeypatch.setattr(copy_worker_service, '_expected_shard_attestation',
                        lambda *_args: ('live-key', {}))
    monkeypatch.setattr(copy_worker_service.aws.EcrRepository, 'from_role',
                        from_role)
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    monkeypatch.setattr(copy_worker_service, '_matching_shard_metadata',
                        matching)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'record_candidate_shard_attestation', record)

    with pytest.raises(copy_worker_service._QualificationDrainRequested):
        copy_worker_service._reconcile_candidate_shard_attestation(
            revision,
            candidate_profile,
            target,
            limiter=mock.Mock(),
            now=100,
            should_stop=stop.is_set)

    from_role.assert_called_once()
    matching.assert_called_once()
    record.assert_not_called()


def test_automatic_canary_scheduler_stops_between_runtime_transactions(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.canonical
    copy_key = models.profile_attestation_key('copy', target.name)
    revision = dataclasses.replace(
        _revision(profile),
        attestations={
            copy_key: {
                'status': 'READY',
                'observed_at': 100,
                'target_fingerprint': target.target_fingerprint,
                'runtime_digest': _DIGEST,
                'platform': profile.qualification.canary_platform,
            }
        })
    stop = threading.Event()
    monkeypatch.setattr(copy_worker_service.qualification, '_database_epoch',
                        lambda **_kwargs: 100)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    request = mock.Mock(
        side_effect=lambda **_kwargs: (stop.set() or mock.sentinel.operation))
    monkeypatch.setattr(copy_worker_service.qualification, 'request_canary',
                        request)

    assert copy_worker_service.qualification.schedule_automatic_canaries(
        should_stop=stop.is_set) == 1

    request.assert_called_once()
    assert request.call_args.kwargs['target_id'] == target.name


def test_qualification_lifecycle_stops_after_provider_acquisition(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.canonical
    backend, binding_id = target.runtime_pull[0]
    binding = profile.bindings[binding_id]
    runtime_id = lifecycle_worker_service.qualification.runtime_ids(
        target, backend, binding)[0]
    copy_key = models.profile_attestation_key('copy', target.name)
    runtime_key = models.profile_attestation_key('runtime', target.name,
                                                 backend, binding.fingerprint,
                                                 runtime_id)
    revision = dataclasses.replace(_revision(profile),
                                   attestations={
                                       copy_key: {
                                           'status': 'READY',
                                           'observed_at': 100,
                                           'runtime_digest': _DIGEST,
                                       },
                                       runtime_key: {
                                           'status': 'READY',
                                           'observed_at': 100,
                                           'runtime_digest': _DIGEST,
                                       },
                                   })
    stop = threading.Event()
    repository = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'get_target_shard', lambda *_args: mock.sentinel.shard)
    monkeypatch.setattr(lifecycle_worker_service.qualification,
                        'qualification_repository', lambda *_args:
                        ('qualification', 'repository-arn'))
    monkeypatch.setattr(lifecycle_worker_service, '_lifecycle_role',
                        lambda *_args: mock.sentinel.role)
    monkeypatch.setattr(
        lifecycle_worker_service.aws.EcrRepository, 'from_role',
        mock.Mock(
            side_effect=lambda *_args, **_kwargs: (stop.set() or repository)))

    assert not lifecycle_worker_service.reconcile_qualification_lifecycle(
        mock.sentinel.limiter, should_stop=stop.is_set)

    repository.exact_delete.assert_not_called()


@pytest.mark.parametrize('stop_after_delete', [True, False],
                         ids=['stop-before-readback', 'concluded-absence'])
def test_qualification_lifecycle_fences_delete_readback_boundary(
        monkeypatch: pytest.MonkeyPatch, profile: models.ManagedRegistryProfile,
        stop_after_delete: bool) -> None:
    target = profile.canonical
    copy_key = models.profile_attestation_key('copy', target.name)
    attestations = {
        copy_key: {
            'status': 'READY',
            'observed_at': 100,
            'runtime_digest': _DIGEST,
        }
    }
    for backend, binding_id in target.runtime_pull:
        binding = profile.bindings[binding_id]
        for runtime_id in lifecycle_worker_service.qualification.runtime_ids(
                target, backend, binding):
            attestations[models.profile_attestation_key(
                'runtime', target.name, backend, binding.fingerprint,
                runtime_id)] = {
                    'status': 'READY',
                    'observed_at': 100,
                    'runtime_digest': _DIGEST,
                }
    revision = dataclasses.replace(_revision(profile),
                                   attestations=attestations)
    stop = threading.Event()
    raw_client = mock.Mock()

    def delete_image(**_kwargs):
        if stop_after_delete:
            stop.set()
        return {}

    raw_client.batch_delete_image.side_effect = delete_image
    raw_client.batch_get_image.return_value = {
        'images': [],
        'failures': [{
            'failureCode': 'ImageNotFound',
        }],
    }

    def repository_from_role(*_args, **kwargs):
        client = aws._ProviderFencedEcrClient(  # pylint: disable=protected-access
            raw_client, kwargs['provider_fence'])
        client = aws._HookedEcrClient(  # pylint: disable=protected-access
            client, kwargs['hooks'])
        return aws.EcrRepository(client,
                                 'qualification',
                                 provider_fence=kwargs['provider_fence'])

    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'get_target_shard',
                        lambda *_args: _shard(profile, target.name))
    monkeypatch.setattr(lifecycle_worker_service.qualification,
                        'qualification_repository', lambda *_args:
                        ('qualification', 'qualification-arn'))
    monkeypatch.setattr(lifecycle_worker_service, '_lifecycle_role',
                        lambda *_args: mock.sentinel.role)
    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)
    record = mock.Mock(return_value=revision)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'record_profile_attestation', record)
    activate = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.qualification,
                        'maybe_activate_profile', activate)

    reconciled = lifecycle_worker_service.reconcile_qualification_lifecycle(
        mock.Mock(), should_stop=stop.is_set)

    raw_client.batch_delete_image.assert_called_once()
    if stop_after_delete:
        assert not reconciled
        raw_client.batch_get_image.assert_not_called()
        record.assert_not_called()
        activate.assert_not_called()
    else:
        assert reconciled
        raw_client.batch_get_image.assert_called_once()
        record.assert_called_once()
        activate.assert_called_once_with(revision.id)


def test_qualification_lifecycle_does_not_redelete_restored_copy(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.canonical
    lifecycle_proof_id = '00000000-0000-4000-8000-000000000099'
    copy_key = models.profile_attestation_key('copy', target.name)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    revision = dataclasses.replace(
        _revision(profile),
        attestations={
            copy_key: {
                'status': 'READY',
                'observed_at': 101,
                'target_fingerprint': target.target_fingerprint,
                'runtime_digest': _DIGEST,
                'platform': profile.qualification.canary_platform,
                'restores_lifecycle_proof_id': lifecycle_proof_id,
            },
            lifecycle_key: {
                'status': 'READY',
                'observed_at': 100,
                'target_fingerprint': target.target_fingerprint,
                'repository_arn': 'qualification-repository-arn',
                'runtime_digest': _DIGEST,
                'exact_absence': True,
                'lifecycle_proof_id': lifecycle_proof_id,
            },
        })
    from_role = mock.Mock()
    activate = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        from_role)
    monkeypatch.setattr(lifecycle_worker_service.qualification,
                        'maybe_activate_profile', activate)

    assert lifecycle_worker_service.reconcile_qualification_lifecycle(
        mock.sentinel.limiter)

    from_role.assert_not_called()
    activate.assert_called_once_with(revision.id)


def test_qualification_lifecycle_upgrades_legacy_proof_without_provider_io(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    target = profile.canonical
    copy_key = models.profile_attestation_key('copy', target.name)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    revision = dataclasses.replace(
        _revision(profile),
        attestations={
            copy_key: {
                'status': 'READY',
                'observed_at': 100,
                'target_fingerprint': target.target_fingerprint,
                'runtime_digest': _DIGEST,
                'platform': profile.qualification.canary_platform,
            },
            lifecycle_key: {
                'status': 'READY',
                'observed_at': 100,
                'target_fingerprint': target.target_fingerprint,
                'repository_arn': 'qualification-repository-arn',
                'runtime_digest': _DIGEST,
                'exact_absence': True,
            },
        })
    from_role = mock.Mock()
    record = mock.Mock(return_value=revision)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'record_profile_attestation', record)
    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        from_role)
    monkeypatch.setattr(lifecycle_worker_service.qualification,
                        'maybe_activate_profile', mock.Mock())

    assert lifecycle_worker_service.reconcile_qualification_lifecycle(
        mock.sentinel.limiter)

    from_role.assert_not_called()
    record.assert_called_once()
    evidence = record.call_args.kwargs['evidence']
    assert evidence['exact_absence'] is True
    assert (lifecycle_worker_service.qualification.
            qualification_lifecycle_proof_id(evidence) is not None)


def test_failed_reservation_reaper_stops_after_provider_acquisition(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    stop = threading.Event()
    location = SimpleNamespace(shard_id='shard-id',
                               target_fingerprint='fingerprint',
                               runtime_digest=_DIGEST,
                               id='location-id',
                               updated_at=100)
    shard = SimpleNamespace(target_fingerprint='fingerprint',
                            region=profile.canonical.region,
                            repository_name='canonical')
    repository = mock.Mock()
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'list_failed_canonical_reap_candidates',
                        lambda **_kwargs: [location])
    monkeypatch.setattr(lifecycle_worker_service.topology_state, 'get_shard',
                        lambda _shard_id: shard)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_profile_target_for_location', lambda *_args:
                        (profile, profile.canonical))
    monkeypatch.setattr(lifecycle_worker_service, '_lifecycle_role',
                        lambda *_args: mock.sentinel.role)
    monkeypatch.setattr(
        lifecycle_worker_service.aws.EcrRepository, 'from_role',
        mock.Mock(
            side_effect=lambda *_args, **_kwargs: (stop.set() or repository)))

    assert not lifecycle_worker_service.reconcile_failed_canonical_reservations(
        mock.sentinel.limiter, should_stop=stop.is_set)

    repository.exact_manifest_exists.assert_not_called()


def test_manifest_ingestion_stops_between_independent_files(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    for name in ('a.json', 'b.json'):
        (tmp_path / name).write_text(json.dumps({'profile': name}),
                                     encoding='utf-8')
    stop = threading.Event()
    ingest = mock.Mock(side_effect=lambda **_kwargs: stop.set())
    monkeypatch.setattr(copy_worker_service.qualification, 'ingest_manifest',
                        ingest)

    assert copy_worker_service._ingest_qualification_manifests(
        str(tmp_path), should_stop=stop.is_set) == 1

    ingest.assert_called_once()
    assert ingest.call_args.kwargs['profile_name'] == 'a.json'


def test_manifest_ingestion_continues_after_sanitized_failure(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    for name in ('a.json', 'b.json'):
        (tmp_path / name).write_text(json.dumps({'profile': name}),
                                     encoding='utf-8')
    ingest = mock.Mock(side_effect=[ValueError('QUALIFICATION_FAILED'), None])
    monkeypatch.setattr(copy_worker_service.qualification, 'ingest_manifest',
                        ingest)

    assert copy_worker_service._ingest_qualification_manifests(
        str(tmp_path)) == 1

    assert [call.kwargs['profile_name'] for call in ingest.call_args_list
           ] == ['a.json', 'b.json']


def test_publication_fanout_stops_between_location_transactions(
        monkeypatch: pytest.MonkeyPatch) -> None:
    stop = threading.Event()
    session = mock.MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = [
        'location-one', 'location-two'
    ]
    session_context = mock.MagicMock()
    session_context.__enter__.return_value = session
    monkeypatch.setattr(transactions.catalog_state, 'engine',
                        lambda: mock.sentinel.engine)
    monkeypatch.setattr(transactions.orm, 'Session',
                        mock.Mock(return_value=session_context))
    reconcile = mock.Mock(side_effect=lambda _location_id: (stop.set() or 1))
    monkeypatch.setattr(transactions, 'reconcile_canonical_publications',
                        reconcile)

    assert transactions.reconcile_pending_canonical_publications(
        should_stop=stop.is_set) == 1

    reconcile.assert_called_once_with('location-one')


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
    ambient_session = mock.Mock()
    session_with_defaults = mock.Mock(return_value=ambient_session)
    monkeypatch.setattr(lifecycle_worker_service.aws.aws_adaptor,
                        'session_with_client_defaults', session_with_defaults)

    with pytest.raises(worker_lease.LeaseLostError,
                       match='lease lost before STS'):
        lifecycle_worker_service.evict_location(location, mock.Mock())

    session_with_defaults.assert_called_once_with(connect_timeout=10,
                                                  read_timeout=60,
                                                  total_max_attempts=1,
                                                  profile=None)
    ambient_session.get_credentials.assert_not_called()


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
                        lambda *_args, **_kwargs: 100)
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


def test_aws_source_reader_confines_private_peer_to_exact_ecr_authority(
        monkeypatch: pytest.MonkeyPatch) -> None:
    authority = '123456789012.dkr.ecr.us-east-1.amazonaws.com'
    binding = models.RegistryAccessBinding(
        id='ecr-source',
        kind=models.RegistryAccessBindingKind.AWS_ASSUME_ROLE,
        purposes=('source_read',),
        authority='arn:aws:iam::123456789012:role/SkyPilotImageSource')
    source = catalog_state.SourceRecord(
        id='00000000-0000-4000-8000-000000000010',
        workspace='research',
        image_id='00000000-0000-4000-8000-000000000011',
        source_ref=f'{authority}/example/runtime@{_DIGEST}',
        source_root_digest=_DIGEST,
        source_root_media_type='',
        requested_platform='linux/amd64',
        selected_child_digest='',
        source_auth_binding_id=binding.id,
        source_auth_fingerprint=binding.fingerprint,
        created_at=10)
    monkeypatch.setattr(copy_worker_service.config, 'get_source_binding',
                        lambda _: binding)
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'get_catalog_authority_id', lambda: 'catalog')
    credentials = mock.Mock()
    mint = mock.Mock(return_value=credentials)
    monkeypatch.setattr(copy_worker_service.aws, 'mint_ecr_source_credentials',
                        mint)
    reader = mock.sentinel.reader
    constructor = mock.Mock(return_value=reader)
    monkeypatch.setattr(copy_worker_service.providers, 'RegistryV2Source',
                        constructor)
    fence = mock.Mock()

    assert copy_worker_service._source_reader(source, 'gpu-production',
                                              fence) is reader

    constructor.assert_called_once_with(source.source_ref,
                                        mock.ANY,
                                        provider_fence=fence,
                                        private_peer_authority=authority)
    resolver = constructor.call_args.args[1]
    assert resolver() is credentials
    mint.assert_called_once()
    assert mint.call_args.kwargs['region'] == 'us-east-1'
    assert mint.call_args.kwargs['account'] == '123456789012'
    assert mint.call_args.kwargs['expected_authority'] == authority
    assert mint.call_args.kwargs['provider_fence'] is fence


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


def test_copy_worker_stop_during_heartbeat_fences_new_work(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = copy_worker_service.CopyWorkerService(worker_id='copy-worker',
                                                    version='test',
                                                    max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(copy_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(copy_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    monkeypatch.setattr(
        copy_worker_service.topology_state, 'heartbeat_worker',
        mock.Mock(
            side_effect=lambda *_args, **_kwargs: (service.stop() or True)))
    reload_config = mock.Mock()
    qualification_maintenance = mock.Mock()
    claim = mock.Mock()
    monkeypatch.setattr(copy_worker_service.skypilot_config,
                        'safe_reload_config', reload_config)
    monkeypatch.setattr(copy_worker_service, '_qualification_maintenance',
                        qualification_maintenance)
    monkeypatch.setattr(service, '_claim', claim)
    monkeypatch.delenv('SKYPILOT_IMAGE_QUALIFICATION_MANIFEST_DIR',
                       raising=False)

    service.run_forever()

    reload_config.assert_called_once_with()
    qualification_maintenance.assert_not_called()
    claim.assert_not_called()
    executor.submit.assert_not_called()


def test_copy_worker_stop_during_config_reload_skips_manifest_ingestion(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = copy_worker_service.CopyWorkerService(worker_id='copy-worker',
                                                    version='test',
                                                    max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(copy_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(copy_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    heartbeat = mock.Mock()
    monkeypatch.setattr(copy_worker_service.topology_state, 'heartbeat_worker',
                        heartbeat)
    monkeypatch.setattr(copy_worker_service.time, 'monotonic', lambda: 61.0)
    monkeypatch.setenv('SKYPILOT_IMAGE_QUALIFICATION_MANIFEST_DIR',
                       '/qualification')
    monkeypatch.setattr(copy_worker_service.skypilot_config,
                        'safe_reload_config',
                        mock.Mock(side_effect=service.stop))
    ingest = mock.Mock()
    monkeypatch.setattr(copy_worker_service, '_ingest_qualification_manifests',
                        ingest)

    service.run_forever()

    ingest.assert_not_called()
    heartbeat.assert_not_called()
    executor.submit.assert_not_called()


@pytest.mark.parametrize('claim_kind',
                         ['initial_inventory', 'publication', 'location'])
def test_copy_internal_claim_sequence_starts_no_later_claim_after_stop(
        monkeypatch: pytest.MonkeyPatch, claim_kind: str) -> None:
    service = copy_worker_service.CopyWorkerService(worker_id='copy-worker',
                                                    version='test',
                                                    max_in_flight=1)
    events: list[str] = []

    def stop_during(kind: str):

        def claim(**_kwargs: object) -> object:
            events.append(kind)
            service.stop()
            return mock.sentinel.claimed

        return claim

    inventory = mock.Mock(return_value=None)
    publication = mock.Mock(return_value=None)
    location = mock.Mock(return_value=None)
    if claim_kind == 'initial_inventory':
        service._claims_since_inventory = 16
        inventory.side_effect = stop_during('inventory')
        expected = ['inventory']
    elif claim_kind == 'publication':
        service._claims_since_inventory = 0
        service._claim_inspection_next = True
        publication.side_effect = stop_during('publication')
        expected = ['publication']
    else:
        service._claims_since_inventory = 0
        service._claim_inspection_next = False
        location.side_effect = stop_during('location')
        expected = ['location']
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'claim_inventory_shard', inventory)
    monkeypatch.setattr(copy_worker_service.catalog_state,
                        'claim_publication_inspection', publication)
    monkeypatch.setattr(copy_worker_service.topology_state,
                        'claim_next_location', location)

    assert service._claim() is None
    assert events == expected


def test_lifecycle_worker_stop_during_heartbeat_fences_new_work(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = lifecycle_worker_service.LifecycleWorkerService(
        worker_id='lifecycle-worker',
        version='test',
        max_in_flight=1,
        retention_seconds=60)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(lifecycle_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'register_worker', mock.Mock())
    monkeypatch.setattr(
        lifecycle_worker_service.topology_state, 'heartbeat_worker',
        mock.Mock(
            side_effect=lambda *_args, **_kwargs: (service.stop() or True)))
    maintenance = mock.Mock()
    consumers = mock.Mock()
    qualification_lifecycle = mock.Mock()
    canonical_reconciliation = mock.Mock()
    policy_refresh = mock.Mock()
    eviction_claim = mock.Mock()
    monkeypatch.setattr(service, '_maintenance', maintenance)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_reconcile_terminal_consumers', consumers)
    monkeypatch.setattr(lifecycle_worker_service,
                        'reconcile_qualification_lifecycle',
                        qualification_lifecycle)
    monkeypatch.setattr(lifecycle_worker_service,
                        'reconcile_failed_canonical_reservations',
                        canonical_reconciliation)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_refresh_workspace_eviction_retentions',
                        policy_refresh)
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'claim_next_eviction', eviction_claim)

    service.run_forever()

    maintenance.assert_not_called()
    consumers.assert_not_called()
    qualification_lifecycle.assert_not_called()
    canonical_reconciliation.assert_not_called()
    policy_refresh.assert_not_called()
    eviction_claim.assert_not_called()
    executor.submit.assert_not_called()


@pytest.mark.parametrize('stop_after', [0, 1, 2])
def test_lifecycle_maintenance_starts_no_later_substep_after_stop(
        monkeypatch: pytest.MonkeyPatch, stop_after: int) -> None:
    service = lifecycle_worker_service.LifecycleWorkerService(
        worker_id='lifecycle-worker',
        version='test',
        max_in_flight=1,
        retention_seconds=60)
    steps = [mock.Mock() for _ in range(4)]
    steps[stop_after].side_effect = lambda *_args, **_kwargs: service.stop()
    monkeypatch.setattr(lifecycle_worker_service,
                        '_reconcile_publication_fanout', steps[0])
    monkeypatch.setattr(lifecycle_worker_service.catalog_state,
                        'compact_terminal_records', steps[1])
    monkeypatch.setattr(lifecycle_worker_service.demand_state,
                        'compact_terminal_demands', steps[2])
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'compact_stale_workers', steps[3])

    service._maintenance()

    for index, step in enumerate(steps):
        assert step.call_count == int(index <= stop_after)


def test_canary_worker_stop_during_heartbeat_fences_paid_work(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = canary_worker_service.CanaryWorkerService(
        worker_id='canary-worker', version='test', max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(canary_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(canary_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    monkeypatch.setattr(
        canary_worker_service.topology_state, 'heartbeat_worker',
        mock.Mock(
            side_effect=lambda *_args, **_kwargs: (service.stop() or True)))
    claim = mock.Mock()
    monkeypatch.setattr(canary_worker_service.qualification, 'claim_canary',
                        claim)

    service.run_forever()

    claim.assert_not_called()
    executor.submit.assert_not_called()


def test_copy_worker_stop_during_claim_prevents_submission(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = copy_worker_service.CopyWorkerService(worker_id='copy-worker',
                                                    version='test',
                                                    max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(copy_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(copy_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    monkeypatch.setattr(copy_worker_service.topology_state, 'heartbeat_worker',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(copy_worker_service.time, 'monotonic', lambda: 1.0)
    claim = mock.Mock(side_effect=lambda:
                      (service.stop() or ('publication', mock.sentinel.record)))
    monkeypatch.setattr(service, '_claim', claim)
    monkeypatch.delenv('SKYPILOT_IMAGE_QUALIFICATION_MANIFEST_DIR',
                       raising=False)

    service.run_forever()

    claim.assert_called_once_with()
    executor.submit.assert_not_called()


def test_lifecycle_worker_stop_during_claim_prevents_submission(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = lifecycle_worker_service.LifecycleWorkerService(
        worker_id='lifecycle-worker',
        version='test',
        max_in_flight=1,
        retention_seconds=60)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(lifecycle_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'register_worker', mock.Mock())
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'heartbeat_worker', mock.Mock(return_value=True))
    monkeypatch.setattr(lifecycle_worker_service.time, 'monotonic',
                        lambda: 61.0)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_CONSUMER_RECONCILIATION_SECONDS', 1000)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_refresh_workspace_eviction_retentions',
                        mock.Mock(return_value={}))
    claim = mock.Mock(
        side_effect=lambda **_kwargs: (service.stop() or mock.sentinel.record))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'claim_next_eviction', claim)

    service.run_forever()

    claim.assert_called_once_with(worker_id='lifecycle-worker',
                                  retention_seconds=60,
                                  workspace_retention_seconds={},
                                  lease_seconds=service.lease_seconds)
    executor.submit.assert_not_called()


def test_copy_worker_stop_inside_submission_gate_never_invokes_claimed_task(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = copy_worker_service.CopyWorkerService(worker_id='copy-worker',
                                                    version='test',
                                                    max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    future = mock.Mock()
    target = mock.Mock(return_value=True)

    def submit(entry, *args, **kwargs):
        service.stop()
        assert entry(*args, **kwargs) is False
        return future

    executor.submit.side_effect = submit
    monkeypatch.setattr(copy_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(copy_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    monkeypatch.setattr(copy_worker_service.topology_state, 'heartbeat_worker',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(copy_worker_service.time, 'monotonic', lambda: 1.0)
    monkeypatch.setattr(
        service, '_claim',
        mock.Mock(return_value=('publication', mock.sentinel.record)))
    monkeypatch.setattr(copy_worker_service, 'inspect_publication', target)
    monkeypatch.delenv('SKYPILOT_IMAGE_QUALIFICATION_MANIFEST_DIR',
                       raising=False)

    service.run_forever()

    executor.submit.assert_called_once()
    target.assert_not_called()


def test_lifecycle_worker_stop_inside_submission_gate_never_invokes_claimed_task(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = lifecycle_worker_service.LifecycleWorkerService(
        worker_id='lifecycle-worker',
        version='test',
        max_in_flight=1,
        retention_seconds=60)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    future = mock.Mock()
    target = mock.Mock(return_value=True)

    def submit(entry, *args, **kwargs):
        service.stop()
        assert entry(*args, **kwargs) is False
        return future

    executor.submit.side_effect = submit
    monkeypatch.setattr(lifecycle_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'register_worker', mock.Mock())
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'heartbeat_worker', mock.Mock(return_value=True))
    monkeypatch.setattr(lifecycle_worker_service.time, 'monotonic',
                        lambda: 61.0)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_CONSUMER_RECONCILIATION_SECONDS', 1000)
    monkeypatch.setattr(lifecycle_worker_service,
                        '_refresh_workspace_eviction_retentions',
                        mock.Mock(return_value={}))
    monkeypatch.setattr(lifecycle_worker_service.topology_state,
                        'claim_next_eviction',
                        mock.Mock(return_value=mock.sentinel.record))
    monkeypatch.setattr(lifecycle_worker_service, 'evict_location', target)

    service.run_forever()

    executor.submit.assert_called_once()
    target.assert_not_called()


def test_canary_worker_stop_during_claim_prevents_submission(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = canary_worker_service.CanaryWorkerService(
        worker_id='canary-worker', version='test', max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    monkeypatch.setattr(canary_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(canary_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    monkeypatch.setattr(canary_worker_service.topology_state,
                        'heartbeat_worker', mock.Mock(return_value=True))
    claim = mock.Mock(side_effect=lambda **_kwargs:
                      (service.stop() or mock.sentinel.operation))
    monkeypatch.setattr(canary_worker_service.qualification, 'claim_canary',
                        claim)
    release = mock.Mock(return_value=True)
    monkeypatch.setattr(canary_worker_service.qualification,
                        'release_drained_canary', release)

    service.run_forever()

    claim.assert_called_once_with(worker_id='canary-worker',
                                  lease_seconds=service.lease_seconds)
    release.assert_called_once_with(mock.sentinel.operation,
                                    teardown_verified=True)
    executor.submit.assert_not_called()


def test_canary_worker_submits_shared_drain_event(
        monkeypatch: pytest.MonkeyPatch) -> None:
    service = canary_worker_service.CanaryWorkerService(
        worker_id='canary-worker', version='test', max_in_flight=1)
    executor = mock.Mock()
    executor_context = mock.MagicMock()
    executor_context.__enter__.return_value = executor
    future = mock.Mock()
    executor.submit.side_effect = lambda *_args, **_kwargs: (service.stop() or
                                                             future)
    monkeypatch.setattr(canary_worker_service.concurrent.futures,
                        'ThreadPoolExecutor',
                        mock.Mock(return_value=executor_context))
    monkeypatch.setattr(canary_worker_service.topology_state, 'register_worker',
                        mock.Mock())
    monkeypatch.setattr(canary_worker_service.topology_state,
                        'heartbeat_worker', mock.Mock(return_value=True))
    operation = _canary_operation()
    monkeypatch.setattr(canary_worker_service.qualification, 'claim_canary',
                        mock.Mock(return_value=operation))

    service.run_forever()

    executor.submit.assert_called_once_with(canary_worker_service.run_canary,
                                            operation,
                                            lease_seconds=service.lease_seconds,
                                            drain_event=service._stop)


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
