"""Exact auxiliary finalization tests for projected paid VM absence."""

from collections.abc import Callable
import contextlib
import dataclasses
import pathlib
from types import SimpleNamespace
from unittest import mock
import uuid

import pytest

from sky import clouds
from sky import global_user_state
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.server.requests import postgres as request_postgres
from sky.skylet import constants
from sky.utils import resources_utils

_SERVICE_NAME = 'paid-finalization'
_REPLICA_ID = 7
_REPLICA_RECORD_ID = '11111111-1111-4111-8111-111111111111'
_CLUSTER_NAME = 'paid-finalization-7'
_CLUSTER_NAME_ON_CLOUD = 'paid-finalization-7-tenant'
_CLUSTER_RECORD_UUID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_GCP_PROJECT_ID = 'paid-finalization-project'


def _identity() -> serve_state.ReplicaResourceActionIdentity:
    return serve_state.ReplicaResourceActionIdentity(
        replica_id=_REPLICA_ID,
        cluster_name=_CLUSTER_NAME,
        replica_incarnation=uuid.UUID('33333333-3333-4333-8333-333333333333'),
        desired_generation=1,
        sky_cluster_record_uuid=_CLUSTER_RECORD_UUID)


def _provider_identity(cloud: str) -> dict[str, object]:
    common: dict[str, object] = {
        'cluster_name_on_cloud': _CLUSTER_NAME_ON_CLOUD,
        'instance_type': ('g2-standard-4' if cloud == 'gcp' else 'g6.xlarge'),
        'num_nodes': 1,
        'region': 'us-central1' if cloud == 'gcp' else 'us-east-1',
        'use_spot': True,
        'workspace': 'default',
        'zone': 'us-central1-a' if cloud == 'gcp' else 'us-east-1a',
    }
    if cloud == 'gcp':
        common['project_id'] = _GCP_PROJECT_ID
    else:
        common.update({
            'aws_account_id': '123456789012',
            'client_token': 'a' * 64,
            'credential_profile': None,
        })
    return common


def _scope(
    cloud: str = 'gcp',
) -> request_postgres.ProjectedPaidAuxiliaryCleanupAuthority:
    return request_postgres.ProjectedPaidAuxiliaryCleanupAuthority(
        service_name=_SERVICE_NAME,
        replica_record_id=uuid.UUID(_REPLICA_RECORD_ID),
        resource_action_identity=_identity(),
        cleanup_scope=(
            request_postgres.ProjectedPaidProviderAbsenceCleanupScope(
                cloud=cloud, provider_identity=_provider_identity(cloud))))


def _handle(
    cloud: str = 'gcp',
    *,
    ports: list[str] | None = None,
    cluster_yaml: str | None = None,
    managed_image: object | None = None,
    volumes: list[dict[str, object]] | None = None,
    network_tier: resources_utils.NetworkTier | None = None,
) -> cloud_vm_ray_backend.CloudVmRayResourceHandle:
    handle = object.__new__(cloud_vm_ray_backend.CloudVmRayResourceHandle)
    provider = clouds.GCP() if cloud == 'gcp' else clouds.AWS()
    identity = _provider_identity(cloud)
    handle.cluster_name = _CLUSTER_NAME
    handle.cluster_name_on_cloud = _CLUSTER_NAME_ON_CLOUD
    handle.cluster_yaml = cluster_yaml
    handle.launched_nodes = 1
    handle.launched_resources = SimpleNamespace(
        cloud=provider,
        instance_type=identity['instance_type'],
        region=identity['region'],
        zone=identity['zone'],
        use_spot=True,
        ports=ports,
        is_image_managed=managed_image,
        volumes=volumes,
        network_tier=network_tier,
    )
    return handle


def _install_common(
    monkeypatch: pytest.MonkeyPatch,
    identity: serve_state.ReplicaResourceActionIdentity | None,
    *,
    cleanup_scope: request_postgres.ProjectedPaidAuxiliaryCleanupAuthority |
    None = None,
    cluster_exists: bool = True,
    snapshot: object | None = None,
):
    if cleanup_scope is None:
        cleanup_scope = _scope()
    info = SimpleNamespace(replica_id=_REPLICA_ID,
                           replica_record_id=_REPLICA_RECORD_ID,
                           cluster_name=_CLUSTER_NAME)
    authorize = mock.Mock(return_value=cleanup_scope)
    monkeypatch.setattr(
        replica_managers.request_postgres,
        'bound_non_pool_projected_paid_provider_absence_cleanup_scope',
        authorize)
    authorize_provider_free = mock.Mock(return_value=True)
    monkeypatch.setattr(
        replica_managers.request_postgres,
        'bound_non_pool_projected_provider_absence_is_authorized',
        authorize_provider_free)
    exact_replica = mock.Mock(return_value=(info, identity))
    monkeypatch.setattr(replica_managers.serve_state,
                        'get_replica_info_with_resource_action_identity',
                        exact_replica)
    exists = mock.Mock(return_value=cluster_exists)
    monkeypatch.setattr(replica_managers.global_user_state,
                        'cluster_with_name_exists', exists)
    read = mock.Mock(return_value=snapshot)
    monkeypatch.setattr(replica_managers.global_user_state,
                        'get_cluster_record_identity_snapshot', read)
    remove = mock.Mock(return_value=global_user_state.
                       ClusterRecordRemovalOutcome.REMOVED_EXACT)
    monkeypatch.setattr(replica_managers.global_user_state, 'remove_cluster',
                        remove)
    retire = mock.Mock(return_value=True)
    monkeypatch.setattr(
        replica_managers.request_postgres,
        'retire_bound_non_pool_projected_paid_provider_absence', retire)
    delete_firewall = mock.Mock(return_value=True)
    monkeypatch.setattr(replica_managers.gcp_provision,
                        'delete_exact_cluster_ports_firewall', delete_firewall)
    generic_cleanup = mock.Mock(
        side_effect=AssertionError('generic provider cleanup must not run'))
    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'post_teardown_cleanup', generic_cleanup)
    provider_census = mock.Mock(
        side_effect=AssertionError('VM/disk census must not run'))
    monkeypatch.setattr(replica_managers.non_pool_launch_reconciliation,
                        '_query_gcp_paid_provider_census', provider_census)
    remove_metadata = mock.Mock()
    monkeypatch.setattr(replica_managers.metadata_utils,
                        'remove_cluster_metadata', remove_metadata)
    remove_file = mock.Mock()
    monkeypatch.setattr(replica_managers.common_utils, 'remove_file_if_exists',
                        remove_file)

    acquired_locks: list[str] = []

    def _get_lock(lock_id: str, timeout: int):
        assert timeout == 1
        acquired_locks.append(lock_id)
        return contextlib.nullcontext()

    monkeypatch.setattr(replica_managers.locks, 'get_lock', _get_lock)
    return SimpleNamespace(authorize=authorize,
                           authorize_provider_free=authorize_provider_free,
                           exact_replica=exact_replica,
                           exists=exists,
                           read=read,
                           remove=remove,
                           retire=retire,
                           delete_firewall=delete_firewall,
                           generic_cleanup=generic_cleanup,
                           provider_census=provider_census,
                           remove_metadata=remove_metadata,
                           remove_file=remove_file,
                           acquired_locks=acquired_locks)


def _finalize(
    *,
    provider_operation_deadline_monotonic: float | None = None,
    continue_guard: Callable[[], bool] | None = None,
) -> bool:
    return replica_managers.finalize_projected_paid_provider_absence(
        _SERVICE_NAME,
        _REPLICA_ID,
        _REPLICA_RECORD_ID,
        _CLUSTER_NAME,
        provider_operation_deadline_monotonic=(
            provider_operation_deadline_monotonic),
        continue_guard=continue_guard)


def test_gcp_ports_cleanup_precedes_exact_database_retirement(
        monkeypatch: pytest.MonkeyPatch) -> None:
    yaml_path = str(
        pathlib.Path(constants.SKY_USER_FILE_PATH).expanduser().resolve() /
        f'{_CLUSTER_NAME}.yml')
    handle = _handle(ports=['8080'], cluster_yaml=yaml_path)
    calls: list[str] = []
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))
    installed.authorize.side_effect = lambda *_args: calls.append('authorize'
                                                                 ) or _scope()
    installed.delete_firewall.side_effect = lambda *_args: calls.append(
        'firewall') or True
    installed.remove_metadata.side_effect = lambda *_args: calls.append(
        'metadata')

    def _record_removed_file(path: str) -> None:
        if '/ssh/' in path:
            calls.append('ssh')
        elif path.endswith('.debug'):
            calls.append('yaml.debug')
        else:
            calls.append('yaml')

    installed.remove_file.side_effect = _record_removed_file
    installed.remove.side_effect = lambda *_args, **_kwargs: calls.append(
        'cluster-row'
    ) or global_user_state.ClusterRecordRemovalOutcome.REMOVED_EXACT
    installed.retire.side_effect = lambda *_args: calls.append('replica-row'
                                                              ) or True

    assert _finalize()

    assert calls == [
        'authorize', 'firewall', 'metadata', 'ssh', 'yaml', 'yaml.debug',
        'cluster-row', 'replica-row'
    ]
    assert installed.acquired_locks == [
        backend_utils.cluster_status_lock_id(_CLUSTER_NAME),
        backend_utils.cluster_resource_operation_lock_id(_CLUSTER_NAME),
    ]
    installed.read.assert_called_once_with(_CLUSTER_NAME, _CLUSTER_RECORD_UUID)
    installed.delete_firewall.assert_called_once_with(_GCP_PROJECT_ID,
                                                      _CLUSTER_NAME_ON_CLOUD)
    installed.remove_metadata.assert_called_once_with(_CLUSTER_NAME)
    ssh_path = str(
        pathlib.Path(constants.SKY_USER_FILE_PATH).expanduser().resolve() /
        'ssh' / _CLUSTER_NAME)
    assert installed.remove_file.call_args_list == [
        mock.call(ssh_path),
        mock.call(yaml_path),
        mock.call(f'{yaml_path}.debug'),
    ]
    installed.remove.assert_called_once_with(
        _CLUSTER_NAME,
        terminate=True,
        expected_cluster_record_uuid=_CLUSTER_RECORD_UUID,
        expected_cluster_handle=handle)
    installed.generic_cleanup.assert_not_called()
    installed.provider_census.assert_not_called()


def test_gcp_firewall_failure_retains_rows_and_retries_exact_identity(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle(ports=['8080'])
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))
    installed.delete_firewall.side_effect = [
        RuntimeError('injected firewall failure'), True
    ]

    with pytest.raises(RuntimeError, match='injected firewall failure'):
        _finalize()

    installed.remove.assert_not_called()
    installed.retire.assert_not_called()
    installed.remove_metadata.assert_not_called()
    installed.remove_file.assert_not_called()

    assert _finalize()
    assert installed.delete_firewall.call_args_list == [
        mock.call(_GCP_PROJECT_ID, _CLUSTER_NAME_ON_CLOUD),
        mock.call(_GCP_PROJECT_ID, _CLUSTER_NAME_ON_CLOUD),
    ]
    installed.remove.assert_called_once()
    installed.retire.assert_called_once()


def test_lane_close_during_firewall_fences_late_database_mutations(
        monkeypatch: pytest.MonkeyPatch) -> None:
    lane = replica_managers.non_pool_launch_reconciliation.OneShotProviderObservationLane(
    )
    installed = _install_common(
        monkeypatch,
        _identity(),
        snapshot=SimpleNamespace(handle=_handle(ports=['8080'])))
    installed.delete_firewall.side_effect = lambda *_args, **_kwargs: (
        lane.close() or True)

    with pytest.raises(RuntimeError, match='lost lifecycle authority'):
        _finalize(continue_guard=lambda: lane.mutation_is_allowed)

    installed.remove_metadata.assert_not_called()
    installed.remove_file.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


def test_lane_close_after_cluster_retirement_fences_replica_retirement(
        monkeypatch: pytest.MonkeyPatch) -> None:
    lane = replica_managers.non_pool_launch_reconciliation.OneShotProviderObservationLane(
    )
    installed = _install_common(
        monkeypatch,
        _identity(),
        snapshot=SimpleNamespace(handle=_handle(ports=None)))
    installed.remove.side_effect = lambda *_args, **_kwargs: (lane.close(
    ) or global_user_state.ClusterRecordRemovalOutcome.REMOVED_EXACT)

    with pytest.raises(RuntimeError, match='lost lifecycle authority'):
        _finalize(continue_guard=lambda: lane.mutation_is_allowed)

    installed.remove.assert_called_once()
    installed.retire.assert_not_called()


def test_local_cleanup_failure_retains_rows_and_retries_idempotently(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle(ports=['8080'])
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))
    installed.remove_file.side_effect = [
        RuntimeError('injected ssh failure'), None
    ]

    with pytest.raises(RuntimeError, match='injected ssh failure'):
        _finalize()

    installed.remove.assert_not_called()
    installed.retire.assert_not_called()

    assert _finalize()
    assert installed.delete_firewall.call_count == 2
    assert installed.remove_metadata.call_count == 2
    assert installed.remove_file.call_count == 2
    installed.remove.assert_called_once()
    installed.retire.assert_called_once()


def test_foreign_cluster_yaml_fails_before_any_cleanup_effect(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle(ports=['8080'], cluster_yaml='/tmp/foreign.yml')
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))

    with pytest.raises(RuntimeError, match='cluster YAML'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove_metadata.assert_not_called()
    installed.remove_file.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


def test_exact_delete_404_is_success_and_retires_rows(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle(ports=['8080'])
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))
    installed.delete_firewall.return_value = False

    assert _finalize()

    installed.remove.assert_called_once()
    installed.retire.assert_called_once()


def test_gcp_firewall_wait_receives_remaining_phase_budget(
        monkeypatch: pytest.MonkeyPatch) -> None:
    installed = _install_common(
        monkeypatch,
        _identity(),
        snapshot=SimpleNamespace(handle=_handle(ports=['8080'])))

    assert _finalize(provider_operation_deadline_monotonic=117)

    installed.delete_firewall.assert_called_once_with(_GCP_PROJECT_ID,
                                                      _CLUSTER_NAME_ON_CLOUD,
                                                      deadline_monotonic=117)


def test_gcp_without_ports_performs_no_firewall_or_provider_census(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle(ports=None)
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))

    assert _finalize()

    installed.delete_firewall.assert_not_called()
    installed.provider_census.assert_not_called()
    installed.remove.assert_called_once()
    installed.retire.assert_called_once()


def test_aws_never_deletes_service_security_group_or_reads_provider(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle('aws', ports=['8080'])
    installed = _install_common(monkeypatch,
                                _identity(),
                                cleanup_scope=_scope('aws'),
                                snapshot=SimpleNamespace(handle=handle))

    assert _finalize()

    installed.delete_firewall.assert_not_called()
    installed.provider_census.assert_not_called()
    installed.generic_cleanup.assert_not_called()
    installed.remove.assert_called_once()
    installed.retire.assert_called_once()


@pytest.mark.parametrize('changed_field', [
    'cluster_name',
    'cluster_name_on_cloud',
    'instance_type',
    'region',
    'zone',
    'use_spot',
])
def test_malformed_or_mismatched_gcp_handle_fails_before_provider_effect(
        monkeypatch: pytest.MonkeyPatch, changed_field: str) -> None:
    handle = _handle(ports=['8080'])
    replacement = False if changed_field == 'use_spot' else 'replacement'
    setattr(
        handle if changed_field in ('cluster_name', 'cluster_name_on_cloud')
        else handle.launched_resources, changed_field, replacement)
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))

    with pytest.raises(RuntimeError, match='does not match'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


@pytest.mark.parametrize('launched_nodes', [True, 1.0])
def test_non_integer_launched_node_count_fails_before_cleanup_effect(
        monkeypatch: pytest.MonkeyPatch, launched_nodes: object) -> None:
    handle = _handle(ports=['8080'])
    handle.launched_nodes = launched_nodes
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))

    with pytest.raises(RuntimeError, match='does not match'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove_metadata.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


def test_malformed_gcp_provider_identity_fails_before_provider_effect(
        monkeypatch: pytest.MonkeyPatch) -> None:
    scope = _scope()
    malformed = dict(scope.provider_identity)
    del malformed['project_id']
    malformed_authority = dataclasses.replace(
        scope,
        cleanup_scope=(
            request_postgres.ProjectedPaidProviderAbsenceCleanupScope(
                cloud='gcp', provider_identity=malformed)))
    installed = _install_common(
        monkeypatch,
        _identity(),
        cleanup_scope=malformed_authority,
        snapshot=SimpleNamespace(handle=_handle(ports=['8080'])))

    with pytest.raises(RuntimeError, match='provider identity'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


@pytest.mark.parametrize('handle', [
    _handle(ports=['8080'], managed_image=True),
    _handle(ports=['8080'], managed_image='false'),
    _handle(ports=['8080'], volumes=[{
        'name': 'unsupported'
    }]),
    _handle(ports=['8080'], network_tier=resources_utils.NetworkTier.BEST),
])
def test_unsupported_paid_auxiliary_shape_is_retained(
        monkeypatch: pytest.MonkeyPatch,
        handle: cloud_vm_ray_backend.CloudVmRayResourceHandle) -> None:
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))

    with pytest.raises(RuntimeError, match='unsupported auxiliary resources'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


def test_replacement_uuid_conflict_has_zero_provider_or_database_effects(
        monkeypatch: pytest.MonkeyPatch) -> None:
    installed = _install_common(monkeypatch, _identity())
    installed.read.side_effect = (
        global_user_state.ClusterRecordIdentityConflictError('replacement UUID')
    )

    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='replacement UUID'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


def test_handle_change_after_idempotent_firewall_delete_retains_replica(
        monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _handle(ports=['8080'])
    installed = _install_common(monkeypatch,
                                _identity(),
                                snapshot=SimpleNamespace(handle=handle))
    installed.remove.side_effect = (
        global_user_state.ClusterRecordHandleChangedError('handle changed'))

    with pytest.raises(global_user_state.ClusterRecordHandleChangedError,
                       match='handle changed'):
        _finalize()

    installed.delete_firewall.assert_called_once()
    installed.retire.assert_not_called()


def test_absent_action_aware_cluster_row_is_prior_cleanup_receipt(
        monkeypatch: pytest.MonkeyPatch) -> None:
    installed = _install_common(monkeypatch, _identity(), snapshot=None)

    assert _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_called_once_with(_SERVICE_NAME, _REPLICA_ID,
                                             _REPLICA_RECORD_ID)


@pytest.mark.parametrize('cluster_exists', [True, False])
def test_missing_current_action_identity_cannot_consume_cleanup_authority(
        monkeypatch: pytest.MonkeyPatch, cluster_exists: bool) -> None:
    installed = _install_common(monkeypatch,
                                None,
                                cluster_exists=cluster_exists)

    with pytest.raises(RuntimeError, match='exact durable auxiliary'):
        _finalize()

    installed.exists.assert_not_called()
    installed.read.assert_not_called()
    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


@pytest.mark.parametrize('cleanup_authority', [
    dataclasses.replace(_scope(), service_name='replacement-service'),
    dataclasses.replace(
        _scope(),
        replica_record_id=uuid.UUID('44444444-4444-4444-8444-444444444444')),
    dataclasses.replace(_scope(),
                        resource_action_identity=dataclasses.replace(
                            _identity(),
                            replica_incarnation=uuid.UUID(
                                '55555555-5555-4555-8555-555555555555'))),
])
def test_typed_cleanup_authority_near_miss_has_no_cleanup_effect(
    monkeypatch: pytest.MonkeyPatch,
    cleanup_authority: request_postgres.ProjectedPaidAuxiliaryCleanupAuthority
) -> None:
    installed = _install_common(
        monkeypatch,
        _identity(),
        cleanup_scope=cleanup_authority,
        snapshot=SimpleNamespace(handle=_handle(ports=['8080'])))

    with pytest.raises(RuntimeError, match='exact durable auxiliary'):
        _finalize()

    installed.exists.assert_not_called()
    installed.read.assert_not_called()
    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


def test_lost_projected_absence_authority_performs_no_cleanup(
        monkeypatch: pytest.MonkeyPatch) -> None:
    installed = _install_common(monkeypatch, _identity())
    installed.authorize.return_value = None

    assert not _finalize()

    installed.exact_replica.assert_called_once()
    installed.authorize_provider_free.assert_not_called()
    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


@pytest.mark.parametrize(('cluster_exists', 'expected_retired'),
                         [(True, False), (False, True)])
def test_legacy_provider_free_fallback_never_performs_auxiliary_cleanup(
        monkeypatch: pytest.MonkeyPatch, cluster_exists: bool,
        expected_retired: bool) -> None:
    installed = _install_common(monkeypatch,
                                None,
                                cluster_exists=cluster_exists)
    installed.authorize.return_value = None

    assert _finalize() is expected_retired

    installed.authorize_provider_free.assert_called_once_with(
        _SERVICE_NAME, _REPLICA_ID, _REPLICA_RECORD_ID)
    installed.exists.assert_called_once_with(_CLUSTER_NAME)
    installed.read.assert_not_called()
    installed.delete_firewall.assert_not_called()
    installed.remove_metadata.assert_not_called()
    installed.remove_file.assert_not_called()
    installed.remove.assert_not_called()
    if expected_retired:
        installed.retire.assert_called_once_with(_SERVICE_NAME, _REPLICA_ID,
                                                 _REPLICA_RECORD_ID)
    else:
        installed.retire.assert_not_called()


def test_legacy_provider_free_fallback_requires_generic_authority(
        monkeypatch: pytest.MonkeyPatch) -> None:
    installed = _install_common(monkeypatch, None, cluster_exists=False)
    installed.authorize.return_value = None
    installed.authorize_provider_free.return_value = False

    assert not _finalize()

    installed.exists.assert_not_called()
    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()


@pytest.mark.parametrize('changed_field', ['replica_record_id', 'cluster_name'])
def test_replaced_replica_row_is_rejected_before_cluster_mutation(
        monkeypatch: pytest.MonkeyPatch, changed_field: str) -> None:
    installed = _install_common(monkeypatch, _identity())
    changed = {
        'replica_id': _REPLICA_ID,
        'replica_record_id': _REPLICA_RECORD_ID,
        'cluster_name': _CLUSTER_NAME,
    }
    changed[changed_field] = 'replacement'
    installed.exact_replica.return_value = (SimpleNamespace(**changed),
                                            _identity())

    with pytest.raises(RuntimeError, match='replaced replica row'):
        _finalize()

    installed.delete_firewall.assert_not_called()
    installed.remove.assert_not_called()
    installed.retire.assert_not_called()
