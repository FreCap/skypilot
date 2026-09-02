"""Exact-status failures use one registered provider inventory projection."""
# pylint: disable=protected-access

import concurrent.futures
import copy
from typing import Any
from unittest import mock

from sky import backends
from sky import exceptions
from sky import provision
from sky.provision import provider_facets
from sky.serve import replica_managers
from sky.utils import common_utils
from sky.utils import status_lib


class _InventoryCloud:

    def __repr__(self) -> str:
        return 'serve-exact-status-inventory-fake'


class _InventoryLifecycle:

    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    def query_instances_batch(
        self,
        queries: tuple[provider_facets.InstanceStatusInventoryQueryV1, ...],
        *,
        deadline_monotonic: float,
    ) -> tuple[provider_facets.InstanceStatusInventoryObservationV1, ...]:
        del deadline_monotonic
        self.batch_sizes.append(len(queries))
        return tuple(
            provider_facets.InstanceStatusInventoryObservationV1(
                query_id=query.query_id,
                disposition=(provider_facets.
                             InstanceStatusInventoryDispositionV1.OBSERVED),
                entries=()) for query in queries)

    def query_instances(
        self,
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
        del (cluster_name, cluster_name_on_cloud, provider_config,
             non_terminated_only, retry_if_missing)
        raise AssertionError('singleton provider lookup is forbidden')

    def bootstrap_instances(self, region: str, cluster_name_on_cloud: str,
                            config: Any) -> Any:
        del region, cluster_name_on_cloud
        return config

    def run_instances(self, region: str, cluster_name: str,
                      cluster_name_on_cloud: str, config: Any) -> Any:
        del region, cluster_name, cluster_name_on_cloud, config
        return None

    def stop_instances(self,
                       cluster_name_on_cloud: str,
                       provider_config: dict[str, Any],
                       worker_only: bool = False) -> None:
        del cluster_name_on_cloud, provider_config, worker_only
        return None

    def terminate_instances(self,
                            cluster_name_on_cloud: str,
                            provider_config: dict[str, Any],
                            worker_only: bool = False) -> None:
        del cluster_name_on_cloud, provider_config, worker_only
        return None

    def wait_instances(self, region: str, cluster_name_on_cloud: str,
                       state: status_lib.ClusterStatus | None) -> None:
        del region, cluster_name_on_cloud, state
        return None

    def get_cluster_info(self,
                         region: str,
                         cluster_name_on_cloud: str,
                         provider_config: dict[str, Any] | None = None) -> Any:
        del region, cluster_name_on_cloud, provider_config
        return None


def _handle(replica_id: int) -> backends.CloudVmRayResourceHandle:
    handle = backends.CloudVmRayResourceHandle.__new__(
        backends.CloudVmRayResourceHandle)
    handle.cluster_name = f'svc-{replica_id}'
    handle.cluster_name_on_cloud = f'provider-{replica_id}'
    handle.launched_nodes = 1
    handle.launched_resources = mock.Mock(cloud=_InventoryCloud())
    handle._cluster_yaml = '/unused/provider.yaml'
    return handle


def _info(replica_id: int) -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    return info


def test_800_command_errors_use_one_facet_and_four_bounded_commits() -> None:
    lifecycle = _InventoryLifecycle()
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(
            canonical_name='serve-exact-status-inventory-fake',
            instance_lifecycle=lifecycle))
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._is_pool = False
    manager._apply_confirmed_preemption = mock.Mock()
    manager._persist_spot_placement_state_if_dirty = mock.Mock()
    manager._cloud_instance_looks_alive = mock.Mock(
        side_effect=AssertionError('singleton liveness lookup is forbidden'))
    captured_plans = []

    def _accept(plans):
        captured_plans.extend(plans)
        return ({
            plan.opening_info.replica_id: copy.deepcopy(plan.desired_info)
            for plan in plans
        }, set())

    commit = mock.Mock(side_effect=_accept)
    manager._commit_probe_row_plans = commit

    fetch_results = []
    for replica_id in range(1, 801):
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        future.set_exception(
            exceptions.CommandError(255, 'status', 'unreachable', None))
        fetch_results.append((_info(replica_id), _handle(replica_id), future))

    with mock.patch.object(replica_managers.serve_utils,
                           'get_provider_configs_for_handles',
                           side_effect=lambda handles, **_kwargs:
                           {replica_id: {} for replica_id in handles}):
        manager._handle_job_status_results(
            fetch_results,
            provider_error_phase_mode=(replica_managers.provider_phase.
                                       ProviderPhaseMode.AMBIENT_LEGACY))

    assert lifecycle.batch_sizes == [800]
    manager._cloud_instance_looks_alive.assert_not_called()
    assert [len(call.args[0]) for call in commit.call_args_list
           ] == [256, 256, 256, 32]
    plans = captured_plans
    assert all(
        plan.effects.preempted and plan.effects.teardown for plan in plans)
    assert manager._apply_confirmed_preemption.call_count == 800
    assert all(
        call.args[1] is None and call.kwargs == {'persist_placement': False}
        for call in manager._apply_confirmed_preemption.call_args_list)
    assert all(plan.desired_info.status_property.sky_down_status ==
               common_utils.ProcessStatus.SCHEDULED for plan in plans)


def test_exact_status_finishes_first_window_before_second_commit_fails(
) -> None:
    lifecycle = _InventoryLifecycle()
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(
            canonical_name='serve-exact-status-inventory-fake',
            instance_lifecycle=lifecycle))
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._is_pool = False
    manager._apply_confirmed_preemption = mock.Mock()
    manager._persist_spot_placement_state_if_dirty = mock.Mock()
    wake = mock.Mock()
    manager._launch_completion_state = mock.Mock(return_value=(mock.Mock(),
                                                               wake))
    route_registry = mock.Mock()
    manager._route_lease_registry = mock.Mock(return_value=route_registry)
    commit_calls = []

    def _commit(plans):
        commit_calls.append(len(plans))
        if len(commit_calls) == 2:
            raise RuntimeError('second commit failed')
        return ({
            plan.opening_info.replica_id: copy.deepcopy(plan.desired_info)
            for plan in plans
        }, set())

    manager._commit_probe_row_plans = mock.Mock(side_effect=_commit)
    fetch_results = []
    for replica_id in range(1, 258):
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        future.set_exception(
            exceptions.CommandError(255, 'status', 'unreachable', None))
        fetch_results.append((_info(replica_id), _handle(replica_id), future))

    with mock.patch.object(replica_managers.serve_utils,
                           'get_provider_configs_for_handles',
                           side_effect=lambda handles, **_kwargs:
                           {replica_id: {} for replica_id in handles}):
        try:
            manager._handle_job_status_results(
                fetch_results,
                provider_error_phase_mode=(replica_managers.provider_phase.
                                           ProviderPhaseMode.AMBIENT_LEGACY))
        except RuntimeError as error:
            assert str(error) == 'second commit failed'
        else:
            raise AssertionError('second window failure was not propagated')

    assert commit_calls == [256, 1]
    assert manager._apply_confirmed_preemption.call_count == 256
    manager._persist_spot_placement_state_if_dirty.assert_called_once_with()
    wake.set.assert_called_once_with()


def test_stale_exact_status_row_has_no_postcommit_effects_but_peer_commits(
) -> None:
    lifecycle = _InventoryLifecycle()
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(
            canonical_name='serve-exact-status-inventory-fake',
            instance_lifecycle=lifecycle))
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._is_pool = False
    manager._apply_confirmed_preemption = mock.Mock()
    manager._persist_spot_placement_state_if_dirty = mock.Mock()
    route_registry = mock.Mock()
    manager._route_lease_registry = mock.Mock(return_value=route_registry)

    captured_plans = []

    def _accept_only_peer(plans):
        captured_plans.extend(plans)
        accepted = {
            plan.opening_info.replica_id: copy.deepcopy(plan.desired_info)
            for plan in plans
            if plan.opening_info.replica_id == 2
        }
        return accepted, {1}

    manager._commit_probe_row_plans = mock.Mock(side_effect=_accept_only_peer)
    fetch_results = []
    for replica_id in (1, 2):
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        future.set_exception(
            exceptions.CommandError(255, 'status', 'unreachable', None))
        fetch_results.append((_info(replica_id), _handle(replica_id), future))

    with mock.patch.object(replica_managers.serve_utils,
                           'get_provider_configs_for_handles',
                           side_effect=lambda handles, **_kwargs:
                           {replica_id: {} for replica_id in handles}):
        manager._handle_job_status_results(
            fetch_results,
            provider_error_phase_mode=(replica_managers.provider_phase.
                                       ProviderPhaseMode.AMBIENT_LEGACY))

    assert lifecycle.batch_sizes == [2]
    assert len(captured_plans) == 2
    assert all(plan.effects.preempted and plan.effects.teardown
               for plan in captured_plans)
    manager._apply_confirmed_preemption.assert_called_once()
    assert manager._apply_confirmed_preemption.call_args.args[0].replica_id == 2
    route_registry.deactivate_record.assert_called_once()
    assert route_registry.deactivate_record.call_args.args[0] == 2
    manager._persist_spot_placement_state_if_dirty.assert_called_once_with()
