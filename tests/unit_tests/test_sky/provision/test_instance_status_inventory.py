"""Production-interface tests for bounded provider status inventories."""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from sky import provision
from sky.backends import backend_utils
from sky.provision import provider_facets
from sky.provision.aws import instance as aws_instance
from sky.provision.gcp import instance as gcp_instance
from sky.utils import status_lib


@pytest.fixture(autouse=True)
def _isolate_provider_registries(monkeypatch):
    monkeypatch.setattr(provision, '_registered_provisioners', {})
    monkeypatch.setattr(provision, '_registered_provisioner_bundles', {})
    monkeypatch.setattr(provision, '_legacy_mixed_owner_diagnostics', set())


def _query(index: int,
           provider_config: dict[str, Any],
           *,
           prefix: str = 'cluster'
          ) -> (provider_facets.InstanceStatusInventoryQueryV1):
    name = f'{prefix}-{index}'
    return provider_facets.InstanceStatusInventoryQueryV1(
        query_id=str(index),
        cluster_name=f'display-{name}',
        cluster_name_on_cloud=name,
        provider_config=provider_config)


def test_aws_800_queries_use_one_bounded_region_inventory(monkeypatch):
    queries = tuple(
        _query(index, {'region': 'us-east-1'}) for index in range(800))
    instances = [{
        'InstanceId': f'i-{index}',
        'State': {
            'Name': 'running'
        },
        'Tags': [{
            'Key': 'ray-cluster-name',
            'Value': f'cluster-{index}',
        }],
    } for index in range(800)]
    client = SimpleNamespace(describe_instances=mock.Mock(
        return_value={'Reservations': [{
            'Instances': instances
        }]}))
    session = SimpleNamespace(client=mock.Mock(return_value=client))
    session_factory = mock.Mock(return_value=session)
    monkeypatch.setattr(aws_instance.aws, 'session_with_client_defaults',
                        session_factory)
    monkeypatch.setattr(aws_instance.aws, 'get_workspace_profile',
                        mock.Mock(return_value='workspace'))

    observations = aws_instance.query_instances_batch(
        queries, deadline_monotonic=time.monotonic() + 30)

    assert len(observations) == 800
    assert all(observation.disposition is
               provider_facets.InstanceStatusInventoryDispositionV1.OBSERVED
               for observation in observations)
    assert all(len(observation.entries) == 1 for observation in observations)
    session_factory.assert_called_once_with(connect_timeout=5,
                                            read_timeout=10,
                                            total_max_attempts=1,
                                            profile='workspace')
    session.client.assert_called_once_with('ec2', region_name='us-east-1')
    client.describe_instances.assert_called_once()
    request = client.describe_instances.call_args.kwargs
    assert request['MaxResults'] == 1000
    assert request['Filters'] == [{
        'Name': 'tag-key',
        'Values': ['ray-cluster-name'],
    }, {
        'Name': 'instance-state-name',
        'Values': ['pending', 'running', 'stopping', 'stopped'],
    }]


def test_aws_partition_failure_is_unknown_without_singleton_retry(monkeypatch):
    queries = (_query(1, {'region': 'us-east-1'}),
               _query(2, {'region': 'us-west-2'}))
    east_client = SimpleNamespace(describe_instances=mock.Mock(
        side_effect=TimeoutError('east down')))
    west_client = SimpleNamespace(describe_instances=mock.Mock(
        return_value={'Reservations': []}))

    def _client(_service: str, *, region_name: str) -> Any:
        return east_client if region_name == 'us-east-1' else west_client

    session = SimpleNamespace(client=mock.Mock(side_effect=_client))
    monkeypatch.setattr(aws_instance.aws, 'session_with_client_defaults',
                        mock.Mock(return_value=session))
    monkeypatch.setattr(aws_instance.aws, 'get_workspace_profile',
                        mock.Mock(return_value=None))

    observations = aws_instance.query_instances_batch(
        queries, deadline_monotonic=time.monotonic() + 30)

    assert observations[0].disposition is (
        provider_facets.InstanceStatusInventoryDispositionV1.UNKNOWN)
    assert observations[1].disposition is (
        provider_facets.InstanceStatusInventoryDispositionV1.OBSERVED)
    assert observations[1].entries == ()
    east_client.describe_instances.assert_called_once()
    west_client.describe_instances.assert_called_once()


def test_gcp_800_queries_use_one_bounded_zone_inventory(monkeypatch):
    config = {
        'project_id': 'project',
        'availability_zone': 'us-central1-a',
    }
    queries = tuple(_query(index, config) for index in range(800))
    items = [{
        'name': f'instance-{index}',
        'status': 'RUNNING',
        'labels': {
            'ray-cluster-name': f'cluster-{index}'
        },
    } for index in range(800)]
    request = SimpleNamespace(http=SimpleNamespace(timeout=None),
                              execute=mock.Mock(return_value={'items': items}))
    instances_api = SimpleNamespace(list=mock.Mock(return_value=request))
    compute = SimpleNamespace(instances=mock.Mock(return_value=instances_api))
    monkeypatch.setattr(gcp_instance.instance_utils.GCPComputeInstance,
                        'load_resource', mock.Mock(return_value=compute))

    observations = gcp_instance.query_instances_batch(
        queries, deadline_monotonic=time.monotonic() + 30)

    assert len(observations) == 800
    assert all(observation.disposition is
               provider_facets.InstanceStatusInventoryDispositionV1.OBSERVED
               for observation in observations)
    assert all(len(observation.entries) == 1 for observation in observations)
    gcp_instance.instance_utils.GCPComputeInstance.load_resource.assert_called_once(
    )
    instances_api.list.assert_called_once()
    request.execute.assert_called_once_with(num_retries=0)
    assert request.http.timeout == 10
    assert instances_api.list.call_args.kwargs == {
        'project': 'project',
        'zone': 'us-central1-a',
        'maxResults': 500,
    }


class _StatefulInventoryLifecycle:
    """Complete strict lifecycle plus one swappable in-memory inventory."""

    def __init__(self) -> None:
        self.batch_calls = 0
        self.deadlines: list[float] = []
        self.live_clusters = {'cluster-1', 'cluster-2'}

    def query_instances_batch(
        self,
        queries: tuple[provider_facets.InstanceStatusInventoryQueryV1, ...],
        *,
        deadline_monotonic: float,
    ) -> tuple[provider_facets.InstanceStatusInventoryObservationV1, ...]:
        self.batch_calls += 1
        self.deadlines.append(deadline_monotonic)
        return tuple(
            provider_facets.InstanceStatusInventoryObservationV1(
                query_id=query.query_id,
                disposition=(provider_facets.
                             InstanceStatusInventoryDispositionV1.OBSERVED),
                entries=((provider_facets.InstanceStatusInventoryEntryV1(
                    instance_id=f'instance-{query.query_id}',
                    status=status_lib.ClusterStatus.UP),) if query.
                         cluster_name_on_cloud in self.live_clusters else ()))
            for query in queries)

    def query_instances(self,
                        cluster_name: str,
                        cluster_name_on_cloud: str,
                        provider_config: dict[str, Any] | None = None,
                        non_terminated_only: bool = True,
                        retry_if_missing: bool = False) -> Any:
        raise AssertionError('singleton provider query is forbidden')

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


class _FakeCloud:

    def __repr__(self) -> str:
        return 'inventory-fake'


def test_registered_stateful_inventory_replays_without_singleton_queries():
    lifecycle = _StatefulInventoryLifecycle()
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(canonical_name='inventory-fake',
                                            instance_lifecycle=lifecycle))
    queries = (_query(1, {'region': 'test'}), _query(2, {'region': 'test'}))

    first = provision.query_instances_batch(
        'inventory-fake', queries, deadline_monotonic=time.monotonic() + 30)
    lifecycle.live_clusters.remove('cluster-2')
    second = provision.query_instances_batch(
        'inventory-fake', queries, deadline_monotonic=time.monotonic() + 30)

    assert lifecycle.batch_calls == 2
    assert tuple(len(item.entries) for item in first) == (1, 1)
    assert tuple(len(item.entries) for item in second) == (1, 0)


def test_backend_projects_800_handles_through_one_registered_batch():
    lifecycle = _StatefulInventoryLifecycle()
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(canonical_name='inventory-fake',
                                            instance_lifecycle=lifecycle))
    handles = {
        index: SimpleNamespace(
            launched_resources=SimpleNamespace(cloud=_FakeCloud()),
            cluster_name=f'display-{index}',
            cluster_name_on_cloud=f'cluster-{index}') for index in range(800)
    }
    configs = {index: {'region': 'test'} for index in range(800)}

    observations = backend_utils.query_cluster_instance_statuses_batch(
        handles, configs)

    assert lifecycle.batch_calls == 1
    assert len(observations) == 800
    assert all(observation.disposition is
               provider_facets.InstanceStatusInventoryDispositionV1.OBSERVED
               for observation in observations.values())


def test_backend_chunks_large_fleet_without_singleton_queries():
    lifecycle = _StatefulInventoryLifecycle()
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(canonical_name='inventory-fake',
                                            instance_lifecycle=lifecycle))
    handles = {
        index: SimpleNamespace(
            launched_resources=SimpleNamespace(cloud=_FakeCloud()),
            cluster_name=f'display-{index}',
            cluster_name_on_cloud=f'cluster-{index}') for index in range(1601)
    }
    configs = {index: {'region': 'test'} for index in handles}

    observations = backend_utils.query_cluster_instance_statuses_batch(
        handles, configs)

    assert lifecycle.batch_calls == 3
    assert len(set(lifecycle.deadlines)) == 1
    assert len(observations) == 1601


def test_old_provider_is_unknown_without_builtin_or_singleton_fallback(
        monkeypatch):
    lifecycle = _StatefulInventoryLifecycle()
    lifecycle.query_instances_batch = None  # type: ignore[assignment]
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(canonical_name='aws',
                                            instance_lifecycle=lifecycle))
    builtin_batch = mock.Mock(side_effect=AssertionError('fallback'))
    monkeypatch.setattr(provision.aws, 'query_instances_batch', builtin_batch)

    observations = provision.query_instances_batch(
        'aws', (_query(1, {'region': 'us-east-1'}),),
        deadline_monotonic=time.monotonic() + 30)

    assert observations[0].disposition is (
        provider_facets.InstanceStatusInventoryDispositionV1.UNKNOWN)
    assert 'no batch' in (observations[0].error or '')
    builtin_batch.assert_not_called()


def test_inventory_hard_bound_rejects_before_provider_call(monkeypatch):
    builtin_batch = mock.Mock()
    monkeypatch.setattr(provision.aws, 'query_instances_batch', builtin_batch)
    queries = tuple(
        _query(index, {'region': 'us-east-1'}) for index in range(801))

    with pytest.raises(ValueError, match='hard bound'):
        provision.query_instances_batch('aws',
                                        queries,
                                        deadline_monotonic=time.monotonic() +
                                        30)

    builtin_batch.assert_not_called()
