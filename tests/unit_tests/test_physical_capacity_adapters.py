"""Unit tests for the bounded physical-capacity source adapters."""

from __future__ import annotations

import dataclasses
import inspect

import pytest

from sky.jobs import naming as managed_job_naming
from sky.jobs import status_types as managed_job_statuses
from sky.physical_capacity import adapters
from sky.physical_capacity import contracts
from sky.physical_capacity import models
from sky.physical_capacity import source_queries
from sky.serve import serve_statuses
from sky.utils import status_lib

_CONTROLLER_ID = '11111111-1111-4111-8111-111111111111'
_CONTROLLER_GENERATION = 7


def _serve_selector(
    *,
    name: str = 'svc',
    kind: models.ProjectionSourceKind = models.ProjectionSourceKind.
    SERVE_SERVICE,
) -> contracts.ServeSourceSelector:
    return contracts.ServeSourceSelector('default', kind, name)


def _service_row(*,
                 name: str = 'svc',
                 pool: int = 0,
                 status: object = 'READY',
                 service_hash: object = 'service-hash',
                 resource_scope: object = 'service-hash',
                 lifecycle_epoch: object = 1,
                 controller_pid: object = 123,
                 controller_port: object = 8080,
                 controller_ip: object = '127.0.0.1',
                 workspace: object = 'default') -> dict[str, object]:
    return {
        'name': name,
        'workspace': workspace,
        'status': status,
        'pool': pool,
        'controller_pid': controller_pid,
        'controller_port': controller_port,
        'controller_ip': controller_ip,
        'hash': service_hash,
        'lifecycle_epoch': lifecycle_epoch,
        'resource_scope': resource_scope,
    }


def _replica_row(*,
                 service_name: str = 'svc',
                 replica_id: object = 0,
                 status: object = 'READY',
                 version: object = 1,
                 cluster_name: object = 'svc-cluster') -> dict[str, object]:
    return {
        'service_name': service_name,
        'replica_id': replica_id,
        'status': status,
        'version': version,
        'cluster_name': cluster_name,
    }


def _cluster_row(*,
                 name: str = 'svc-cluster',
                 cluster_hash: object = 'cluster-hash',
                 status: object = 'UP',
                 workspace: object = 'default',
                 is_managed: object = 1,
                 workload_type: object = 'service',
                 workload_id: object = 'svc',
                 workload_task_id: object = 1) -> dict[str, object]:
    return {
        'name': name,
        'cluster_hash': cluster_hash,
        'status': status,
        'workspace': workspace,
        'is_managed': is_managed,
        'workload_type': workload_type,
        'workload_id': workload_id,
        'workload_task_id': workload_task_id,
    }


def _history_row(*,
                 cluster_hash: str = 'cluster-hash',
                 workspace: object = 'default',
                 cloud: object = 'aws',
                 region: object = 'us-east-1',
                 zone: object = None,
                 num_nodes: object = 1,
                 is_managed: object = 1,
                 workload_type: object = 'service',
                 workload_id: object = 'svc',
                 workload_task_id: object = 1) -> dict[str, object]:
    return {
        'cluster_hash': cluster_hash,
        'workspace': workspace,
        'cloud': cloud,
        'region': region,
        'zone': zone,
        'num_nodes': num_nodes,
        'is_managed': is_managed,
        'workload_type': workload_type,
        'workload_id': workload_id,
        'workload_task_id': workload_task_id,
    }


def _job_selector(job_id: int = 42,
                  task_id: int = 0) -> contracts.ManagedJobTaskSelector:
    return contracts.ManagedJobTaskSelector('default', job_id, task_id)


def _job_info(*,
              job_id: int = 42,
              workspace: object = 'default',
              controller_instance_id: object = _CONTROLLER_ID,
              controller_generation: object = _CONTROLLER_GENERATION,
              pool: object = None,
              current_cluster_name: object = None,
              is_batch: object = False) -> dict[str, object]:
    return {
        'spot_job_id': job_id,
        'workspace': workspace,
        'controller_instance_id': controller_instance_id,
        'controller_generation': controller_generation,
        'pool': pool,
        'current_cluster_name': current_cluster_name,
        'is_batch': is_batch,
    }


def _task(*,
          row_id: object = 99,
          job_id: int = 42,
          task_id: int = 0,
          task_name: object = 'train',
          status: object = 'RUNNING') -> dict[str, object]:
    return {
        'job_id': row_id,
        'spot_job_id': job_id,
        'task_id': task_id,
        'task_name': task_name,
        'status': status,
    }


def _scan_serve(
    reader: source_queries.SourceReader,
    selector: contracts.ServeSourceSelector | None = None,
) -> contracts.PartitionEvidenceResult:
    selector = selector or _serve_selector()
    return adapters.scan_serve_partition(
        contracts.SourcePartition(selector.workspace,
                                  selector.source_kind), [selector],
        reader,
        controller_instance_id=_CONTROLLER_ID,
        controller_generation=_CONTROLLER_GENERATION)


def _scan_job(
    reader: source_queries.SourceReader,
    selector: contracts.ManagedJobTaskSelector | None = None,
    dependencies: list[contracts.SourceSelector] | None = None,
) -> contracts.PartitionEvidenceResult:
    selector = selector or _job_selector()
    return adapters.scan_managed_job_partition(
        contracts.SourcePartition(selector.workspace,
                                  selector.source_kind), [selector],
        dependencies or [selector],
        reader,
        controller_instance_id=_CONTROLLER_ID,
        controller_generation=_CONTROLLER_GENERATION)


def test_query_builders_select_only_authorized_scalars() -> None:
    allowed_tables = {
        'services', 'replicas', 'job_info', 'spot', 'clusters',
        'cluster_history'
    }
    forbidden = {
        'requested_resources', 'launched_resources', 'full_resources',
        'replica_info', 'handle', 'usage_intervals', 'spec', 'yaml_content'
    }
    seen_tables: set[str] = set()
    for statement in source_queries.iter_query_builders():
        assert statement.is_select
        tables = set(source_queries.selected_table_names(statement))
        assert len(tables) == 1
        assert tables <= allowed_tables
        seen_tables.update(tables)
        assert forbidden.isdisjoint(
            source_queries.selected_column_names(statement))
    assert seen_tables == allowed_tables
    requirements = source_queries.source_schema_requirements()
    assert {requirement.relation for requirement in requirements
           } == allowed_tables
    spot = next(requirement for requirement in requirements
                if requirement.relation == 'spot')
    assert ('spot_job_id', 'task_id') in spot.index_leading_columns
    source = inspect.getsource(source_queries)
    assert 'sky.adaptors' not in source
    assert 'sky.clouds' not in source

    exact_value_columns = {
        'services': {
            'name', 'workspace', 'status', 'pool', 'controller_pid',
            'controller_port', 'controller_ip', 'hash', 'lifecycle_epoch',
            'resource_scope'
        },
        'replicas': {
            'service_name', 'replica_id', 'status', 'version', 'cluster_name'
        },
        'job_info': {
            'spot_job_id', 'workspace', 'controller_instance_id',
            'controller_generation', 'pool', 'current_cluster_name', 'is_batch'
        },
        'spot': {'job_id', 'spot_job_id', 'task_id', 'task_name', 'status'},
        'clusters': {
            'name', 'cluster_hash', 'status', 'workspace', 'is_managed',
            'workload_type', 'workload_id', 'workload_task_id'
        },
        'cluster_history': {
            'cluster_hash', 'workspace', 'cloud', 'region', 'zone', 'num_nodes',
            'is_managed', 'workload_type', 'workload_id', 'workload_task_id'
        },
    }
    exact_length_columns = {
        'services': {
            'name', 'workspace_bytes', 'status_bytes', 'pool', 'controller_pid',
            'controller_port', 'controller_ip_bytes', 'hash_bytes',
            'lifecycle_epoch', 'resource_scope_bytes'
        },
        'replicas': {
            'service_name', 'replica_id', 'status_bytes', 'version',
            'cluster_name_bytes'
        },
        'job_info': {
            'spot_job_id', 'workspace_bytes', 'controller_instance_id_bytes',
            'controller_generation', 'pool_bytes', 'current_cluster_name_bytes',
            'is_batch'
        },
        'spot': {
            'job_id', 'spot_job_id', 'task_id', 'task_name_bytes',
            'status_bytes'
        },
        'clusters': {
            'name', 'cluster_hash_bytes', 'status_bytes', 'workspace_bytes',
            'is_managed', 'workload_type_bytes', 'workload_id_bytes',
            'workload_task_id'
        },
        'cluster_history': {
            'cluster_hash', 'workspace_bytes', 'cloud_bytes', 'region_bytes',
            'zone_bytes', 'num_nodes', 'is_managed', 'workload_type_bytes',
            'workload_id_bytes', 'workload_task_id'
        },
    }
    length_statements = (
        source_queries.service_length_query('service'),
        source_queries.replica_length_query('service', 1),
        source_queries.job_info_length_query(1),
        source_queries.spot_task_length_query(1, 0),
        source_queries.cluster_length_query('cluster'),
        source_queries.cluster_history_length_query('hash'),
    )
    for statement in length_statements:
        table = source_queries.selected_table_names(statement)[0]
        assert set(source_queries.selected_column_names(statement)) == (
            exact_length_columns[table])
    value_statements = (
        source_queries.service_value_query('service'),
        source_queries.replica_value_query('service', [0]),
        source_queries.job_info_value_query(1),
        source_queries.spot_task_value_query([1]),
        source_queries.cluster_value_query('cluster'),
        source_queries.cluster_history_value_query('hash'),
    )
    for statement in value_statements:
        table = source_queries.selected_table_names(statement)[0]
        assert set(source_queries.selected_column_names(statement)) == (
            exact_value_columns[table])


def test_serve_exact_group_legacy_allocation_and_scalar_placement() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row()},
        history={'cluster-hash': _history_row()})

    result = _scan_serve(reader)

    assert result.findings.to_dict() == {
        'source_rows': 2,
        'selectors_present': 1,
        'selectors_missing': 0,
        'groups_exact': 1,
        'groups_legacy': 0,
        'groups_unknown': 0,
        'allocation_candidates': 1,
        'allocations_exact': 0,
        'allocations_legacy': 1,
        'allocations_unknown': 0,
        'identity_gap': 1,
        'no_cluster_yet': 0,
        'scalar_placement_known': 1,
        'selected_spec_gap': 1,
        'desired_present': 1,
        'desired_absent': 0,
        'desired_unknown': 0,
        'source_conflict': 0,
        'pool_assignment_unfenced': 0,
        'pool_assignment_ambiguous': 0,
    }
    assert result.rows_seen == 4
    group, allocation = result.records
    assert isinstance(group, contracts.GroupEvidenceRecord)
    assert group.confidence is contracts.EvidenceGroupConfidence.EXACT
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    assert allocation.association_status is (
        contracts.EvidenceAssociationStatus.REGISTRY_HASH)
    assert allocation.observed_state is contracts.EvidenceObservedState.UP
    assert allocation.scalar_placement_hash is not None


@pytest.mark.parametrize('service_status', list(serve_statuses.ServiceStatus))
def test_every_service_status_member(
        service_status: serve_statuses.ServiceStatus) -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row(status=service_status.value)})
    result = _scan_serve(reader)
    group = result.records[0]
    assert isinstance(group, contracts.GroupEvidenceRecord)
    if service_status in (serve_statuses.ServiceStatus.SHUTTING_DOWN,
                          serve_statuses.ServiceStatus.FAILED_CLEANUP):
        assert group.lifecycle is contracts.EvidenceLifecycle.RETIRING
        assert group.status_class is contracts.EvidenceStatusClass.ABSENT
    else:
        assert group.lifecycle is contracts.EvidenceLifecycle.ACTIVE
        assert group.status_class is contracts.EvidenceStatusClass.PRESENT
    assert result.findings.source_conflict == 0


@pytest.mark.parametrize('replica_status', list(serve_statuses.ReplicaStatus))
def test_every_replica_status_member(
        replica_status: serve_statuses.ReplicaStatus) -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={
            'svc': [
                _replica_row(status=replica_status.value,
                             cluster_name='svc-cluster')
            ]
        })
    result = _scan_serve(reader)
    allocation = result.records[1]
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    if replica_status in {
            serve_statuses.ReplicaStatus.PENDING,
            serve_statuses.ReplicaStatus.PROVISIONING,
            serve_statuses.ReplicaStatus.STARTING,
            serve_statuses.ReplicaStatus.READY,
            serve_statuses.ReplicaStatus.NOT_READY,
    }:
        expected = contracts.EvidenceDesiredState.PRESENT
    elif replica_status is serve_statuses.ReplicaStatus.UNKNOWN:
        expected = contracts.EvidenceDesiredState.UNKNOWN
    else:
        expected = contracts.EvidenceDesiredState.ABSENT
    assert allocation.desired_state is expected
    assert result.findings.source_conflict == 0


@pytest.mark.parametrize('managed_status',
                         list(managed_job_statuses.ManagedJobStatus))
def test_every_managed_status_member(
        managed_status: managed_job_statuses.ManagedJobStatus) -> None:
    cluster_name = managed_job_naming.generate_managed_job_cluster_name(
        'train', 42)
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info()},
        spot_tasks={(42, 0): [_task(status=managed_status.value)]},
        clusters={
            cluster_name: _cluster_row(name=cluster_name,
                                       workload_type='managed_job',
                                       workload_id='42',
                                       workload_task_id=0)
        })
    result = _scan_job(reader)
    group, allocation = result.records
    assert isinstance(group, contracts.GroupEvidenceRecord)
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    if managed_status in managed_job_statuses.ManagedJobStatus.processing_statuses(
    ):
        assert group.lifecycle is contracts.EvidenceLifecycle.ACTIVE
        assert allocation.desired_state is contracts.EvidenceDesiredState.PRESENT
    elif (managed_status
          in managed_job_statuses.ManagedJobStatus.terminal_statuses() or
          managed_status is managed_job_statuses.ManagedJobStatus.CANCELLING):
        assert group.lifecycle is contracts.EvidenceLifecycle.RETIRING
        assert allocation.desired_state is contracts.EvidenceDesiredState.ABSENT
    else:
        assert group.lifecycle is contracts.EvidenceLifecycle.UNKNOWN
        assert allocation.desired_state is contracts.EvidenceDesiredState.UNKNOWN
    assert result.findings.source_conflict == 0


@pytest.mark.parametrize('cluster_status', list(status_lib.ClusterStatus))
def test_every_registry_cluster_status_member(
        cluster_status: status_lib.ClusterStatus) -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row(status=cluster_status.value)})
    result = _scan_serve(reader)
    allocation = result.records[1]
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    expected = {
        status_lib.ClusterStatus.INIT:
            contracts.EvidenceObservedState.PROVISIONING,
        status_lib.ClusterStatus.UP: contracts.EvidenceObservedState.UP,
        status_lib.ClusterStatus.STOPPED:
            contracts.EvidenceObservedState.STOPPED,
        status_lib.ClusterStatus.AUTOSTOPPING:
            contracts.EvidenceObservedState.PARTIAL,
        status_lib.ClusterStatus.PENDING:
            contracts.EvidenceObservedState.UNKNOWN,
    }[cluster_status]
    assert allocation.observed_state is expected
    assert result.findings.source_conflict == 0


def test_unrecognized_statuses_are_conflicts() -> None:
    service = _scan_serve(
        source_queries.InMemorySourceReader(
            services={'svc': _service_row(status='FUTURE')}))
    assert service.findings.source_conflict == 1

    replica = _scan_serve(
        source_queries.InMemorySourceReader(
            services={'svc': _service_row()},
            replicas={'svc': [_replica_row(status='FUTURE')]}))
    assert replica.findings.source_conflict == 1

    registry = _scan_serve(
        source_queries.InMemorySourceReader(
            services={'svc': _service_row()},
            replicas={'svc': [_replica_row()]},
            clusters={'svc-cluster': _cluster_row(status='FUTURE')}))
    assert registry.findings.source_conflict == 1

    job = _scan_job(
        source_queries.InMemorySourceReader(
            job_info={42: _job_info()},
            spot_tasks={(42, 0): [_task(status='FUTURE')]}))
    # The one unrecognized task status independently conflicts the owner and
    # its allocation candidate.
    assert job.findings.source_conflict == 2


@pytest.mark.parametrize(
    ('replica_status', 'desired'),
    [('READY', contracts.EvidenceDesiredState.PRESENT),
     ('FAILED', contracts.EvidenceDesiredState.ABSENT),
     ('UNKNOWN', contracts.EvidenceDesiredState.UNKNOWN)],
)
def test_serve_replica_status_mapping(
        replica_status: str, desired: contracts.EvidenceDesiredState) -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row(status=replica_status)]},
        clusters={'svc-cluster': _cluster_row()})
    result = _scan_serve(reader)
    allocation = result.records[1]
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    assert allocation.desired_state is desired
    assert result.findings.source_conflict == 0


def test_serve_pending_without_cluster_has_no_candidate() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row(status='PENDING', cluster_name=None)]})
    result = _scan_serve(reader)
    assert len(result.records) == 1
    assert result.findings.no_cluster_yet == 1
    assert result.findings.allocation_candidates == 0


def test_serve_parent_retiring_overrides_child_desire() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row(status='SHUTTING_DOWN')},
        replicas={'svc': [_replica_row(status='READY')]},
        clusters={'svc-cluster': _cluster_row()})
    result = _scan_serve(reader)
    group, allocation = result.records
    assert isinstance(group, contracts.GroupEvidenceRecord)
    assert group.lifecycle is contracts.EvidenceLifecycle.RETIRING
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    assert allocation.desired_state is contracts.EvidenceDesiredState.ABSENT


def test_serve_legacy_and_malformed_group_fences() -> None:
    legacy_reader = source_queries.InMemorySourceReader(
        services={
            'svc': _service_row(service_hash=None,
                                resource_scope=None,
                                lifecycle_epoch=None,
                                controller_pid=None,
                                controller_port=None,
                                controller_ip=None)
        })
    legacy = _scan_serve(legacy_reader)
    legacy_group = legacy.records[0]
    assert isinstance(legacy_group, contracts.GroupEvidenceRecord)
    assert legacy_group.confidence is contracts.EvidenceGroupConfidence.LEGACY

    malformed_reader = source_queries.InMemorySourceReader(services={
        'svc': _service_row(resource_scope='other', controller_port='bad')
    })
    malformed = _scan_serve(malformed_reader)
    malformed_group = malformed.records[0]
    assert isinstance(malformed_group, contracts.GroupEvidenceRecord)
    assert malformed_group.confidence is (
        contracts.EvidenceGroupConfidence.UNKNOWN)
    assert malformed.findings.source_conflict == 1


def test_registry_missing_is_unknown_without_conflict() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()}, replicas={'svc': [_replica_row()]})
    result = _scan_serve(reader)
    allocation = result.records[1]
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    assert allocation.association_status is (
        contracts.EvidenceAssociationStatus.REGISTRY_MISSING)
    assert allocation.identity_confidence is (
        contracts.EvidenceIdentityConfidence.UNKNOWN)
    assert result.findings.source_conflict == 0


def test_registry_unsafe_is_unknown_and_conflicted() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row(is_managed=0)})
    result = _scan_serve(reader)
    allocation = result.records[1]
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    assert allocation.association_status is (
        contracts.EvidenceAssociationStatus.REGISTRY_UNSAFE)
    assert result.findings.source_conflict == 1


def test_registry_cross_workspace_fails_partition() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row(workspace='other')})
    with pytest.raises(source_queries.SourceConflictError,
                       match='another workspace'):
        _scan_serve(reader)


def test_duplicate_cluster_hash_demotes_every_claim_without_history_read(
) -> None:
    replicas = [
        _replica_row(replica_id=0, cluster_name='cluster-a'),
        _replica_row(replica_id=1, cluster_name='cluster-b'),
    ]
    clusters = {
        'cluster-a': _cluster_row(name='cluster-a'),
        'cluster-b': _cluster_row(name='cluster-b'),
    }
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': replicas},
        clusters=clusters,
        history={'cluster-hash': _history_row()})
    result = _scan_serve(reader)
    allocations = result.records[1:]
    assert all(
        isinstance(record, contracts.AllocationCandidateEvidenceRecord) and
        record.association_status is contracts.EvidenceAssociationStatus.
        REGISTRY_UNSAFE and record.scalar_placement_hash is None
        for record in allocations)
    assert result.findings.allocations_unknown == 2
    assert result.findings.source_conflict == 2
    assert result.rows_seen == 5  # service + 2 replicas + 2 registry rows


def test_mixed_serve_children_remain_independent_candidates() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={
            'svc': [
                _replica_row(replica_id=0,
                             status='READY',
                             cluster_name='missing-a'),
                _replica_row(replica_id=1,
                             status='FAILED',
                             cluster_name='missing-b'),
                _replica_row(replica_id=2,
                             status='FUTURE',
                             cluster_name='missing-c'),
            ]
        })
    result = _scan_serve(reader)
    assert len(result.records) == 4
    assert result.findings.allocation_candidates == 3
    assert result.findings.desired_present == 1
    assert result.findings.desired_absent == 1
    assert result.findings.desired_unknown == 1
    assert result.findings.source_conflict == 1


def test_null_registry_attribution_is_safe_but_partial_is_conflicted() -> None:
    safe_reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={
            'svc-cluster': _cluster_row(workload_type=None,
                                        workload_id=None,
                                        workload_task_id=None)
        })
    safe = _scan_serve(safe_reader)
    assert safe.findings.allocations_legacy == 1
    assert safe.findings.source_conflict == 0

    unsafe_reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={
            'svc-cluster': _cluster_row(workload_type='service',
                                        workload_id=None,
                                        workload_task_id=1)
        })
    unsafe = _scan_serve(unsafe_reader)
    assert unsafe.findings.allocations_unknown == 1
    assert unsafe.findings.source_conflict == 1


def test_history_null_attribution_is_safe_and_conflict_suppresses_scalars(
) -> None:
    null_reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row()},
        history={
            'cluster-hash': _history_row(workload_type=None,
                                         workload_id=None,
                                         workload_task_id=None)
        })
    null_attribution = _scan_serve(null_reader)
    assert null_attribution.findings.scalar_placement_known == 1
    assert null_attribution.findings.source_conflict == 0

    conflict_reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row()},
        history={'cluster-hash': _history_row(workload_id='other')})
    conflict = _scan_serve(conflict_reader)
    assert conflict.findings.scalar_placement_known == 0
    assert conflict.findings.source_conflict == 1


@pytest.mark.parametrize(
    'history_overrides',
    [
        {
            'cloud': None,
            'num_nodes': None
        },
        {
            'cloud': 123
        },
        {
            'num_nodes': 0
        },
        {
            'is_managed': 0
        },
        {
            'workspace': None
        },
    ],
)
def test_malformed_history_scalars_are_unknown_without_conflict(
        history_overrides: dict[str, object]) -> None:
    history = _history_row()
    history.update(history_overrides)
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row()},
        history={'cluster-hash': history})
    result = _scan_serve(reader)
    assert result.findings.scalar_placement_known == 0
    assert result.findings.source_conflict == 0


def test_history_cross_workspace_fails_partition() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        replicas={'svc': [_replica_row()]},
        clusters={'svc-cluster': _cluster_row()},
        history={'cluster-hash': _history_row(workspace='other')})
    with pytest.raises(source_queries.SourceConflictError,
                       match='History row belongs'):
        _scan_serve(reader)


def test_managed_exact_running_candidate() -> None:
    cluster_name = managed_job_naming.generate_managed_job_cluster_name(
        'train', 42)
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info()},
        spot_tasks={(42, 0): [_task()]},
        clusters={
            cluster_name: _cluster_row(name=cluster_name,
                                       workload_type='managed_job',
                                       workload_id='42',
                                       workload_task_id=0)
        },
        history={
            'cluster-hash': _history_row(workload_type='managed_job',
                                         workload_id='42',
                                         workload_task_id=0)
        })
    result = _scan_job(reader)
    assert result.findings.groups_exact == 1
    assert result.findings.allocations_legacy == 1
    assert result.findings.desired_present == 1
    assert result.findings.scalar_placement_known == 1
    assert result.rows_seen == 4


def test_managed_pending_missing_registry_has_no_candidate() -> None:
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info()},
        spot_tasks={(42, 0): [_task(status='PENDING')]})
    result = _scan_job(reader)
    assert result.findings.no_cluster_yet == 1
    assert result.findings.allocation_candidates == 0
    assert len(result.records) == 1


def test_managed_pending_malformed_name_is_unknown_candidate() -> None:
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info()},
        spot_tasks={(42, 0): [_task(status='PENDING', task_name=None)]})
    result = _scan_job(reader)
    allocation = result.records[1]
    assert isinstance(allocation, contracts.AllocationCandidateEvidenceRecord)
    assert allocation.association_status is (
        contracts.EvidenceAssociationStatus.SOURCE_MALFORMED)
    assert result.findings.no_cluster_yet == 0
    assert result.findings.source_conflict == 1


def test_managed_null_default_workspace_forces_legacy() -> None:
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info(workspace=None)},
        spot_tasks={(42, 0): [_task(status='PENDING')]})
    result = _scan_job(reader)
    group = result.records[0]
    assert isinstance(group, contracts.GroupEvidenceRecord)
    assert group.confidence is contracts.EvidenceGroupConfidence.LEGACY


def test_managed_stale_nonterminal_controller_is_unknown() -> None:
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info(controller_generation=6)},
        spot_tasks={(42, 0): [_task(status='PENDING')]})
    result = _scan_job(reader)
    group = result.records[0]
    assert isinstance(group, contracts.GroupEvidenceRecord)
    assert group.confidence is contracts.EvidenceGroupConfidence.UNKNOWN
    assert result.findings.source_conflict == 1


def test_duplicate_managed_task_key_fails_partition() -> None:
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info()},
        spot_tasks={(42, 0): [_task(row_id=99),
                              _task(row_id=100)]})
    with pytest.raises(source_queries.SourceConflictError,
                       match='duplicate rows'):
        _scan_job(reader)


def test_pool_assignment_diagnostic_unambiguous_without_job_candidate() -> None:
    job_selector = _job_selector()
    pool_selector = _serve_selector(name='pool-a',
                                    kind=models.ProjectionSourceKind.SERVE_POOL)
    reader = source_queries.InMemorySourceReader(
        job_info={
            42: _job_info(pool='pool-a', current_cluster_name='pool-a-cluster')
        },
        spot_tasks={(42, 0): [_task()]},
        services={'pool-a': _service_row(name='pool-a', pool=1)},
        replicas={
            'pool-a': [
                _replica_row(service_name='pool-a',
                             cluster_name='pool-a-cluster')
            ]
        },
        clusters={
            'pool-a-cluster': _cluster_row(name='pool-a-cluster',
                                           workload_type='pool',
                                           workload_id='pool-a')
        })
    result = _scan_job(reader, job_selector, [job_selector, pool_selector])
    assert result.findings.pool_assignment_unfenced == 1
    assert result.findings.pool_assignment_ambiguous == 0
    assert result.findings.allocation_candidates == 0
    assert len(result.records) == 1
    assert result.rows_seen == 5


@pytest.mark.parametrize(('matching_replicas', 'ambiguous'), [(0, 1), (1, 0),
                                                              (2, 1)])
def test_pool_assignment_requires_exactly_one_linked_replica(
        matching_replicas: int, ambiguous: int) -> None:
    job_selector = _job_selector()
    pool_selector = _serve_selector(name='pool-a',
                                    kind=models.ProjectionSourceKind.SERVE_POOL)
    replicas = [
        _replica_row(service_name='pool-a',
                     replica_id=index,
                     cluster_name=('pool-a-cluster' if index < matching_replicas
                                   else f'other-{index}'))
        for index in range(max(1, matching_replicas))
    ]
    reader = source_queries.InMemorySourceReader(
        job_info={
            42: _job_info(pool='pool-a', current_cluster_name='pool-a-cluster')
        },
        spot_tasks={(42, 0): [_task()]},
        services={'pool-a': _service_row(name='pool-a', pool=1)},
        replicas={'pool-a': replicas},
        clusters={
            'pool-a-cluster': _cluster_row(name='pool-a-cluster',
                                           workload_type='pool',
                                           workload_id='pool-a')
        })
    result = _scan_job(reader, job_selector, [job_selector, pool_selector])
    assert result.findings.pool_assignment_unfenced == 1
    assert result.findings.pool_assignment_ambiguous == ambiguous


def test_pool_assignment_unsafe_registry_attribution_is_ambiguous() -> None:
    job_selector = _job_selector()
    pool_selector = _serve_selector(name='pool-a',
                                    kind=models.ProjectionSourceKind.SERVE_POOL)
    reader = source_queries.InMemorySourceReader(
        job_info={
            42: _job_info(pool='pool-a', current_cluster_name='pool-a-cluster')
        },
        spot_tasks={(42, 0): [_task()]},
        services={'pool-a': _service_row(name='pool-a', pool=1)},
        replicas={
            'pool-a': [
                _replica_row(service_name='pool-a',
                             cluster_name='pool-a-cluster')
            ]
        },
        clusters={
            'pool-a-cluster': _cluster_row(name='pool-a-cluster',
                                           workload_type='pool',
                                           workload_id='different')
        })
    result = _scan_job(reader, job_selector, [job_selector, pool_selector])
    assert result.findings.pool_assignment_unfenced == 1
    assert result.findings.pool_assignment_ambiguous == 1


def test_pool_assignment_null_link_is_excluded() -> None:
    reader = source_queries.InMemorySourceReader(
        job_info={42: _job_info(pool='pool-a', current_cluster_name=None)},
        spot_tasks={(42, 0): [_task()]})
    result = _scan_job(reader)
    assert result.findings.pool_assignment_unfenced == 0
    assert result.findings.pool_assignment_ambiguous == 0


@pytest.mark.parametrize('is_batch', [False, True])
def test_pool_assignment_missing_selector_is_ambiguous_unless_batch(
        is_batch: bool) -> None:
    reader = source_queries.InMemorySourceReader(job_info={
        42: _job_info(pool='pool-a',
                      current_cluster_name='pool-a-cluster',
                      is_batch=is_batch)
    },
                                                 spot_tasks={
                                                     (42, 0): [_task()]
                                                 })
    result = _scan_job(reader)
    expected = int(not is_batch)
    assert result.findings.pool_assignment_unfenced == expected
    assert result.findings.pool_assignment_ambiguous == expected


def test_source_reader_negative_cache_and_row_accounting() -> None:
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()}, replicas={'svc': [_replica_row()]})
    assert reader.service('svc') is not None
    assert reader.service('svc') is not None
    assert len(reader.replicas('svc')) == 1
    assert len(reader.replicas('svc')) == 1
    assert reader.cluster('missing') is None
    assert reader.cluster('missing') is None
    assert reader.source_rows == 2
    assert reader.rows_seen == 2


class _FakeMappingResult:
    """Minimal streaming mappings result for bounded-read tests."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> '_FakeMappingResult':
        return self

    def partitions(self, size: int):
        for offset in range(0, len(self._rows), size):
            yield self._rows[offset:offset + size]


class _FakeConnection:
    """Deterministic connection seam that records source statements."""

    def __init__(self,
                 responses: list[list[dict[str, object]]],
                 on_execute=None) -> None:
        self._responses = list(responses)
        self._on_execute = on_execute
        self.execute_count = 0
        self.statements = []

    def execution_options(self, **kwargs) -> '_FakeConnection':
        del kwargs
        return self

    def execute(self, statement) -> _FakeMappingResult:
        self.execute_count += 1
        self.statements.append(statement)
        if self._on_execute is not None:
            self._on_execute()
        if not self._responses:
            raise AssertionError('Unexpected source query.')
        return _FakeMappingResult(self._responses.pop(0))


@pytest.mark.parametrize(('count', 'fails'), [(9_999, False), (10_000, False),
                                              (10_001, True)])
def test_partition_cache_exact_source_row_bound(count: int,
                                                fails: bool) -> None:
    cache = source_queries.PartitionSourceCache(_FakeConnection([]))
    rows = [{}] * count
    if fails:
        with pytest.raises(source_queries.RowLimitExceededError):
            cache._charge_rows(rows, source=True)  # pylint: disable=protected-access
        assert cache.source_rows == 0
    else:
        cache._charge_rows(rows, source=True)  # pylint: disable=protected-access
        assert cache.source_rows == count


def test_point_read_rejects_exhausted_row_budget_before_value_fetch() -> None:
    connection = _FakeConnection([[{'name': 'svc'}]])
    cache = source_queries.PartitionSourceCache(
        connection, source_queries.SourceReadLimits(max_source_rows=1))
    cache._charge_rows([{}], source=True)  # pylint: disable=protected-access

    with pytest.raises(source_queries.RowLimitExceededError):
        cache.service('svc')

    assert connection.execute_count == 1


def test_registry_point_read_rejects_total_budget_before_value_fetch() -> None:
    connection = _FakeConnection([[{'name': 'cluster'}]])
    cache = source_queries.PartitionSourceCache(connection)
    cache._charge_rows(  # pylint: disable=protected-access
        [{}] * 30_000, source=False)

    with pytest.raises(source_queries.RowLimitExceededError):
        cache.cluster('cluster')

    assert connection.execute_count == 1


def test_spot_probe_uses_remaining_plus_one_before_value_fetch() -> None:
    connection = _FakeConnection([[{
        'job_id': 1,
        'spot_job_id': 42,
        'task_id': 0,
    }]])
    cache = source_queries.PartitionSourceCache(connection)
    cache._charge_rows(  # pylint: disable=protected-access
        [{}] * 10_000, source=True)

    with pytest.raises(source_queries.RowLimitExceededError):
        cache.spot_tasks(42, 0)

    assert connection.execute_count == 1
    assert connection.statements[0]._limit_clause.value == 1  # pylint: disable=protected-access


def test_partition_cache_rejects_30001st_total_row() -> None:
    cache = source_queries.PartitionSourceCache(_FakeConnection([]))
    cache._charge_rows(  # pylint: disable=protected-access
        [{}] * 30_000, source=False)
    with pytest.raises(source_queries.RowLimitExceededError):
        cache._charge_rows([{}], source=False)  # pylint: disable=protected-access
    assert cache.rows_seen == 30_000


@pytest.mark.parametrize(('value_bytes', 'fails'), [((1 << 20), False),
                                                    ((1 << 20) + 1, True)])
def test_partition_cache_exact_variable_value_bound(value_bytes: int,
                                                    fails: bool) -> None:
    cache = source_queries.PartitionSourceCache(_FakeConnection([]))
    probe = [{'value_bytes': value_bytes}]
    if fails:
        with pytest.raises(source_queries.ByteLimitExceededError):
            cache._preflight(probe)  # pylint: disable=protected-access
    else:
        cache._preflight(probe)  # pylint: disable=protected-access


def test_partition_cache_exact_four_mib_fetch_batch_bound() -> None:
    cache = source_queries.PartitionSourceCache(_FakeConnection([]))
    exact = [{'value_bytes': (1 << 20) - 8}] * 4
    assert cache._preflight(exact) == 4 << 20  # pylint: disable=protected-access
    over = exact[:-1] + [{'value_bytes': (1 << 20) - 7}]
    with pytest.raises(source_queries.ByteLimitExceededError,
                       match='fetch batch'):
        cache._preflight(over)  # pylint: disable=protected-access


def test_replica_prefix_charges_aggregate_probes_with_canonical_bytes() -> None:
    probes = [{
        'service_name': 'svc',
        'replica_id': replica_id,
        'status_bytes': 10,
        'version': 1,
        'cluster_name_bytes': 10,
    } for replica_id in range(2)]
    connection = _FakeConnection([probes])
    cache = source_queries.PartitionSourceCache(
        connection,
        source_queries.SourceReadLimits(max_retained_bytes=250,
                                        fetch_batch_rows=1))
    cache.retain_canonical_bytes(b'x' * 70)
    with pytest.raises(source_queries.ByteLimitExceededError, match='overlap'):
        cache.replicas('svc')
    assert connection.execute_count == 1


def test_partition_cache_negative_lookup_is_charged_and_queried_once() -> None:
    connection = _FakeConnection([[]])
    cache = source_queries.PartitionSourceCache(connection)
    assert cache.service('missing') is None
    assert cache.service('missing') is None
    assert connection.execute_count == 1
    assert cache.source_rows == 0
    assert cache.rows_seen == 0


def test_partition_cache_checks_deadline_before_and_after_execute() -> None:
    before_now = [1.0]
    before_connection = _FakeConnection([[]])
    before = source_queries.PartitionSourceCache(before_connection,
                                                 deadline_monotonic=1.0,
                                                 clock=lambda: before_now[0])
    with pytest.raises(source_queries.SourceReadDeadlineExceededError):
        before.service('svc')
    assert before_connection.execute_count == 0

    after_now = [0.0]
    after_connection = _FakeConnection(
        [[]], on_execute=lambda: after_now.__setitem__(0, 1.0))
    after = source_queries.PartitionSourceCache(after_connection,
                                                deadline_monotonic=1.0,
                                                clock=lambda: after_now[0])
    with pytest.raises(source_queries.SourceReadDeadlineExceededError):
        after.service('svc')
    assert after_connection.execute_count == 1


def test_combined_canonical_retention_bound_is_enforced() -> None:
    reader = source_queries.InMemorySourceReader(
        limits=source_queries.SourceReadLimits(max_retained_bytes=3))
    reader.retain_canonical_bytes(b'abc')
    with pytest.raises(source_queries.ByteLimitExceededError, match='Combined'):
        reader.retain_canonical_bytes(b'd')


def test_adapter_checks_absolute_deadline() -> None:
    now = [10.0]
    reader = source_queries.InMemorySourceReader(
        services={'svc': _service_row()},
        deadline_monotonic=10.0,
        clock=lambda: now[0])
    with pytest.raises(source_queries.SourceReadDeadlineExceededError):
        _scan_serve(reader)


def test_missing_selector_closes_without_records() -> None:
    result = _scan_serve(source_queries.InMemorySourceReader())
    assert not result.records
    assert dataclasses.asdict(result.findings)['selectors_missing'] == 1
    assert result.findings.source_rows == 0
