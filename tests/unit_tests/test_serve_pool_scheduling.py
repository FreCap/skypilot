"""Tests for pooled SkyServe scheduling query batching."""

import contextlib
from unittest import mock

import pytest

from sky.resources import Resources
from sky.serve import serve_state
from sky.serve import serve_utils


def _mock_pool_replica(replica_id: int,
                       cluster_name: str,
                       *,
                       launched_resources=None,
                       status=serve_state.ReplicaStatus.READY):
    replica = mock.Mock()
    replica.replica_id = replica_id
    replica.cluster_name = cluster_name
    replica.status = status
    handle = None
    if launched_resources is not None:
        handle = mock.Mock()
        handle.launched_resources = launched_resources
    replica.handle.return_value = handle
    return replica


def test_get_free_worker_resources_uses_grouped_pool_resource_lookup():
    replica_a = _mock_pool_replica(1,
                                   'replica-a',
                                   launched_resources=Resources(cpus='8'))
    replica_b = _mock_pool_replica(2,
                                   'replica-b',
                                   launched_resources=Resources(cpus='8'))
    with mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[replica_a, replica_b]), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_clusters_from_names',
             side_effect=lambda names:
             {name: {'handle': object()} for name in names}) as get_clusters, \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_handle_from_cluster_name',
             side_effect=AssertionError(
                 'per-worker cluster-record read used')), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources_by_cluster',
             return_value={
                 'replica-a': Resources(cpus='2'),
                 'replica-b': Resources(),
             }) as grouped_usage, \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_nonterminal_job_ids_by_pool',
             side_effect=AssertionError('legacy per-replica scan used')), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources',
             side_effect=AssertionError('legacy per-replica usage used')):
        free_resources = serve_utils.get_free_worker_resources('pool-a')

    grouped_usage.assert_called_once_with('pool-a')
    get_clusters.assert_called_once_with(['replica-a', 'replica-b'])
    assert free_resources is not None
    assert float(free_resources['replica-a'].cpus) == pytest.approx(6.0)
    assert free_resources['replica-b'].is_empty()


def test_get_free_worker_resources_reuses_provided_cluster_snapshot():
    replica = _mock_pool_replica(1,
                                 'replica-a',
                                 launched_resources=Resources(cpus='8'))
    cluster_records = {'replica-a': {'handle': object()}}
    with mock.patch.object(
            serve_utils.global_user_state,
            'get_clusters_from_names',
            side_effect=AssertionError(
                'duplicate batched cluster read used')), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources_by_cluster',
             return_value={'replica-a': Resources(cpus='2')}):
        free_resources = serve_utils.get_free_worker_resources(
            'pool-a', replicas=[replica], cluster_records=cluster_records)

    assert free_resources is not None
    assert float(free_resources['replica-a'].cpus) == pytest.approx(6.0)


def test_get_next_cluster_name_uses_grouped_pool_counts_in_fallback():
    busy = _mock_pool_replica(1, 'replica-busy')
    idle = _mock_pool_replica(2, 'replica-idle')
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }) as get_status, \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_utils,
                           'get_free_worker_resources',
                           return_value={
                               'replica-busy': None,
                               'replica-idle': None,
                           }), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[busy, idle]), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_nonterminal_job_counts_by_pool',
             return_value={
                 'replica-busy': 1,
             }) as grouped_counts, \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_nonterminal_job_ids_by_pool',
             side_effect=AssertionError('legacy fallback scan used')), \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra') as set_infra:
        selected = serve_utils.get_next_cluster_name('pool-a',
                                                     job_id=17,
                                                     task_resources=None)

    get_status.assert_called_once_with('pool-a',
                                       pool=True,
                                       with_replica_info=False,
                                       with_yaml=False,
                                       status_snapshot_only=True)
    grouped_counts.assert_called_once_with('pool-a')
    assert selected == 'replica-idle'
    set_cluster.assert_called_once_with(17, 'replica-idle')
    set_infra.assert_not_called()


@pytest.mark.parametrize('task_resources', [None, Resources()])
def test_get_next_cluster_name_skips_resource_scan_without_constraints(
        task_resources):
    busy = _mock_pool_replica(1, 'replica-busy')
    idle = _mock_pool_replica(2, 'replica-idle')
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }), \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[busy, idle]), \
         mock.patch.object(
             serve_utils,
             'get_free_worker_resources',
             side_effect=AssertionError('resource scan should be skipped')
         ), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_nonterminal_job_counts_by_pool',
             return_value={
                 'replica-busy': 1,
             }) as grouped_counts, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra') as set_infra:
        selected = serve_utils.get_next_cluster_name(
            'pool-a', job_id=17, task_resources=task_resources)

    grouped_counts.assert_called_once_with('pool-a')
    assert selected == 'replica-idle'
    set_cluster.assert_called_once_with(17, 'replica-idle')
    set_infra.assert_not_called()


def test_get_next_cluster_name_uses_resource_scan_for_constrained_task():
    constrained = _mock_pool_replica(1, 'replica-constrained')
    roomy = _mock_pool_replica(2, 'replica-roomy')
    cluster_records = {
        'replica-constrained': None,
        'replica-roomy': None,
    }
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }), \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[constrained, roomy]), \
         mock.patch.object(serve_utils.global_user_state,
                           'get_clusters_from_names',
                           return_value=cluster_records), \
         mock.patch.object(
             serve_utils,
             'get_free_worker_resources',
             return_value={
                 'replica-constrained': Resources(cpus='1'),
                 'replica-roomy': Resources(cpus='4'),
             }) as free_resources, \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_nonterminal_job_counts_by_pool',
             side_effect=AssertionError('fallback counts should not run')
         ), \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra') as set_infra:
        selected = serve_utils.get_next_cluster_name(
            'pool-a', job_id=23, task_resources=Resources(cpus='2'))

    free_resources.assert_called_once_with('pool-a',
                                           replicas=[constrained, roomy],
                                           cluster_records=cluster_records)
    assert selected == 'replica-roomy'
    set_cluster.assert_called_once_with(23, 'replica-roomy')
    set_infra.assert_not_called()


def test_get_next_cluster_name_reuses_resource_scan_cluster_snapshot_for_infra(
):
    worker = _mock_pool_replica(1,
                                'replica-a',
                                launched_resources=Resources(cpus='8'))
    cluster_record = {'handle': worker.handle.return_value}
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }), \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[worker]), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_clusters_from_names',
             return_value={
                 'replica-a': cluster_record
             }) as get_clusters, \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_handle_from_cluster_name',
             side_effect=AssertionError(
                 'post-selection handle reread used')), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources_by_cluster',
             return_value={}), \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra') as set_infra:
        selected = serve_utils.get_next_cluster_name(
            'pool-a', job_id=29, task_resources=Resources(cpus='2'))

    assert selected == 'replica-a'
    get_clusters.assert_called_once_with(['replica-a'])
    assert worker.handle.call_args_list == [
        mock.call(cluster_record),
        mock.call(cluster_record),
    ]
    set_cluster.assert_called_once_with(29, 'replica-a')
    set_infra.assert_called_once_with(29, cloud=None, region=None, zone=None)


def test_get_next_cluster_name_does_not_reread_missing_snapshot_for_infra():
    worker = _mock_pool_replica(1,
                                'replica-a',
                                launched_resources=Resources(cpus='8'))
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }), \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[worker]), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_clusters_from_names',
             return_value={
                 'replica-a': None
             }) as get_clusters, \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources_by_cluster',
             return_value={}), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_nonterminal_job_counts_by_pool',
             return_value={}), \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra') as set_infra:
        selected = serve_utils.get_next_cluster_name(
            'pool-a', job_id=30, task_resources=Resources(cpus='2'))

    assert selected == 'replica-a'
    get_clusters.assert_called_once_with(['replica-a'])
    worker.handle.assert_not_called()
    set_cluster.assert_called_once_with(30, 'replica-a')
    set_infra.assert_not_called()


def test_get_next_cluster_name_reads_replica_snapshot_once():
    """The scheduling decision must use one consistent replica snapshot."""
    worker = _mock_pool_replica(1,
                                'replica-a',
                                launched_resources=Resources(cpus='8'))
    not_ready = _mock_pool_replica(
        2,
        'replica-b',
        launched_resources=Resources(cpus='8'),
        status=serve_state.ReplicaStatus.PROVISIONING)
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }), \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[worker, not_ready]) as replica_reads, \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources_by_cluster',
             return_value={}), \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra'):
        selected = serve_utils.get_next_cluster_name(
            'pool-a', job_id=31, task_resources=Resources(cpus='2'))

    # Readiness filtering and free-resource accounting share one DB read.
    replica_reads.assert_called_once_with('pool-a')
    assert selected == 'replica-a'
    set_cluster.assert_called_once_with(31, 'replica-a')


def test_get_next_cluster_name_persists_chosen_heterogeneous_resource():
    """The persisted full_resources must be the option that fit."""
    worker = _mock_pool_replica(1, 'replica-a')
    big = Resources(cpus='8')
    small = Resources(cpus='2')
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={
                               'pool': True,
                           }), \
         mock.patch.object(serve_utils,
                           'get_service_filelock_path',
                           return_value='/tmp/pool.lock'), \
         mock.patch.object(serve_utils.filelock,
                           'FileLock',
                           side_effect=lambda _path: contextlib.
                           nullcontext()), \
         mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[worker]), \
         mock.patch.object(serve_utils,
                           'get_free_worker_resources',
                           return_value={
                               'replica-a': Resources(cpus='4'),
                           }), \
         mock.patch.object(serve_utils,
                           '_task_fits',
                           wraps=serve_utils._task_fits  # pylint: disable=protected-access
                          ) as task_fits, \
         mock.patch.object(serve_utils.managed_job_state,
                           'update_job_full_resources') as update_full, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_current_cluster_name') as set_cluster, \
         mock.patch.object(serve_utils.managed_job_state,
                           'set_job_infra'):
        selected = serve_utils.get_next_cluster_name(
            'pool-a', job_id=41, task_resources=[big, small])

    assert selected == 'replica-a'
    set_cluster.assert_called_once_with(41, 'replica-a')
    update_full.assert_called_once_with(41, small.to_yaml_config())
    # The fit decision is computed once per option during candidate
    # enumeration and not recomputed for the selected worker.
    assert task_fits.call_count == 2


def test_get_free_worker_resources_skips_worker_missing_cluster_record():
    """A worker whose cluster record is absent from the batched snapshot
    (terminated between snapshot and walk) maps to None without a fallback
    per-name cluster read."""
    replica_a = _mock_pool_replica(1,
                                   'replica-a',
                                   launched_resources=Resources(cpus='8'))
    replica_gone = _mock_pool_replica(2, 'replica-gone')
    with mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[replica_a, replica_gone]), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_clusters_from_names',
             return_value={
                 'replica-a': {'handle': object()},
                 'replica-gone': None,
             }), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_handle_from_cluster_name',
             side_effect=AssertionError(
                 'missing record must not trigger a per-name read')), \
         mock.patch.object(
             serve_utils.managed_job_state,
             'get_pool_worker_used_resources_by_cluster',
             return_value={'replica-a': Resources(cpus='2')}):
        free_resources = serve_utils.get_free_worker_resources('pool-a')

    assert free_resources is not None
    assert float(free_resources['replica-a'].cpus) == pytest.approx(6.0)
    assert free_resources['replica-gone'] is None
    replica_gone.handle.assert_not_called()
