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
                       launched_resources=None):
    replica = mock.Mock()
    replica.replica_id = replica_id
    replica.cluster_name = cluster_name
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
    assert free_resources is not None
    assert float(free_resources['replica-a'].cpus) == pytest.approx(6.0)
    assert free_resources['replica-b'].is_empty()


def test_get_next_cluster_name_uses_grouped_pool_counts_in_fallback():
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
         mock.patch.object(serve_utils,
                           'get_free_worker_resources',
                           return_value={
                               'replica-busy': None,
                               'replica-idle': None,
                           }), \
         mock.patch.object(serve_utils,
                           'get_ready_replicas',
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

    grouped_counts.assert_called_once_with('pool-a')
    assert selected == 'replica-idle'
    set_cluster.assert_called_once_with(17, 'replica-idle')
    set_infra.assert_not_called()
