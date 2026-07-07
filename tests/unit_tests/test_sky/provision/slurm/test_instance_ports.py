"""Tests for Slurm provisioner port endpoint resolution and termination."""
import unittest
from unittest import mock

from sky.provision import common
from sky.provision.slurm import instance as slurm_instance


def _cluster_info(head_internal_ip: str) -> common.ClusterInfo:
    info = common.InstanceInfo(
        instance_id='job-1-node-1',
        internal_ip=head_internal_ip,
        external_ip='login.example.com',
        ssh_port=22,
        tags={},
    )
    return common.ClusterInfo(
        instances={'job-1-node-1': [info]},
        head_instance_id='job-1-node-1',
        provider_name='slurm',
        provider_config={},
    )


_EMPTY_CLUSTER_INFO = common.ClusterInfo(instances={},
                                         head_instance_id=None,
                                         provider_name='slurm',
                                         provider_config={})


class TestQueryPorts(unittest.TestCase):

    def setUp(self):
        slurm_instance._query_ports_cache.clear()

    def test_resolves_to_compute_node_internal_ip(self):
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               return_value=_cluster_info('10.0.0.42')):
            endpoints = slurm_instance.query_ports('cluster-abc', ['8080'],
                                                   head_ip='login.example.com',
                                                   provider_config={'ssh': {}})
        self.assertEqual(list(endpoints.keys()), [8080])
        self.assertEqual(endpoints[8080][0].url(), '10.0.0.42:8080')

    def test_no_running_allocation_returns_empty(self):
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               return_value=_EMPTY_CLUSTER_INFO):
            endpoints = slurm_instance.query_ports('cluster-abc', ['8080'],
                                                   head_ip='login.example.com',
                                                   provider_config={'ssh': {}})
        self.assertEqual(endpoints, {})

    def test_lookup_failure_degrades_to_empty(self):
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               side_effect=RuntimeError('ssh failed')):
            endpoints = slurm_instance.query_ports('cluster-abc', ['8080'],
                                                   head_ip='login.example.com',
                                                   provider_config={'ssh': {}})
        self.assertEqual(endpoints, {})

    def test_resolution_is_cached(self):
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               return_value=_cluster_info('10.0.0.42')) as m:
            first = slurm_instance.query_ports('cluster-abc', ['8080'],
                                               provider_config={'ssh': {}})
            second = slurm_instance.query_ports('cluster-abc', ['8080'],
                                                provider_config={'ssh': {}})
        self.assertEqual(m.call_count, 1)
        self.assertEqual(first[8080][0].url(), second[8080][0].url())

    def test_empty_result_is_not_cached(self):
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               side_effect=[
                                   _EMPTY_CLUSTER_INFO,
                                   _cluster_info('10.0.0.42'),
                               ]) as m:
            first = slurm_instance.query_ports('cluster-abc', ['8080'],
                                               provider_config={'ssh': {}})
            second = slurm_instance.query_ports('cluster-abc', ['8080'],
                                                provider_config={'ssh': {}})
        self.assertEqual(m.call_count, 2)
        self.assertEqual(first, {})
        self.assertEqual(second[8080][0].url(), '10.0.0.42:8080')


class TestTerminateDurability(unittest.TestCase):
    """terminate_instances must be crash-durable and idempotent."""

    def _run_terminate(self,
                       initial_state,
                       later_states=(),
                       terminate_side_effect=None):
        client = mock.Mock()
        client.get_jobs_state_by_name.side_effect = ([initial_state] +
                                                     list(later_states))
        if terminate_side_effect is not None:
            client.terminate_jobs_by_name.side_effect = terminate_side_effect
        with mock.patch.object(slurm_instance.slurm,
                               'SlurmClient',
                               return_value=client), \
             mock.patch.object(slurm_instance.slurm_utils,
                               'is_inside_slurm_cluster',
                               return_value=False):
            slurm_instance.terminate_instances('cluster-abc',
                                               provider_config={
                                                   'ssh': {
                                                       'hostname': 'login',
                                                       'port': 22,
                                                       'user': 'u',
                                                   }
                                               })
        return client

    def test_running_job_is_terminated_in_one_call(self):
        client = self._run_terminate(['RUNNING'])
        client.terminate_jobs_by_name.assert_called_once_with('cluster-abc')
        client.cancel_jobs_by_name.assert_not_called()

    def test_completing_job_is_still_enforced(self):
        client = self._run_terminate(['COMPLETING'])
        client.terminate_jobs_by_name.assert_called_once_with('cluster-abc')

    def test_pending_job_cancelled_without_signal(self):
        client = self._run_terminate(['PENDING'])
        client.cancel_jobs_by_name.assert_called_once_with('cluster-abc',
                                                           signal=None)
        client.terminate_jobs_by_name.assert_not_called()

    def test_terminal_state_needs_no_action(self):
        client = self._run_terminate(['COMPLETED'])
        client.terminate_jobs_by_name.assert_not_called()
        client.cancel_jobs_by_name.assert_not_called()

    def test_termination_racing_job_exit_is_tolerated(self):
        client = self._run_terminate(
            ['RUNNING'],
            later_states=[[]],
            terminate_side_effect=RuntimeError('no matching job'))
        client.terminate_jobs_by_name.assert_called_once_with('cluster-abc')

    def test_termination_failure_with_live_job_propagates(self):
        with self.assertRaises(RuntimeError):
            self._run_terminate(
                ['RUNNING'],
                later_states=[['RUNNING']],
                terminate_side_effect=RuntimeError('scancel failed'))

    def test_terminate_invalidates_query_ports_cache(self):
        slurm_instance._query_ports_cache['cluster-abc'] = (0.0, '10.0.0.42')
        self._run_terminate(['RUNNING'])
        self.assertNotIn('cluster-abc', slurm_instance._query_ports_cache)


if __name__ == '__main__':
    unittest.main()
