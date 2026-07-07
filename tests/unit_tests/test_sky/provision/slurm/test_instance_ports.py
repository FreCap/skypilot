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


class TestTerminateEscalation(unittest.TestCase):
    """terminate_instances must enforce termination if TERM is survived."""

    def _run_terminate(self, states_after_term, cancel_side_effect=None):
        """Run terminate_instances against a fake client.

        states_after_term: sequence of job-state lists returned by
        successive get_jobs_state_by_name calls after the initial
        RUNNING answer.
        """
        client = mock.Mock()
        client.get_jobs_state_by_name.side_effect = ([['RUNNING']] +
                                                     list(states_after_term))
        if cancel_side_effect is not None:
            client.cancel_jobs_by_name.side_effect = cancel_side_effect
        with mock.patch.object(slurm_instance.slurm,
                               'SlurmClient',
                               return_value=client), \
             mock.patch.object(slurm_instance.slurm_utils,
                               'is_inside_slurm_cluster',
                               return_value=False), \
             mock.patch.object(slurm_instance.time, 'sleep'):
            slurm_instance.terminate_instances('cluster-abc',
                                               provider_config={
                                                   'ssh': {
                                                       'hostname': 'login',
                                                       'port': 22,
                                                       'user': 'u',
                                                   }
                                               })
        return client

    def test_graceful_exit_needs_no_escalation(self):
        client = self._run_terminate([[]])
        client.cancel_jobs_by_name.assert_called_once_with('cluster-abc',
                                                           signal='TERM',
                                                           full=True)

    def test_completing_after_term_needs_no_escalation(self):
        client = self._run_terminate([['COMPLETING']])
        client.cancel_jobs_by_name.assert_called_once_with('cluster-abc',
                                                           signal='TERM',
                                                           full=True)

    def test_surviving_job_gets_enforced_scancel(self):
        polls = slurm_instance._TERMINATION_GRACE_POLLS
        client = self._run_terminate([['RUNNING']] * polls)
        self.assertEqual(client.cancel_jobs_by_name.call_args_list, [
            mock.call('cluster-abc', signal='TERM', full=True),
            mock.call('cluster-abc', signal=None),
        ])

    def test_enforced_scancel_racing_job_exit_is_tolerated(self):
        # Job stays RUNNING through all polls, then exits right before the
        # enforced scancel: the scancel failure must not propagate because
        # a final state check shows the job is gone.
        polls = slurm_instance._TERMINATION_GRACE_POLLS
        client = self._run_terminate(
            [['RUNNING']] * polls + [[]],
            cancel_side_effect=[None, RuntimeError('no matching job')])
        self.assertEqual(client.cancel_jobs_by_name.call_count, 2)

    def test_enforced_scancel_failure_with_live_job_propagates(self):
        polls = slurm_instance._TERMINATION_GRACE_POLLS
        with self.assertRaises(RuntimeError):
            self._run_terminate(
                [['RUNNING']] * (polls + 1),
                cancel_side_effect=[None, RuntimeError('scancel failed')])


if __name__ == '__main__':
    unittest.main()
