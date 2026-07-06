"""Tests for Slurm provisioner port endpoint resolution."""
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


class TestQueryPorts(unittest.TestCase):

    def test_resolves_to_compute_node_internal_ip(self):
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               return_value=_cluster_info('10.0.0.42')):
            endpoints = slurm_instance.query_ports(
                'cluster-abc', ['8080'],
                head_ip='login.example.com',
                provider_config={'ssh': {}})
        self.assertEqual(list(endpoints.keys()), [8080])
        self.assertEqual(endpoints[8080][0].url(), '10.0.0.42:8080')

    def test_no_running_allocation_returns_empty(self):
        empty = common.ClusterInfo(instances={},
                                   head_instance_id=None,
                                   provider_name='slurm',
                                   provider_config={})
        with mock.patch.object(slurm_instance,
                               'get_cluster_info',
                               return_value=empty):
            endpoints = slurm_instance.query_ports(
                'cluster-abc', ['8080'],
                head_ip='login.example.com',
                provider_config={'ssh': {}})
        self.assertEqual(endpoints, {})


class TestTerminateEscalation(unittest.TestCase):
    """terminate_instances must enforce termination if TERM is survived."""

    def _run_terminate(self, states_after_term):
        """Run terminate_instances against a fake client.

        states_after_term: sequence of job-state lists returned by
        successive get_jobs_state_by_name calls after the initial
        RUNNING answer.
        """
        client = mock.Mock()
        client.get_jobs_state_by_name.side_effect = (
            [['RUNNING']] + list(states_after_term))
        with mock.patch.object(slurm_instance.slurm,
                               'SlurmClient',
                               return_value=client), \
             mock.patch.object(slurm_instance.slurm_utils,
                               'is_inside_slurm_cluster',
                               return_value=False), \
             mock.patch.object(slurm_instance.time, 'sleep'):
            slurm_instance.terminate_instances(
                'cluster-abc',
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

    def test_surviving_job_gets_enforced_scancel(self):
        polls = slurm_instance._TERMINATION_GRACE_POLLS
        client = self._run_terminate([['RUNNING']] * polls)
        self.assertEqual(client.cancel_jobs_by_name.call_args_list, [
            mock.call('cluster-abc', signal='TERM', full=True),
            mock.call('cluster-abc', signal=None),
        ])


if __name__ == '__main__':
    unittest.main()
