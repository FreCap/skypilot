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


if __name__ == '__main__':
    unittest.main()
