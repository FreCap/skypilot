"""Tests for the cloud provision interface."""

from unittest import mock

import pytest

from sky import provision
from sky.utils import command_runner


def test_get_command_runners_preserves_matching_connection_metadata():
    cluster_info = mock.Mock()
    cluster_info.get_feasible_ips.return_value = ['10.0.0.1', '10.0.0.2']
    cluster_info.get_ssh_ports.return_value = [22, 2222]
    runners = [mock.Mock(), mock.Mock()]

    with mock.patch.object(command_runner.SSHCommandRunner,
                           'make_runner_list',
                           return_value=runners) as make_runners:
        result = provision.get_command_runners('aws',
                                               cluster_info,
                                               ssh_user='sky')

    assert result == runners
    make_runners.assert_called_once_with(node_list=[('10.0.0.1', 22),
                                                    ('10.0.0.2', 2222)],
                                         ssh_user='sky')


def test_get_command_runners_rejects_mismatched_connection_metadata():
    cluster_info = mock.Mock()
    cluster_info.get_feasible_ips.return_value = ['10.0.0.1', '10.0.0.2']
    cluster_info.get_ssh_ports.return_value = [22]

    with mock.patch.object(command_runner.SSHCommandRunner,
                           'make_runner_list') as make_runners:
        with pytest.raises(ValueError, match='mismatched IP and SSH port'):
            provision.get_command_runners('aws', cluster_info)

    make_runners.assert_not_called()
