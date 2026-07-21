"""Tests for JobGroup networking lifecycle behavior."""

import asyncio
from unittest import mock

import pytest

from sky.jobs import job_group_networking


async def _run_node_setup(inject_hosts: mock.AsyncMock) -> bool:
    task = mock.MagicMock()
    task.name = 'worker'
    runner = mock.MagicMock()
    handle = mock.MagicMock()
    handle.get_command_runners.return_value = [runner]

    with mock.patch.object(job_group_networking,
                           '_generate_hosts_entries',
                           return_value='hosts'), mock.patch.object(
                               job_group_networking,
                               '_generate_k8s_dns_mappings',
                               return_value=[]), mock.patch.object(
                                   job_group_networking,
                                   '_is_kubernetes',
                                   return_value=False), mock.patch.object(
                                       job_group_networking,
                                       '_inject_hosts_on_node', inject_hosts):
        result = await job_group_networking.NetworkConfigurator._inject_etc_hosts(  # pylint: disable=protected-access
            'group', [(task, handle)])

    inject_hosts.assert_awaited_once_with(runner, 'hosts', 'group')
    return result


@pytest.mark.asyncio
async def test_node_setup_propagates_cancellation():
    inject_hosts = mock.AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await _run_node_setup(inject_hosts)


@pytest.mark.asyncio
async def test_node_setup_preserves_ordinary_failure_policy():
    inject_hosts = mock.AsyncMock(side_effect=RuntimeError('setup failed'))

    assert await _run_node_setup(inject_hosts) is False
