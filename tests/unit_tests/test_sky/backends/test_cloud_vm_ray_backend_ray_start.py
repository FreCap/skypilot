"""Tests for single-node Ray startup readiness retries."""

import asyncio
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky.backends import cloud_vm_ray_backend
from sky.backends.cloud_vm_ray_backend import RetryingVmProvisioner


def test_ensure_cluster_ray_started_cancellation_stops_before_next_probe():
    provisioner = object.__new__(RetryingVmProvisioner)
    handle = MagicMock(launched_nodes=1)

    with patch.object(
            cloud_vm_ray_backend.CloudVmRayBackend,
            'run_on_head',
            side_effect=[
                (0, 'No cluster status', ''),
                AssertionError('probed ray status after cancellation'),
            ]) as run_on_head, patch.object(
                cloud_vm_ray_backend.context_utils,
                'sleep_with_cancellation',
                side_effect=asyncio.CancelledError(),
                create=True) as wait, patch.object(
                    cloud_vm_ray_backend.time,
                    'sleep',
                    side_effect=AssertionError(
                        'used raw sleep instead of cancelable wait')):
        with pytest.raises(asyncio.CancelledError):
            provisioner._ensure_cluster_ray_started(  # pylint: disable=protected-access
                handle, '/tmp/provision.log')

    assert run_on_head.call_count == 1
    wait.assert_called_once_with(1)


def test_ensure_cluster_ray_started_active_retry_preserves_probe_count():
    provisioner = object.__new__(RetryingVmProvisioner)
    handle = MagicMock(launched_nodes=1)

    with patch.object(cloud_vm_ray_backend.CloudVmRayBackend,
                      'run_on_head',
                      side_effect=[
                          (0, 'No cluster status', ''),
                          (0, 'ray status: ready', ''),
                      ]) as run_on_head, patch.object(
                          cloud_vm_ray_backend.context_utils,
                          'sleep_with_cancellation',
                          create=True) as wait:
        provisioner._ensure_cluster_ray_started(  # pylint: disable=protected-access
            handle, '/tmp/provision.log')

    assert run_on_head.call_count == 2
    first_call = run_on_head.call_args_list[0]
    second_call = run_on_head.call_args_list[1]
    assert first_call.args == second_call.args == (
        handle,
        cloud_vm_ray_backend.instance_setup.
        RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
    )
    assert first_call.kwargs == second_call.kwargs == {'require_outputs': True}
    wait.assert_called_once_with(1)
