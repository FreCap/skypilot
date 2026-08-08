"""Unit tests for CloudVmRayBackend task configuration redaction and locking."""

import asyncio
import multiprocessing
import socket
import subprocess
import sys
import time
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch
import uuid

import pytest

from sky import exceptions
from sky import global_user_state
from sky import resources
from sky import task
from sky.backends import cloud_vm_ray_backend
from sky.backends import skylet_transport
from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle
from sky.backends.cloud_vm_ray_backend import GangSchedulingStatus
from sky.backends.cloud_vm_ray_backend import RetryingVmProvisioner
from sky.backends.cloud_vm_ray_backend import SSHTunnelInfo
from sky.schemas.generated import jobsv1_pb2
from sky.utils import status_lib


@pytest.mark.parametrize('metadata, expected', [
    ((1, 1), SSHTunnelInfo(port=1, pid=1, generation='legacy:1:1')),
    ((65535, 2**31 - 1),
     SSHTunnelInfo(
         port=65535, pid=2**31 - 1, generation=f'legacy:{2**31 - 1}:65535')),
    ((12345, 23456, '00000000-0000-0000-0000-000000000001'),
     SSHTunnelInfo(port=12345,
                   pid=23456,
                   generation='00000000-0000-0000-0000-000000000001')),
])
def test_decode_skylet_ssh_tunnel_metadata_compatibility(metadata, expected):
    decoded = cloud_vm_ray_backend._decode_skylet_ssh_tunnel_metadata(  # pylint: disable=protected-access
        metadata)
    assert decoded == expected
    assert metadata[0] == decoded.port
    assert metadata[1] == decoded.pid


@pytest.mark.parametrize('metadata', [
    None,
    [],
    (),
    (1,),
    (1, 2, 3, 4),
    (True, 2),
    (1, False),
    (0, 1),
    (65536, 1),
    (1, 0),
    (1, 2**31),
    ('1', 2),
    (1, '2'),
    (1, 2, 3),
    (1, 2, 'legacy:2:1'),
    (1, 2, 'not-a-uuid'),
    (1, 2, '00000000-0000-0000-0000-00000000000A'),
    (1, 2, '{00000000-0000-0000-0000-000000000001}'),
])
def test_decode_skylet_ssh_tunnel_metadata_rejects_malformed(metadata):
    with pytest.raises(ValueError):
        cloud_vm_ray_backend._decode_skylet_ssh_tunnel_metadata(  # pylint: disable=protected-access
            metadata)


class TestCloudVmRayResourceHandleCardinality:

    @staticmethod
    def _make_handle(*,
                     stable_internal_external_ips=None,
                     stable_ssh_ports=None):
        launched_resources = MagicMock(spec=resources.Resources)
        launched_resources.assert_launchable.return_value = launched_resources
        launched_resources.cloud.PROVISIONER_VERSION = (
            cloud_vm_ray_backend.clouds.ProvisionerVersion.RAY_AUTOSCALER)
        return CloudVmRayResourceHandle(
            cluster_name='test-cluster',
            cluster_name_on_cloud='test-cluster-on-cloud',
            cluster_yaml='/tmp/test-cluster.yaml',
            launched_nodes=2,
            launched_resources=launched_resources,
            stable_internal_external_ips=stable_internal_external_ips,
            stable_ssh_ports=stable_ssh_ports)

    def test_update_cluster_ips_rejects_mismatched_lists(self):
        handle = self._make_handle()
        cluster_info = MagicMock()
        cluster_info.get_feasible_ips.side_effect = [[
            '203.0.113.1', '203.0.113.2'
        ], ['10.0.0.1']]

        with pytest.raises(AssertionError,
                           match='Expected same number of internal IPs'):
            handle.update_cluster_ips(cluster_info=cluster_info)

        assert handle.stable_internal_external_ips is None

    def test_update_cluster_ips_rejects_mismatched_lists_under_optimization(
            self):
        if sys.flags.optimize == 0:
            # Load this module directly in the optimized child instead of
            # starting a nested pytest session. Nested pytest competes with
            # the already parallel unit suite for shared fixtures and has
            # repeatedly exceeded this otherwise generous process timeout.
            # The direct child still imports SkyPilot, and a passing probe has
            # exceeded 30 seconds under the 16-way Python 3.14 CI suite. Keep
            # the probe bounded while allowing for that observed contention.
            script = ('import runpy; '
                      f'module = runpy.run_path({__file__!r}); '
                      "module['TestCloudVmRayResourceHandleCardinality']()."
                      'test_update_cluster_ips_rejects_mismatched_lists()')
            result = subprocess.run([sys.executable, '-O', '-c', script],
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                    timeout=120)
            assert result.returncode == 0, result.stdout + result.stderr
            return

        self.test_update_cluster_ips_rejects_mismatched_lists()

    def test_get_command_runners_rejects_mismatched_ports(self):
        handle = self._make_handle(stable_internal_external_ips=[
            ('10.0.0.1', '203.0.113.1'), ('10.0.0.2', '203.0.113.2')
        ],
                                   stable_ssh_ports=[22])

        with patch(
                'sky.backends.cloud_vm_ray_backend.backend_utils.'
                'ssh_credential_from_yaml',
                return_value={}), pytest.raises(
                    ValueError, match='same number of SSH ports'):
            handle.get_command_runners()


def test_set_job_info_encodes_nullable_job_group_roles():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = MagicMock(is_grpc_enabled_with_flag=True)
    client = MagicMock()
    client.set_job_info_without_job_id.return_value = (
        jobsv1_pb2.SetJobInfoWithoutJobIdResponse(job_ids=[7]))

    with patch.object(cloud_vm_ray_backend, 'SkyletClient',
                      return_value=client), patch.object(
                          cloud_vm_ray_backend.backend_utils,
                          'invoke_skylet_with_retries',
                          side_effect=lambda callback: callback()):
        job_ids = backend.set_job_info_without_job_id(
            handle=handle,
            name='job',
            workspace='default',
            entrypoint='sky jobs launch job.yaml',
            pool=None,
            pool_hash=None,
            user_hash=None,
            task_ids=[0, 1],
            task_names=['standalone', 'primary'],
            resources_str='{}',
            metadata_jsons=['{}', '{}'],
            is_primary_in_job_groups=[None, True])

    assert job_ids == [7]
    request = client.set_job_info_without_job_id.call_args.args[0]
    assert list(request.is_primary_in_job_groups) == [False, True]
    assert not request.is_primary_in_job_groups_v2[0].HasField('value')
    assert request.is_primary_in_job_groups_v2[1].value is True


def test_add_job_retries_target_not_connected():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = MagicMock()
    handle.is_grpc_enabled_with_flag = False
    handle.cluster_name = 'test-cluster'
    target_not_connected = (
        255, '',
        'An error occurred (TargetNotConnected) when calling StartSession')
    success = (0, 'Job ID: 7\nLog Dir: ~/sky_logs/7-job\n', '')

    with patch.object(
            backend, 'run_on_head',
            side_effect=[target_not_connected, success]) as run_on_head, patch(
                'sky.backends.cloud_vm_ray_backend.context_utils.'
                'sleep_with_cancellation') as sleep:
        job_id, log_dir = backend._add_job(  # pylint: disable=protected-access
            handle, 'job', '{}', '{}')

    assert (job_id, log_dir) == (7, '~/sky_logs/7-job')
    assert run_on_head.call_count == 2
    sleep.assert_called_once()


def test_add_job_stops_target_not_connected_retry_after_cancellation():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = MagicMock()
    handle.is_grpc_enabled_with_flag = False
    handle.cluster_name = 'test-cluster'
    target_not_connected = (
        255, '',
        'An error occurred (TargetNotConnected) when calling StartSession')

    with patch.object(backend, 'run_on_head',
                      return_value=target_not_connected) as run_on_head, patch(
                          'sky.backends.cloud_vm_ray_backend.context_utils.'
                          'sleep_with_cancellation',
                          side_effect=asyncio.CancelledError) as sleep:
        with pytest.raises(asyncio.CancelledError):
            backend._add_job(  # pylint: disable=protected-access
                handle, 'job', '{}', '{}')

    run_on_head.assert_called_once()
    sleep.assert_called_once()


def test_add_job_bounds_target_not_connected_retries():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = MagicMock()
    handle.is_grpc_enabled_with_flag = False
    handle.cluster_name = 'test-cluster'
    target_not_connected = (
        255, '',
        'An error occurred (TargetNotConnected) when calling StartSession')

    with patch.object(backend, 'run_on_head',
                      return_value=target_not_connected) as run_on_head, patch(
                          'sky.backends.cloud_vm_ray_backend.context_utils.'
                          'sleep_with_cancellation') as sleep:
        with pytest.raises(exceptions.CommandError,
                           match='Failed to fetch job id'):
            backend._add_job(  # pylint: disable=protected-access
                handle, 'job', '{}', '{}')

    assert run_on_head.call_count == (
        cloud_vm_ray_backend._JOB_ID_SSM_RECONNECT_MAX_ATTEMPTS)  # pylint: disable=protected-access
    assert sleep.call_count == run_on_head.call_count - 1


@pytest.mark.parametrize(('returncode', 'stderr'),
                         [(255, 'Connection reset by peer'),
                          (1, 'TargetNotConnected')])
def test_add_job_does_not_retry_ambiguous_failures(returncode, stderr):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = MagicMock()
    handle.is_grpc_enabled_with_flag = False
    handle.cluster_name = 'test-cluster'

    with patch.object(
            backend, 'run_on_head',
            return_value=(returncode, '', stderr)) as run_on_head, patch(
                'sky.backends.cloud_vm_ray_backend.context_utils.'
                'sleep_with_cancellation') as sleep:
        with pytest.raises(exceptions.CommandError,
                           match='Failed to fetch job id'):
            backend._add_job(  # pylint: disable=protected-access
                handle, 'job', '{}', '{}')

    run_on_head.assert_called_once()
    sleep.assert_not_called()


@pytest.mark.parametrize(('workers_ready', 'expected_status'),
                         [(False, GangSchedulingStatus.GANG_FAILED),
                          (True, GangSchedulingStatus.CLUSTER_READY)])
def test_gang_schedule_uses_worker_readiness_result(workers_ready,
                                                    expected_status):
    handle = MagicMock(cluster_name='test-cluster', launched_nodes=2)
    logging_info = {'region_name': 'us-east-1', 'zone_str': ''}

    with patch.object(
            cloud_vm_ray_backend,
            'write_ray_up_script_with_patched_launch_hash_fn',
            return_value='/tmp/ray-up.py'), patch.object(
                cloud_vm_ray_backend.log_lib,
                'run_with_log',
                return_value=(
                    0, 'head ready', '')) as mock_ray_up, patch.object(
                        cloud_vm_ray_backend.backend_utils,
                        'wait_until_ray_cluster_ready',
                        return_value=(workers_ready,
                                      'docker-user')) as mock_wait_ready:
        status, stdout, stderr, head_internal_ip, head_external_ip = (
            RetryingVmProvisioner._gang_schedule_ray_up(  # pylint: disable=protected-access
                MagicMock(),
                cloud_vm_ray_backend.clouds.AWS(),
                '/tmp/cluster.yaml',
                handle,
                '/tmp/provision.log',
                stream_logs=False,
                logging_info=logging_info,
                use_spot=False))

    assert status is expected_status
    assert (stdout, stderr, head_internal_ip, head_external_ip) == ('', '',
                                                                    None, None)
    mock_ray_up.assert_called_once()
    mock_wait_ready.assert_called_once_with('/tmp/cluster.yaml',
                                            num_nodes=2,
                                            log_path='/tmp/provision.log',
                                            nodes_launching_progress_timeout=90)


def test_gang_schedule_cancellation_stops_before_next_ray_up():
    handle = MagicMock(cluster_name='test-cluster', launched_nodes=2)
    logging_info = {'region_name': 'us-east-1', 'zone_str': ''}
    retryable_failure = (
        1, 'Processing file mounts',
        'Failed to setup head node. ConnectionResetError: [Errno 54] '
        'Connection reset by peer')

    with patch.object(
            cloud_vm_ray_backend,
            'write_ray_up_script_with_patched_launch_hash_fn',
            return_value='/tmp/ray-up.py'), patch.object(
                cloud_vm_ray_backend.log_lib,
                'run_with_log',
                side_effect=[
                    retryable_failure,
                    AssertionError('ray up retried after cancellation'),
                ]) as ray_up, patch.object(
                    cloud_vm_ray_backend.context_utils,
                    'sleep_with_cancellation',
                    side_effect=asyncio.CancelledError(),
                    create=True) as wait, patch.object(
                        cloud_vm_ray_backend.time, 'sleep'), patch.object(
                            cloud_vm_ray_backend.common_utils,
                            'Backoff') as backoff_cls:
        backoff_cls.return_value.current_backoff.return_value = 17
        with pytest.raises(asyncio.CancelledError):
            RetryingVmProvisioner._gang_schedule_ray_up(  # pylint: disable=protected-access
                MagicMock(),
                cloud_vm_ray_backend.clouds.AWS(),
                '/tmp/cluster.yaml',
                handle,
                '/tmp/provision.log',
                stream_logs=False,
                logging_info=logging_info,
                use_spot=False)

    wait.assert_called_once_with(17)
    ray_up.assert_called_once()


def test_gang_schedule_retry_preserves_backoff_and_call_count():
    handle = MagicMock(cluster_name='test-cluster', launched_nodes=2)
    logging_info = {'region_name': 'us-east-1', 'zone_str': ''}
    retryable_failure = (
        1, 'Processing file mounts',
        'Failed to setup head node. ConnectionResetError: [Errno 54] '
        'Connection reset by peer')

    with patch.object(cloud_vm_ray_backend,
                      'write_ray_up_script_with_patched_launch_hash_fn',
                      return_value='/tmp/ray-up.py'), patch.object(
                          cloud_vm_ray_backend.log_lib,
                          'run_with_log',
                          side_effect=[
                              retryable_failure, (0, 'head ready', '')
                          ]) as ray_up, patch.object(
                              cloud_vm_ray_backend.context_utils,
                              'sleep_with_cancellation') as wait, patch.object(
                                  cloud_vm_ray_backend.backend_utils,
                                  'wait_until_ray_cluster_ready',
                                  return_value=(True, None)), patch.object(
                                      cloud_vm_ray_backend.common_utils,
                                      'Backoff') as backoff_cls:
        backoff_cls.return_value.current_backoff.return_value = 17
        status, _, _, _, _ = RetryingVmProvisioner._gang_schedule_ray_up(  # pylint: disable=protected-access
            MagicMock(),
            cloud_vm_ray_backend.clouds.AWS(),
            '/tmp/cluster.yaml',
            handle,
            '/tmp/provision.log',
            stream_logs=False,
            logging_info=logging_info,
            use_spot=False)

    assert status is GangSchedulingStatus.CLUSTER_READY
    wait.assert_called_once_with(17)
    assert ray_up.call_count == 2


def test_gang_schedule_revalidates_serve_fence_before_every_ray_up_retry():
    handle = MagicMock(cluster_name='test-cluster', launched_nodes=2)
    logging_info = {'region_name': 'us-east-1', 'zone_str': ''}
    retryable_failure = (
        1, 'Processing file mounts',
        'Failed to setup head node. ConnectionResetError: [Errno 54] '
        'Connection reset by peer')
    provisioner = MagicMock()
    provisioner._validate_service_replica_launch_fence.side_effect = [
        None,
        exceptions.RequestCancelled('generation changed'),
    ]

    with patch.object(
            cloud_vm_ray_backend,
            'write_ray_up_script_with_patched_launch_hash_fn',
            return_value='/tmp/ray-up.py'), patch.object(
                cloud_vm_ray_backend.log_lib,
                'run_with_log',
                return_value=retryable_failure) as ray_up, patch.object(
                    cloud_vm_ray_backend.context_utils,
                    'sleep_with_cancellation') as wait, patch.object(
                        cloud_vm_ray_backend.common_utils,
                        'Backoff') as backoff_cls:
        backoff_cls.return_value.current_backoff.return_value = 17
        with pytest.raises(exceptions.RequestCancelled,
                           match='generation changed'):
            RetryingVmProvisioner._gang_schedule_ray_up(  # pylint: disable=protected-access
                provisioner,
                cloud_vm_ray_backend.clouds.AWS(),
                '/tmp/cluster.yaml',
                handle,
                '/tmp/provision.log',
                stream_logs=False,
                logging_info=logging_info,
                use_spot=False)

    assert provisioner._validate_service_replica_launch_fence.call_count == 2
    ray_up.assert_called_once()
    wait.assert_called_once_with(17)


def test_wait_service_registration_rpc_covers_both_phase_budgets():
    """The RPC deadline must not preempt either server-side wait phase."""
    client = object.__new__(cloud_vm_ray_backend.SkyletClient)
    client._serve_stub = MagicMock()  # pylint: disable=protected-access
    request = MagicMock()

    client.wait_service_registration(request)

    expected_timeout = (
        cloud_vm_ray_backend.serve_constants.CONTROLLER_SETUP_TIMEOUT_SECONDS +
        cloud_vm_ray_backend.serve_constants.SERVICE_REGISTER_TIMEOUT_SECONDS +
        10)
    client._serve_stub.WaitServiceRegistration.assert_called_once_with(  # pylint: disable=protected-access
        request, timeout=expected_timeout)


class TestCloudVmRayBackendTaskRedaction:
    """Tests for CloudVmRayBackend usage of redacted task configs."""

    def test_cloud_vm_ray_backend_redaction_usage_pattern(self):
        """Test the exact usage pattern from the CloudVmRayBackend code."""
        # Create a task with sensitive secret variables and regular environment variables
        test_task = task.Task(run='echo hello',
                              envs={
                                  'DEBUG': 'true',
                                  'PORT': '8080'
                              },
                              secrets={
                                  'API_KEY': 'sk-very-secret-key-123',
                                  'DATABASE_PASSWORD': 'super-secret-password',
                                  'JWT_SECRET': 'jwt-signing-secret-456'
                              })

        # Test the exact call pattern used in cloud_vm_ray_backend.py
        task_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # Verify that environment variables are NOT redacted
        assert 'envs' in task_config
        assert task_config['envs']['DEBUG'] == 'true'
        assert task_config['envs']['PORT'] == '8080'

        # Verify that secrets ARE redacted
        assert 'secrets' in task_config
        assert task_config['secrets']['API_KEY'] == '<redacted>'
        assert task_config['secrets']['DATABASE_PASSWORD'] == '<redacted>'
        assert task_config['secrets']['JWT_SECRET'] == '<redacted>'

        # Verify other task fields are preserved
        assert task_config['run'] == 'echo hello'

    def test_backend_task_config_without_secrets(self):
        """Test task config generation when no secrets are present."""
        test_task = task.Task(run='python train.py',
                              envs={'PYTHONPATH': '/app'})

        task_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # Environment variables should be preserved
        assert task_config['envs']['PYTHONPATH'] == '/app'

        # No secrets field should be present
        assert 'secrets' not in task_config or not task_config.get('secrets')

    def test_backend_task_config_empty_secrets(self):
        """Test task config generation with empty secrets dict."""
        test_task = task.Task(run='python train.py',
                              envs={'PYTHONPATH': '/app'},
                              secrets={})

        task_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # Environment variables should be preserved
        assert task_config['envs']['PYTHONPATH'] == '/app'

        # Empty secrets should not appear in config
        assert 'secrets' not in task_config

    def test_backend_redaction_redacts_all_values(self):
        """Test that all secret values (including non-string) are redacted."""
        test_task = task.Task(run='echo hello',
                              secrets={
                                  'STRING_SECRET': 'actual-secret',
                                  'NUMERIC_PORT': 5432,
                                  'BOOLEAN_FLAG': True,
                                  'NULL_VALUE': None
                              })

        task_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # String values should be redacted
        assert task_config['secrets']['STRING_SECRET'] == '<redacted>'

        # All values should be redacted (including non-string values)
        assert task_config['secrets']['NUMERIC_PORT'] == '<redacted>'
        assert task_config['secrets']['BOOLEAN_FLAG'] == '<redacted>'
        assert task_config['secrets']['NULL_VALUE'] == '<redacted>'

    def test_backend_supports_both_redaction_modes(self):
        """Test that backend can use both redacted and non-redacted configs."""
        test_task = task.Task(run='echo hello',
                              secrets={'API_KEY': 'secret-key-123'})

        # Test redacted mode (for logging/display)
        redacted_config = test_task.to_yaml_config(use_user_specified_yaml=True)
        assert redacted_config['secrets']['API_KEY'] == '<redacted>'

        # Test non-redacted mode (for execution)
        full_config = test_task.to_yaml_config(use_user_specified_yaml=False)
        assert full_config['secrets']['API_KEY'] == 'secret-key-123'

    def test_backend_mixed_envs_and_secrets(self):
        """Test backend behavior with both envs and secrets containing sensitive data."""
        test_task = task.Task(
            run='echo hello',
            envs={
                'PUBLIC_API_URL': 'https://api.example.com',
                'DEBUG_MODE': 'true',
                'ENVIRONMENT': 'production'
            },
            secrets={
                'PRIVATE_API_KEY': 'sk-secret-key-123',
                'DATABASE_URL': 'postgresql://user:pass@host:5432/db',
                'OAUTH_CLIENT_SECRET': 'oauth-secret-456'
            })

        task_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # All environment variables should remain visible
        assert task_config['envs'][
            'PUBLIC_API_URL'] == 'https://api.example.com'
        assert task_config['envs']['DEBUG_MODE'] == 'true'
        assert task_config['envs']['ENVIRONMENT'] == 'production'

        # All secrets should be redacted
        assert task_config['secrets']['PRIVATE_API_KEY'] == '<redacted>'
        assert task_config['secrets']['DATABASE_URL'] == '<redacted>'
        assert task_config['secrets']['OAUTH_CLIENT_SECRET'] == '<redacted>'

    def test_backend_config_serialization_safety(self):
        """Test that redacted configs are safe for serialization/logging."""
        import json

        import yaml

        test_task = task.Task(
            run='echo hello',
            envs={'PUBLIC_VAR': 'public-value'},
            secrets={'PRIVATE_KEY': 'very-sensitive-key-data'})

        redacted_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # Should be serializable to JSON
        json_str = json.dumps(redacted_config)
        assert 'very-sensitive-key-data' not in json_str
        assert '<redacted>' in json_str

        # Should be serializable to YAML
        yaml_str = yaml.dump(redacted_config)
        assert 'very-sensitive-key-data' not in yaml_str
        assert '<redacted>' in yaml_str

        # Public values should still be present
        assert 'public-value' in yaml_str

    def test_redacted_config_contains_no_sensitive_data(self):
        """Test that redacted task config doesn't contain sensitive secret data."""
        # Create a task with sensitive secret variables and regular environment variables
        test_task = task.Task(run='echo hello',
                              envs={
                                  'DEBUG': 'true',
                                  'PORT': 8080,
                                  'PUBLIC_VAR': 'public-value'
                              },
                              secrets={
                                  'API_KEY': 'secret-api-key-123',
                                  'DATABASE_PASSWORD': 'super-secret-password',
                                  'AWS_SECRET_ACCESS_KEY': 'aws-secret-key',
                                  'STRIPE_SECRET_KEY': 'sk_live_sensitive_key',
                                  'JWT_SECRET': 'jwt-signing-secret',
                              })

        # Get the redacted config as the backend would
        redacted_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # Verify sensitive string values in secrets are redacted
        assert redacted_config['secrets']['API_KEY'] == '<redacted>'
        assert redacted_config['secrets']['DATABASE_PASSWORD'] == '<redacted>'
        assert redacted_config['secrets'][
            'AWS_SECRET_ACCESS_KEY'] == '<redacted>'
        assert redacted_config['secrets']['STRIPE_SECRET_KEY'] == '<redacted>'
        assert redacted_config['secrets']['JWT_SECRET'] == '<redacted>'

        # Verify envs are NOT redacted (preserved as-is)
        assert redacted_config['envs']['DEBUG'] == 'true'
        assert redacted_config['envs']['PORT'] == 8080
        assert redacted_config['envs']['PUBLIC_VAR'] == 'public-value'

        # Ensure no sensitive data appears anywhere in the config
        config_str = str(redacted_config)
        assert 'secret-api-key-123' not in config_str
        assert 'super-secret-password' not in config_str
        assert 'aws-secret-key' not in config_str
        assert 'sk_live_sensitive_key' not in config_str
        assert 'jwt-signing-secret' not in config_str

        # But public values should still be present
        assert 'public-value' in config_str

    def test_non_redacted_config_contains_actual_values(self):
        """Test that non-redacted config contains actual secret values."""
        # Create a task with environment variables and secrets
        test_task = task.Task(run='echo hello',
                              envs={
                                  'DEBUG': 'true',
                                  'PORT': 8080
                              },
                              secrets={
                                  'API_KEY': 'actual-api-key',
                                  'JWT_SECRET': 'actual-jwt-secret'
                              })

        # Get the non-redacted config
        non_redacted_config = test_task.to_yaml_config(
            use_user_specified_yaml=False)

        # Verify actual values are present in both envs and secrets
        assert non_redacted_config['envs']['DEBUG'] == 'true'
        assert non_redacted_config['envs']['PORT'] == 8080
        assert non_redacted_config['secrets']['API_KEY'] == 'actual-api-key'
        assert non_redacted_config['secrets'][
            'JWT_SECRET'] == 'actual-jwt-secret'

        # Also test default behavior (should NOT redact secrets by default)
        default_config = test_task.to_yaml_config()
        assert default_config['envs'] == non_redacted_config[
            'envs']  # envs same
        assert default_config['secrets'][
            'API_KEY'] == 'actual-api-key'  # secrets not redacted by default

    def test_backend_redaction_with_no_secrets(self):
        """Test backend behavior when task has no secret variables."""
        # Create a task with only environment variables, no secrets
        test_task = task.Task(run='echo hello', envs={'DEBUG': 'true'})

        # Get redacted config
        redacted_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # Should not have secrets key at all
        assert 'secrets' not in redacted_config

        # Should have envs key with actual values (not redacted)
        assert 'envs' in redacted_config
        assert redacted_config['envs']['DEBUG'] == 'true'

        # Should still have other task properties
        assert redacted_config['run'] == 'echo hello'

    def test_backend_redaction_preserves_task_structure(self):
        """Test that redaction preserves all non-secret task configuration."""
        from sky import resources

        # Create a comprehensive task
        test_task = task.Task(run='python train.py',
                              envs={
                                  'DEBUG': 'true',
                                  'PORT': 8080
                              },
                              secrets={
                                  'API_KEY': 'secret-value',
                                  'DB_PASSWORD': 'secret-password'
                              },
                              workdir='/app',
                              name='training-task')
        # Set resources using the proper method
        test_task.set_resources(resources.Resources(cpus=4, memory=8))

        # Get both configs
        original_config = test_task.to_yaml_config(
            use_user_specified_yaml=False)
        redacted_config = test_task.to_yaml_config(use_user_specified_yaml=True)

        # All non-secret fields should be identical
        for key in original_config:
            if key != 'secrets':
                assert original_config[key] == redacted_config[key]

        # Envs should be identical (not redacted)
        assert original_config['envs'] == redacted_config['envs']
        assert redacted_config['envs']['DEBUG'] == 'true'
        assert redacted_config['envs']['PORT'] == 8080

        # Secret handling should be different
        assert original_config['secrets']['API_KEY'] == 'secret-value'
        assert redacted_config['secrets']['API_KEY'] == '<redacted>'
        assert original_config['secrets']['DB_PASSWORD'] == 'secret-password'
        assert redacted_config['secrets']['DB_PASSWORD'] == '<redacted>'


class TestCloudVmRayBackendGetGrpcChannel:
    """Tests for CloudVmRayBackend get_grpc_channel."""
    MOCK_HANDLE_KWARGS = {
        'cluster_name': 'test-cluster',
        'cluster_name_on_cloud': 'test-cluster-abc',
        'cluster_yaml': None,
        'launched_nodes': 1,
        'launched_resources': MagicMock(),
    }

    INITIAL_TUNNEL_PORT = 10000
    INITIAL_TUNNEL_PID = 12345
    PROCESS_JOIN_TIMEOUT_SECONDS = 30

    @pytest.fixture(autouse=True)
    def _reset_tunnel_process_ownership(self):
        cloud_vm_ray_backend._reset_skylet_tunnel_process_ownership_after_fork(  # pylint: disable=protected-access
        )
        yield
        cloud_vm_ray_backend._reset_skylet_tunnel_process_ownership_after_fork(  # pylint: disable=protected-access
        )

    @staticmethod
    def _tunnel_state(tunnel, *, cluster_hash='cluster-hash'):
        metadata = None
        if tunnel is not None:
            metadata = (tunnel.port, tunnel.pid, tunnel.generation)
        return cloud_vm_ray_backend._SkyletTunnelStateV1(  # pylint: disable=protected-access
            observed=global_user_state.ClusterSkyletSSHTunnelSnapshotV1(
                cluster_hash=cluster_hash,
                metadata=metadata,
                serialized_metadata=None,
            ),
            tunnel=tunnel,
        )

    class _FakeClock:
        """Minimal monotonic clock for deterministic deadline tests."""

        def __init__(self):
            self.now = 0.0
            self.sleeps = []

        def perf_counter(self):
            return self.now

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            self.now += seconds

    def _simulate_process_get_grpc_channel(self, queue, tunnel_creation_count,
                                           tunnel_port, tunnel_pid,
                                           socket_connect_side_effect):
        """Simulate a process calling get_grpc_channel.

        This test mocks:
        - _get_skylet_ssh_tunnel_state: To avoid making an actual DB query
        - _open_and_update_skylet_tunnel: To avoid actually opening an SSH tunnel
        - grpc.insecure_channel: To just return the address instead of a Channel object
        - socket.socket.connect: To avoid actually connecting to the tunnel

        This test does not mock:
        - lock.acquire
        """
        try:
            # Different processes have different handle instances.
            handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)

            def mock_get_tunnel_side_effect():
                # Return None if the tunnel is not created yet.
                if tunnel_port.value == -1 or tunnel_pid.value == -1:
                    return self._tunnel_state(None)
                tunnel = SSHTunnelInfo(
                    port=tunnel_port.value,
                    pid=tunnel_pid.value,
                    generation=str(uuid.UUID(int=tunnel_port.value)),
                )
                return self._tunnel_state(tunnel)

            def mock_open_tunnel(tunnel_state):
                del tunnel_state
                # Simulate time taken to create tunnel.
                time.sleep(2)
                with tunnel_creation_count.get_lock():
                    tunnel_creation_count.value += 1
                    created = tunnel_creation_count.value
                with tunnel_port.get_lock(), tunnel_pid.get_lock():
                    # First creation -> 10000/12345; second -> 10001/12346; and so on.
                    tunnel_port.value = self.INITIAL_TUNNEL_PORT + (created - 1)
                    tunnel_pid.value = self.INITIAL_TUNNEL_PID + (created - 1)
                    return SSHTunnelInfo(
                        port=tunnel_port.value,
                        pid=tunnel_pid.value,
                        generation=str(uuid.UUID(int=tunnel_port.value)),
                    )

            with patch.object(handle, '_get_skylet_ssh_tunnel_state', side_effect=mock_get_tunnel_side_effect), \
                patch.object(handle, '_open_and_update_skylet_tunnel', side_effect=mock_open_tunnel), \
                patch('grpc.insecure_channel', side_effect=lambda addr, options: addr), \
                patch('socket.socket') as mock_socket:

                mock_socket.return_value.__enter__.return_value.connect.side_effect = socket_connect_side_effect

                res = handle.get_grpc_channel()
                assert res is not None
                queue.put(res)

        except Exception as e:
            import traceback
            error_msg = f"Error: {e}\nTraceback: {traceback.format_exc()}"
            queue.put(error_msg)

    def _socket_connect_side_effect(self, addr):
        _, port = addr
        # Force an error on the original port to test the retry logic.
        if port == self.INITIAL_TUNNEL_PORT:
            raise socket.error("Connection error")
        return None

    def test_channel_snapshot_binds_fast_path_identity(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                               pid=self.INITIAL_TUNNEL_PID,
                               generation=str(uuid.UUID(int=11)))
        tunnel_state = self._tunnel_state(tunnel, cluster_hash='hash-fast')

        with patch.object(
                handle, '_get_skylet_ssh_tunnel_state',
                return_value=tunnel_state) as get_state, patch.object(
                    cloud_vm_ray_backend,
                    '_is_tunnel_healthy',
                    return_value=True), patch(
                        'grpc.insecure_channel',
                        side_effect=lambda endpoint, options: endpoint):
            snapshot = handle.get_grpc_channel_with_snapshot()

        assert snapshot.channel == f'localhost:{self.INITIAL_TUNNEL_PORT}'
        assert snapshot.key == skylet_transport.SkyletChannelKeyV1(
            cluster_hash='hash-fast',
            endpoint=f'localhost:{self.INITIAL_TUNNEL_PORT}',
            tunnel_generation=tunnel.generation,
        )
        get_state.assert_called_once_with()

    def test_channel_snapshot_binds_newly_published_identity(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        empty_state = self._tunnel_state(None, cluster_hash='hash-new')
        new_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT + 1,
                                   pid=self.INITIAL_TUNNEL_PID + 1,
                                   generation=str(uuid.UUID(int=17)))

        class _AcquiredLock:

            def acquire(self, blocking):
                assert not blocking
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                del exc_type, exc_value, traceback

        with patch.object(
                handle,
                '_get_skylet_ssh_tunnel_state',
                side_effect=[empty_state, empty_state]), patch.object(
                    handle,
                    '_open_and_update_skylet_tunnel',
                    return_value=new_tunnel) as open_tunnel, patch.object(
                        cloud_vm_ray_backend.locks,
                        'get_lock',
                        return_value=_AcquiredLock()), patch(
                            'grpc.insecure_channel',
                            side_effect=lambda endpoint, options: endpoint):
            snapshot = handle.get_grpc_channel_with_snapshot()

        assert snapshot.channel == f'localhost:{new_tunnel.port}'
        assert snapshot.key == skylet_transport.SkyletChannelKeyV1(
            cluster_hash='hash-new',
            endpoint=f'localhost:{new_tunnel.port}',
            tunnel_generation=new_tunnel.generation,
        )
        open_tunnel.assert_called_once_with(empty_state)

    def test_channel_snapshot_binds_shared_wakeup_identity(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        empty_state = self._tunnel_state(None, cluster_hash='hash-shared')
        fresh_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT + 2,
                                     pid=self.INITIAL_TUNNEL_PID + 2,
                                     generation=str(uuid.UUID(int=18)))
        fresh_state = self._tunnel_state(fresh_tunnel,
                                         cluster_hash='hash-shared')

        class _ContendedLock:

            def acquire(self, blocking):
                assert not blocking
                raise cloud_vm_ray_backend.locks.LockTimeout

        class _SharedLock:

            def acquire(self, blocking):
                assert blocking

            def release(self):
                pass

        def get_lock(lock_id, timeout, *, shared_lock=False):
            del lock_id, timeout
            return _SharedLock() if shared_lock else _ContendedLock()

        with patch.object(
                handle,
                '_get_skylet_ssh_tunnel_state',
                side_effect=[empty_state, fresh_state]), patch.object(
                    cloud_vm_ray_backend,
                    '_is_tunnel_healthy',
                    return_value=True), patch.object(
                        cloud_vm_ray_backend.locks,
                        'get_lock',
                        side_effect=get_lock), patch.object(
                            cloud_vm_ray_backend.random,
                            'uniform',
                            return_value=0.0), patch(
                                'grpc.insecure_channel',
                                side_effect=lambda endpoint, options: endpoint):
            snapshot = handle.get_grpc_channel_with_snapshot()

        assert snapshot.channel == f'localhost:{fresh_tunnel.port}'
        assert snapshot.key == skylet_transport.SkyletChannelKeyV1(
            cluster_hash='hash-shared',
            endpoint=f'localhost:{fresh_tunnel.port}',
            tunnel_generation=fresh_tunnel.generation,
        )

    def test_null_hash_channel_is_capability_read_only(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                               pid=self.INITIAL_TUNNEL_PID,
                               generation=str(uuid.UUID(int=12)))
        tunnel_state = self._tunnel_state(tunnel, cluster_hash=None)

        with patch.object(
                handle, '_get_skylet_ssh_tunnel_state',
                return_value=tunnel_state), patch.object(
                    cloud_vm_ray_backend,
                    '_is_tunnel_healthy',
                    return_value=True), patch(
                        'grpc.insecure_channel',
                        side_effect=lambda endpoint, options: endpoint):
            capability_snapshot = (
                handle.get_capability_channel_with_snapshot())
            channel = handle.get_grpc_channel()
            with pytest.raises(exceptions.SkyletUnavailableError,
                               match='no fenced healthy'):
                handle.get_grpc_channel_with_snapshot()

        assert capability_snapshot.channel == channel
        assert capability_snapshot.key.cluster_hash is None
        assert not capability_snapshot.publishable

    @pytest.mark.parametrize('tunnel, healthy', [
        (None, False),
        (SSHTunnelInfo(
            port=10000,
            pid=12345,
            generation='00000000-0000-0000-0000-00000000000d'), False),
    ])
    def test_null_hash_missing_or_unhealthy_never_recovers(
            self, tunnel, healthy):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel_state = self._tunnel_state(tunnel, cluster_hash=None)

        with patch.object(
                handle,
                '_get_skylet_ssh_tunnel_state',
                return_value=tunnel_state), patch.object(
                    cloud_vm_ray_backend,
                    '_is_tunnel_healthy',
                    return_value=healthy), patch.object(
                        cloud_vm_ray_backend.locks,
                        'get_lock') as get_lock, patch.object(
                            handle,
                            '_open_and_update_skylet_tunnel') as open_tunnel, \
                patch.object(global_user_state,
                             'compare_and_set_cluster_skylet_ssh_tunnel_metadata') as cas, \
                patch.object(handle,
                             '_terminate_ssh_tunnel_process') as terminate:
            with pytest.raises(exceptions.SkyletUnavailableError,
                               match='recovery is disabled'):
                handle.get_grpc_channel()

        get_lock.assert_not_called()
        open_tunnel.assert_not_called()
        cas.assert_not_called()
        terminate.assert_not_called()

    def test_missing_cluster_row_never_enters_tunnel_recovery(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        with patch.object(
                global_user_state,
                'get_cluster_skylet_ssh_tunnel_snapshot',
                return_value=None), patch.object(
                    cloud_vm_ray_backend.locks,
                    'get_lock') as get_lock, patch.object(
                        handle,
                        '_open_and_update_skylet_tunnel') as open_tunnel:
            with pytest.raises(exceptions.SkyletUnavailableError,
                               match='Cluster row'):
                handle.get_grpc_channel()

        get_lock.assert_not_called()
        open_tunnel.assert_not_called()

    def test_malformed_tunnel_metadata_is_quarantined(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        observed = global_user_state.ClusterSkyletSSHTunnelSnapshotV1(
            cluster_hash='hash-malformed',
            metadata=(10000,),
            serialized_metadata=b'raw-malformed',
        )
        with patch.object(
                global_user_state,
                'get_cluster_skylet_ssh_tunnel_snapshot',
                return_value=observed), patch.object(
                    cloud_vm_ray_backend.locks,
                    'get_lock') as get_lock, patch.object(
                        handle,
                        '_open_and_update_skylet_tunnel') as open_tunnel, \
                patch.object(handle,
                             '_terminate_ssh_tunnel_process') as terminate:
            with pytest.raises(exceptions.SkyletUnavailableError,
                               match='malformed'):
                handle.get_grpc_channel()
            with pytest.raises(exceptions.SkyletUnavailableError,
                               match='malformed'):
                handle.close_skylet_ssh_tunnel()

        get_lock.assert_not_called()
        open_tunnel.assert_not_called()
        terminate.assert_not_called()

    def test_unfenced_tunnel_mutations_have_no_side_effects(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                               pid=self.INITIAL_TUNNEL_PID,
                               generation=str(uuid.UUID(int=14)))
        tunnel_state = self._tunnel_state(tunnel, cluster_hash=None)

        with patch.object(handle, 'get_command_runners') as runners, \
                patch.object(cloud_vm_ray_backend.backend_utils,
                             'open_ssh_tunnel') as open_tunnel, \
                patch.object(global_user_state,
                             'compare_and_set_cluster_skylet_ssh_tunnel_metadata') as cas, \
                patch.object(handle,
                             '_terminate_ssh_tunnel_process') as terminate:
            publish = handle._open_and_update_skylet_tunnel(  # pylint: disable=protected-access
                tunnel_state)
            with patch.object(handle,
                              '_get_skylet_ssh_tunnel_state',
                              return_value=tunnel_state):
                clear = handle.close_skylet_ssh_tunnel()

        assert publish is (
            skylet_transport.TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION)
        assert clear is (
            skylet_transport.TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION)
        runners.assert_not_called()
        open_tunnel.assert_not_called()
        cas.assert_not_called()
        terminate.assert_not_called()

    def test_matching_tunnel_process_ownership_is_consumed_once(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                               pid=self.INITIAL_TUNNEL_PID,
                               generation=str(uuid.UUID(int=21)))
        process = MagicMock(pid=tunnel.pid)
        process.is_running.return_value = True
        process.status.return_value = 'running'
        descendant = MagicMock(pid=tunnel.pid + 1)
        process.children.return_value = [descendant]

        with patch.object(
                cloud_vm_ray_backend.psutil, 'Process',
                return_value=process) as process_constructor, patch.object(
                    cloud_vm_ray_backend.subprocess_utils,
                    'kill_process_with_grace_period') as kill_process:
            cloud_vm_ray_backend._register_skylet_tunnel_process(  # pylint: disable=protected-access
                tunnel.generation, tunnel.pid)
            handle._terminate_ssh_tunnel_process(tunnel)  # pylint: disable=protected-access
            handle._terminate_ssh_tunnel_process(tunnel)  # pylint: disable=protected-access

        process_constructor.assert_called_once_with(tunnel.pid)
        process.children.assert_called_once_with(recursive=True)
        assert kill_process.call_args_list == [call(process), call(descendant)]

    @pytest.mark.parametrize('metadata', [
        (INITIAL_TUNNEL_PORT, INITIAL_TUNNEL_PID),
        (INITIAL_TUNNEL_PORT, INITIAL_TUNNEL_PID,
         '00000000-0000-0000-0000-000000000016'),
    ])
    def test_unowned_tunnel_metadata_never_signals(self, metadata):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel = cloud_vm_ray_backend._decode_skylet_ssh_tunnel_metadata(  # pylint: disable=protected-access
            metadata)

        with patch.object(cloud_vm_ray_backend.psutil,
                          'Process') as process_constructor, patch.object(
                              cloud_vm_ray_backend.subprocess_utils,
                              'kill_process_with_grace_period') as kill_process:
            handle._terminate_ssh_tunnel_process(tunnel)  # pylint: disable=protected-access

        process_constructor.assert_not_called()
        kill_process.assert_not_called()

    def test_publish_same_pid_uses_exact_generation_identity(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        old_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                                   pid=self.INITIAL_TUNNEL_PID,
                                   generation=str(uuid.UUID(int=23)))
        tunnel_state = self._tunnel_state(old_tunnel,
                                          cluster_hash='hash-pid-reuse')
        original_process = MagicMock(pid=old_tunnel.pid)
        # psutil.Process.is_running() compares its cached create time, so False
        # represents the numeric PID now identifying a different process.
        original_process.is_running.return_value = False
        replacement_process = MagicMock(pid=old_tunnel.pid)
        replacement_process.is_running.return_value = True
        replacement_process.status.return_value = 'running'
        replacement_process.children.return_value = []

        class _ReadyFuture:

            def result(self, *, timeout):
                assert timeout == cloud_vm_ray_backend.constants.SKYLET_GRPC_TIMEOUT_SECONDS

        with patch.object(handle,
                          'get_command_runners',
                          return_value=[object()]), patch.object(
                              cloud_vm_ray_backend.backend_utils,
                              'open_ssh_tunnel',
                              return_value=MagicMock(pid=old_tunnel.pid)), \
                patch.object(cloud_vm_ray_backend.psutil,
                             'Process',
                             side_effect=[original_process,
                                          replacement_process]) as process_constructor, \
                patch('grpc.insecure_channel',
                      return_value=object()), patch(
                          'grpc.channel_ready_future',
                          return_value=_ReadyFuture()), patch.object(
                              global_user_state,
                              'compare_and_set_cluster_skylet_ssh_tunnel_metadata',
                              return_value=skylet_transport.TunnelMutationResult.UPDATED), \
                patch.object(cloud_vm_ray_backend.subprocess_utils,
                             'kill_process_with_grace_period') as kill_process:
            cloud_vm_ray_backend._register_skylet_tunnel_process(  # pylint: disable=protected-access
                old_tunnel.generation, old_tunnel.pid)
            replacement_tunnel = handle._open_and_update_skylet_tunnel(  # pylint: disable=protected-access
                tunnel_state)
            assert isinstance(replacement_tunnel, SSHTunnelInfo)
            assert replacement_tunnel.pid == old_tunnel.pid
            assert replacement_tunnel.generation != old_tunnel.generation
            handle._terminate_ssh_tunnel_process(  # pylint: disable=protected-access
                replacement_tunnel)

        assert process_constructor.call_args_list == [
            call(old_tunnel.pid),
            call(old_tunnel.pid),
        ]
        original_process.children.assert_not_called()
        kill_process.assert_called_once_with(replacement_process)

    def test_process_ownership_is_cleared_after_pid_change(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        inherited_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                                         pid=self.INITIAL_TUNNEL_PID,
                                         generation=str(uuid.UUID(int=24)))
        child_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT + 1,
                                     pid=self.INITIAL_TUNNEL_PID + 1,
                                     generation=str(uuid.UUID(int=25)))
        inherited_process = MagicMock(pid=inherited_tunnel.pid)
        child_process = MagicMock(pid=child_tunnel.pid)
        child_process.is_running.return_value = True
        child_process.status.return_value = 'running'
        child_process.children.return_value = []
        original_pid = cloud_vm_ray_backend.os.getpid()

        with patch.object(
                cloud_vm_ray_backend.psutil,
                'Process',
                side_effect=[inherited_process,
                             child_process]) as process_constructor, \
                patch.object(cloud_vm_ray_backend.subprocess_utils,
                             'kill_process_with_grace_period') as kill_process:
            cloud_vm_ray_backend._register_skylet_tunnel_process(  # pylint: disable=protected-access
                inherited_tunnel.generation, inherited_tunnel.pid)
            with patch.object(cloud_vm_ray_backend.os,
                              'getpid',
                              return_value=original_pid + 1):
                handle._terminate_ssh_tunnel_process(  # pylint: disable=protected-access
                    inherited_tunnel)
                cloud_vm_ray_backend._register_skylet_tunnel_process(  # pylint: disable=protected-access
                    child_tunnel.generation, child_tunnel.pid)
                handle._terminate_ssh_tunnel_process(  # pylint: disable=protected-access
                    child_tunnel)

        assert process_constructor.call_args_list == [
            call(inherited_tunnel.pid),
            call(child_tunnel.pid),
        ]
        kill_process.assert_called_once_with(child_process)

    @pytest.mark.parametrize('failure', ['conflict', 'readiness'])
    def test_new_process_cleanup_uses_registered_identity(self, failure):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel_state = self._tunnel_state(None, cluster_hash='hash-cleanup')
        new_pid = self.INITIAL_TUNNEL_PID + 1
        process = MagicMock(pid=new_pid)
        process.is_running.return_value = True
        process.status.return_value = 'running'
        descendant = MagicMock(pid=new_pid + 1)
        process.children.return_value = [descendant]

        class _ReadyFuture:

            def result(self, *, timeout):
                assert timeout == cloud_vm_ray_backend.constants.SKYLET_GRPC_TIMEOUT_SECONDS
                if failure == 'readiness':
                    raise cloud_vm_ray_backend.grpc.FutureTimeoutError()

        with patch.object(handle,
                          'get_command_runners',
                          return_value=[object()]), patch.object(
                              cloud_vm_ray_backend.random,
                              'randint',
                              return_value=self.INITIAL_TUNNEL_PORT + 2), \
                patch.object(cloud_vm_ray_backend.backend_utils,
                             'open_ssh_tunnel',
                             return_value=MagicMock(pid=new_pid)), patch.object(
                                 cloud_vm_ray_backend.psutil,
                                 'Process',
                                 return_value=process) as process_constructor, \
                patch('grpc.insecure_channel',
                      return_value=object()), patch(
                          'grpc.channel_ready_future',
                          return_value=_ReadyFuture()), patch.object(
                              global_user_state,
                              'compare_and_set_cluster_skylet_ssh_tunnel_metadata',
                              return_value=skylet_transport.TunnelMutationResult.CONFLICT) as cas, \
                patch.object(cloud_vm_ray_backend.subprocess_utils,
                             'kill_process_with_grace_period') as kill_process:
            if failure == 'readiness':
                with pytest.raises(
                        cloud_vm_ray_backend.grpc.FutureTimeoutError):
                    handle._open_and_update_skylet_tunnel(  # pylint: disable=protected-access
                        tunnel_state)
                cas.assert_not_called()
            else:
                result = handle._open_and_update_skylet_tunnel(  # pylint: disable=protected-access
                    tunnel_state)
                assert result is skylet_transport.TunnelMutationResult.CONFLICT
                cas.assert_called_once()

        process_constructor.assert_called_once_with(new_pid)
        process.children.assert_called_once_with(recursive=True)
        assert kill_process.call_args_list == [call(process), call(descendant)]

    def test_registration_failure_cleans_up_exact_opened_process(self):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel_state = self._tunnel_state(None, cluster_hash='hash-register')
        opened_process = MagicMock(pid=self.INITIAL_TUNNEL_PID + 1)
        registration_error = RuntimeError('injected registration failure')

        with patch.object(handle,
                          'get_command_runners',
                          return_value=[object()]), patch.object(
                              cloud_vm_ray_backend.backend_utils,
                              'open_ssh_tunnel',
                              return_value=opened_process), patch.object(
                                  cloud_vm_ray_backend,
                                  '_register_skylet_tunnel_process',
                                  side_effect=registration_error), patch.object(
                                      cloud_vm_ray_backend.subprocess_utils,
                                      'kill_process_with_grace_period') as kill_process, \
                patch.object(global_user_state,
                             'compare_and_set_cluster_skylet_ssh_tunnel_metadata') as cas:
            with pytest.raises(RuntimeError,
                               match='injected registration failure'):
                handle._open_and_update_skylet_tunnel(  # pylint: disable=protected-access
                    tunnel_state)

        kill_process.assert_called_once_with(opened_process)
        cas.assert_not_called()

    @pytest.mark.parametrize('outcome, expected_events', [
        (skylet_transport.TunnelMutationResult.UPDATED,
         ['open', 'register', 'ready', 'cas', 'kill-old']),
        (skylet_transport.TunnelMutationResult.CONFLICT,
         ['open', 'register', 'ready', 'cas', 'kill-new']),
    ])
    def test_nonnull_tunnel_publish_cas_owns_process_order(
            self, outcome, expected_events):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        old_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                                   pid=self.INITIAL_TUNNEL_PID,
                                   generation=str(uuid.UUID(int=15)))
        tunnel_state = self._tunnel_state(old_tunnel,
                                          cluster_hash='hash-publish')
        new_pid = self.INITIAL_TUNNEL_PID + 1
        events = []

        class _ReadyFuture:

            def result(self, *, timeout):
                assert timeout == cloud_vm_ray_backend.constants.SKYLET_GRPC_TIMEOUT_SECONDS
                events.append('ready')

        def open_tunnel(runner, ports):
            del runner
            assert ports == (self.INITIAL_TUNNEL_PORT + 2,
                             cloud_vm_ray_backend.constants.SKYLET_GRPC_PORT)
            events.append('open')
            return MagicMock(pid=new_pid)

        def cas(cluster_name, *, observed, replacement):
            assert cluster_name == 'test-cluster'
            assert observed is tunnel_state.observed
            assert replacement[:2] == (self.INITIAL_TUNNEL_PORT + 2, new_pid)
            assert str(uuid.UUID(replacement[2])) == replacement[2]
            events.append('cas')
            return outcome

        def register(generation, pid):
            assert str(uuid.UUID(generation)) == generation
            assert pid == new_pid
            events.append('register')

        def terminate(tunnel):
            events.append('kill-old' if tunnel is old_tunnel else 'kill-new')

        with patch.object(handle,
                          'get_command_runners',
                          return_value=[object()]), patch.object(
                              cloud_vm_ray_backend.random,
                              'randint',
                              return_value=self.INITIAL_TUNNEL_PORT + 2), \
                patch.object(cloud_vm_ray_backend.backend_utils,
                             'open_ssh_tunnel',
                             side_effect=open_tunnel), patch.object(
                                 cloud_vm_ray_backend,
                                 '_register_skylet_tunnel_process',
                                 side_effect=register), patch(
                                 'grpc.insecure_channel',
                                 return_value=object()), patch(
                                     'grpc.channel_ready_future',
                                     return_value=_ReadyFuture()), patch.object(
                                         global_user_state,
                                         'compare_and_set_cluster_skylet_ssh_tunnel_metadata',
                                         side_effect=cas), patch.object(
                                             handle,
                                             '_terminate_ssh_tunnel_process',
                                             side_effect=terminate):
            result = handle._open_and_update_skylet_tunnel(  # pylint: disable=protected-access
                tunnel_state)

        assert events == expected_events
        if outcome is skylet_transport.TunnelMutationResult.UPDATED:
            assert isinstance(result, SSHTunnelInfo)
            assert result.pid == new_pid
        else:
            assert result is outcome

    @pytest.mark.parametrize('outcome, expected_events', [
        (skylet_transport.TunnelMutationResult.UPDATED, ['cas', 'kill']),
        (skylet_transport.TunnelMutationResult.CONFLICT, ['cas']),
    ])
    def test_nonnull_tunnel_close_cas_owns_process_order(
            self, outcome, expected_events):
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                               pid=self.INITIAL_TUNNEL_PID,
                               generation=str(uuid.UUID(int=16)))
        tunnel_state = self._tunnel_state(tunnel, cluster_hash='hash-close')
        events = []

        def cas(cluster_name, *, observed, replacement):
            assert cluster_name == 'test-cluster'
            assert observed is tunnel_state.observed
            assert replacement is None
            events.append('cas')
            return outcome

        def terminate(observed_tunnel):
            assert observed_tunnel is tunnel
            events.append('kill')

        with patch.object(
                handle, '_get_skylet_ssh_tunnel_state',
                return_value=tunnel_state), patch.object(
                    global_user_state,
                    'compare_and_set_cluster_skylet_ssh_tunnel_metadata',
                    side_effect=cas), patch.object(
                        handle,
                        '_terminate_ssh_tunnel_process',
                        side_effect=terminate):
            result = handle.close_skylet_ssh_tunnel()

        assert result is outcome
        assert events == expected_events

    def test_get_grpc_channel_deadline_preserves_retry_budget(self):
        """Each retry derives its timeout from one immutable deadline."""
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        clock = self._FakeClock()
        lock_timeouts = []
        open_attempts = 0

        class _AcquiredLock:
            """Context manager returned by an immediately acquired lock."""

            def acquire(self, blocking):
                assert not blocking
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                del exc_type, exc_value, traceback

        def get_lock(lock_id, timeout, **kwargs):
            del lock_id, kwargs
            lock_timeouts.append(timeout)
            return _AcquiredLock()

        def open_tunnel(tunnel_state):
            del tunnel_state
            nonlocal open_attempts
            open_attempts += 1
            clock.now += 1.0
            raise RuntimeError('tunnel startup failed')

        with patch.object(
                handle,
                '_get_skylet_ssh_tunnel_state',
                return_value=self._tunnel_state(None)), patch.object(
                    handle,
                    '_open_and_update_skylet_tunnel',
                    side_effect=open_tunnel), patch.object(
                        cloud_vm_ray_backend.backend_utils,
                        'CLUSTER_TUNNEL_LOCK_TIMEOUT_SECONDS',
                        3.0), patch.object(cloud_vm_ray_backend, 'time',
                                           clock), patch.object(
                                               cloud_vm_ray_backend.locks,
                                               'get_lock',
                                               side_effect=get_lock):
            with pytest.raises(RuntimeError,
                               match='Timeout waiting for gRPC channel'):
                handle.get_grpc_channel()

        assert open_attempts == 3
        assert lock_timeouts == [3.0, 2.0, 1.0]
        assert clock.now == 3.0

    def test_get_grpc_channel_deadline_clamps_reader_jitter(self):
        """Reader jitter cannot oversleep the residual lock budget."""
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        clock = self._FakeClock()

        class _ContendedLock:
            """Exclusive lock held by another tunnel creator."""

            def acquire(self, blocking):
                assert not blocking
                raise cloud_vm_ray_backend.locks.LockTimeout

        class _SharedLock:
            """Reader lock released with only a small budget remaining."""

            def acquire(self, blocking):
                assert blocking
                clock.now += 0.98

            def release(self):
                pass

        def get_lock(lock_id, timeout, *, shared_lock=False):
            del lock_id, timeout
            return _SharedLock() if shared_lock else _ContendedLock()

        with patch.object(
                handle,
                '_get_skylet_ssh_tunnel_state',
                return_value=self._tunnel_state(None)), patch.object(
                    cloud_vm_ray_backend.backend_utils,
                    'CLUSTER_TUNNEL_LOCK_TIMEOUT_SECONDS', 1.0), patch.object(
                        cloud_vm_ray_backend, 'time',
                        clock), patch.object(cloud_vm_ray_backend.random,
                                             'uniform',
                                             return_value=0.05), patch.object(
                                                 cloud_vm_ray_backend.locks,
                                                 'get_lock',
                                                 side_effect=get_lock):
            with pytest.raises(RuntimeError,
                               match='Timeout waiting for gRPC channel'):
                handle.get_grpc_channel()

        assert clock.sleeps == pytest.approx([0.02])
        assert clock.now == pytest.approx(1.0)

    def test_get_grpc_channel_rechecks_tunnel_after_exclusive_lock(self):
        """A late lock winner reuses a tunnel refreshed by another process."""
        handle = CloudVmRayResourceHandle(**self.MOCK_HANDLE_KWARGS)
        stale_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT,
                                     pid=self.INITIAL_TUNNEL_PID,
                                     generation=str(uuid.UUID(int=1)))
        fresh_tunnel = SSHTunnelInfo(port=self.INITIAL_TUNNEL_PORT + 1,
                                     pid=self.INITIAL_TUNNEL_PID + 1,
                                     generation=str(uuid.UUID(int=2)))

        class _AcquiredLock:

            def acquire(self, blocking):
                assert not blocking
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                del exc_type, exc_value, traceback

        open_tunnel = MagicMock()
        with patch.object(
                handle,
                '_get_skylet_ssh_tunnel_state',
                side_effect=[
                    self._tunnel_state(stale_tunnel),
                    self._tunnel_state(fresh_tunnel),
                ]), patch.object(
                    handle, '_open_and_update_skylet_tunnel',
                    open_tunnel), patch.object(
                        cloud_vm_ray_backend,
                        '_is_tunnel_healthy',
                        side_effect=[False, True]) as is_healthy, patch.object(
                            cloud_vm_ray_backend.locks,
                            'get_lock',
                            return_value=_AcquiredLock()), patch(
                                'grpc.insecure_channel',
                                side_effect=lambda addr, options: addr):
            snapshot = handle.get_grpc_channel_with_snapshot()

        assert snapshot.channel == f'localhost:{self.INITIAL_TUNNEL_PORT + 1}'
        assert snapshot.key == skylet_transport.SkyletChannelKeyV1(
            cluster_hash='cluster-hash',
            endpoint=f'localhost:{self.INITIAL_TUNNEL_PORT + 1}',
            tunnel_generation=fresh_tunnel.generation,
        )
        assert is_healthy.call_args_list == [
            call(stale_tunnel),
            call(fresh_tunnel),
        ]
        open_tunnel.assert_not_called()

    def test_get_grpc_channel_multiprocess_race_condition(self):
        """Test get_grpc_channel with multiple processes racing for tunnel creation."""
        tunnel_creation_count = multiprocessing.Value('i', 0)
        tunnel_port = multiprocessing.Value('i', -1)
        tunnel_pid = multiprocessing.Value('i', -1)

        num_processes = 5
        processes = []
        queue = multiprocessing.Queue()
        for _ in range(num_processes):
            p = multiprocessing.Process(
                target=self._simulate_process_get_grpc_channel,
                args=(queue, tunnel_creation_count, tunnel_port, tunnel_pid,
                      None))
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=self.PROCESS_JOIN_TIMEOUT_SECONDS)
            if p.is_alive():
                p.terminate()
                p.join()

        results = []
        while not queue.empty():
            results.append(queue.get())
        assert len(
            results
        ) == num_processes, f"Expected {num_processes} results, got {len(results)}"
        # All processes should get the same channel (localhost:10000).
        for item in results:
            assert item == f'localhost:{self.INITIAL_TUNNEL_PORT}', f"Failed: {item}"

        assert tunnel_creation_count.value == 1, f"Expected tunnel to be created exactly once, but was created {tunnel_creation_count.value} times"

        # Try again, this tests the case where the tunnel is already created.
        # This time, tunnel.port will be 10000, but the check should fail,
        # as our _socket_connect_side_effect will raise an error. So we
        # should invoke _open_and_update_skylet_tunnel again,
        # this time returning another port.
        for _ in range(num_processes):
            p = multiprocessing.Process(
                target=self._simulate_process_get_grpc_channel,
                args=(queue, tunnel_creation_count, tunnel_port, tunnel_pid,
                      self._socket_connect_side_effect))
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=self.PROCESS_JOIN_TIMEOUT_SECONDS)
            if p.is_alive():
                p.terminate()
                p.join()

        results = []
        while not queue.empty():
            results.append(queue.get())
        assert len(
            results
        ) == num_processes, f"Expected {num_processes} results, got {len(results)}"

        # All processes should get the same channel (localhost:10001).
        for i in range(num_processes):
            assert results[
                i] == f'localhost:{self.INITIAL_TUNNEL_PORT + 1}', f"Process {i} failed: {results[i]}"

        assert tunnel_creation_count.value == 2, f"Expected tunnel to be created exactly once, but was created {tunnel_creation_count.value} times"

    def test_setup_num_gpus(self, monkeypatch):
        """Test setup num GPUs."""
        test_task = task.Task(resources=resources.Resources(
            accelerators={'A100': 8}))
        monkeypatch.setattr(CloudVmRayResourceHandle, '__init__',
                            lambda self, *args, **kwargs: None)
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        assert backend._get_num_gpus(test_task) == 8


class TestIsMessageTooLong:
    """Tests for _is_message_too_long function."""

    @pytest.mark.parametrize(
        'returncode,message,expected',
        [
            # Valid matches with correct returncode
            (255, 'too long', True),
            (255, 'Argument list too long', True),
            (1, 'request-uri too large', True),
            (1, '414 Request-URI Too Large', True),
            (1, 'request header fields too large', True),
            (1, '431 Request Header Fields Too Large', True),
            # CloudFlare 400 Bad Request patterns
            (1, '400 bad request', True),
            (1, '400 Bad request', True),
            (1, '400 Bad Request', True),
            (1,
             'error: unable to upgrade connection: <html><body><h1>400 Bad request</h1>',
             True),
            # Case insensitivity
            (255, 'TOO LONG', True),
            (1, 'REQUEST HEADER FIELDS TOO LARGE', True),
            (1, '400 BAD REQUEST', True),
            # Wrong returncode
            (1, 'too long', False),
            (255, 'request-uri too large', False),
            (127, 'too long', False),
            (255, '400 bad request', False),
            # Wrong message
            (255, 'command not found', False),
            (1, 'some other error', False),
            (1, 'unable to upgrade connection', False),
            # Empty output
            (255, '', False),
        ])
    def test_detection_with_output(self, returncode, message, expected):
        """Test message detection with various returncode/message combinations."""
        assert cloud_vm_ray_backend._is_message_too_long(
            returncode, output=message) == expected

    def test_detection_with_file_path(self, tmp_path):
        """Test detection when reading from file."""
        log_file = tmp_path / "test.log"
        log_file.write_text("Error: command too long")
        assert cloud_vm_ray_backend._is_message_too_long(
            255, file_path=str(log_file))

        log_file.write_text("431 Request Header Fields Too Large")
        assert cloud_vm_ray_backend._is_message_too_long(
            1, file_path=str(log_file))

    def test_file_read_error_returns_true(self, tmp_path):
        """Test that file read errors return True for safety."""
        # Non-existent file
        assert cloud_vm_ray_backend._is_message_too_long(
            255, file_path="/nonexistent/file.log")

        # Unreadable file
        log_file = tmp_path / "unreadable.log"
        log_file.write_text("content")
        log_file.chmod(0o000)
        try:
            assert cloud_vm_ray_backend._is_message_too_long(
                255, file_path=str(log_file))
        finally:
            log_file.chmod(0o644)

    def test_requires_either_output_or_file_path(self):
        """Test that function requires either output or file_path."""
        with pytest.raises(AssertionError):
            cloud_vm_ray_backend._is_message_too_long(255)
        with pytest.raises(AssertionError):
            cloud_vm_ray_backend._is_message_too_long(255,
                                                      output="test",
                                                      file_path="/tmp/test")

    def test_partial_match_in_long_output(self):
        """Test that partial matches in longer messages are detected."""
        long_output = """Error executing command on remote server:
        bash: /usr/bin/ssh: Argument list too long
        Failed to run setup script"""
        assert cloud_vm_ray_backend._is_message_too_long(255,
                                                         output=long_output)

        http_error = "<html><h1>414 Request-URI Too Large</h1></html>"
        assert cloud_vm_ray_backend._is_message_too_long(1, output=http_error)

    def test_multiple_patterns_match_by_returncode(self):
        """Test that returncode determines which pattern to match."""
        mixed = "too long and request-uri too large"
        assert cloud_vm_ray_backend._is_message_too_long(255, output=mixed)
        assert cloud_vm_ray_backend._is_message_too_long(1, output=mixed)


class TestCloudVmRayBackendTeardownNoLock:
    """Tests for CloudVmRayBackend.teardown_no_lock() guards."""

    @staticmethod
    def _make_handle(cluster_name: str, cluster_yaml: str, has_ray: bool):

        class _FakeLaunchedResources:

            def __init__(self, cloud_obj):
                self.cloud = cloud_obj

            def assert_launchable(self):
                return self

        cloud = MagicMock()
        cloud.PROVISIONER_VERSION = (
            cloud_vm_ray_backend.clouds.ProvisionerVersion.SKYPILOT)
        launched_resources = _FakeLaunchedResources(cloud)

        handle = CloudVmRayResourceHandle(
            cluster_name=cluster_name,
            cluster_name_on_cloud=f'{cluster_name}-on-cloud',
            cluster_yaml=cluster_yaml,
            launched_nodes=1,
            launched_resources=launched_resources,
        )
        handle.provision_runtime_metadata = (
            cloud_vm_ray_backend.provision_common.ProvisionRuntimeMetadata(
                has_ray=has_ray))
        handle.close_skylet_ssh_tunnel = MagicMock()
        return handle

    def test_uses_refreshed_handle_to_avoid_stale_metadata(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        stale_handle = self._make_handle('test-cluster',
                                         '/tmp/stale.yaml',
                                         has_ray=True)
        refreshed_handle = self._make_handle('test-cluster',
                                             '/tmp/refreshed.yaml',
                                             has_ray=False)

        with patch(
                'sky.backends.cloud_vm_ray_backend.requests_lib.'
                'kill_cluster_requests'), patch(
                    'sky.backends.cloud_vm_ray_backend.backend_utils.'
                    'refresh_cluster_status_handle',
                    return_value=(
                        status_lib.ClusterStatus.UP, refreshed_handle)), patch(
                            'sky.backends.cloud_vm_ray_backend.'
                            'global_user_state.'
                            'get_cluster_yaml_dict',
                            return_value={'provider': {
                            }}) as (mock_get_yaml), patch(
                                'sky.backends.cloud_vm_ray_backend'
                                '.provisioner.teardown_cluster'), patch.object(
                                    backend,
                                    'post_teardown_cleanup'), patch.object(
                                        backend,
                                        'run_on_head') as mock_run_on_head:
            backend.teardown_no_lock(stale_handle,
                                     terminate=True,
                                     refresh_cluster_status=True)

        mock_run_on_head.assert_not_called()
        mock_get_yaml.assert_called_once_with(refreshed_handle.cluster_yaml)

    def test_expected_uuid_is_revalidated_after_refresh_before_provider_io(
            self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = self._make_handle('action-fenced', '/tmp/exact.yaml', False)
        record_uuid = uuid.UUID('11111111-1111-4111-8111-111111111111')
        snapshot = global_user_state.ClusterRecordIdentitySnapshot(
            cluster_name=handle.cluster_name,
            cluster_record_uuid=record_uuid,
            serialized_handle=b'exact-handle',
            handle=handle)

        with patch(
                'sky.backends.cloud_vm_ray_backend.requests_lib.'
                'kill_cluster_requests'), patch(
                    'sky.backends.cloud_vm_ray_backend.backend_utils.'
                    'refresh_cluster_status_handle',
                    return_value=(status_lib.ClusterStatus.UP, handle)), patch(
                        'sky.backends.cloud_vm_ray_backend.'
                        'global_user_state.'
                        'get_cluster_record_identity_snapshot',
                        return_value=snapshot) as snapshot_reader, patch(
                            'sky.backends.cloud_vm_ray_backend.pickle.dumps',
                            return_value=b'exact-handle'), patch(
                                'sky.backends.cloud_vm_ray_backend.'
                                'global_user_state.get_cluster_yaml_dict',
                                return_value={'provider': {}}), patch(
                                    'sky.backends.cloud_vm_ray_backend.'
                                    'provisioner.teardown_cluster'
                                ) as provider_teardown, patch.object(
                                    backend,
                                    'post_teardown_cleanup') as cleanup:
            backend.teardown_no_lock(
                handle,
                terminate=True,
                expected_cluster_record_uuid=str(record_uuid))

        assert snapshot_reader.call_args_list == [
            call(handle.cluster_name, str(record_uuid)),
            call(handle.cluster_name, str(record_uuid)),
        ]
        provider_teardown.assert_called_once()
        cleanup.assert_called_once_with(
            handle,
            True,
            False,
            True,
            expected_cluster_record_uuid=str(record_uuid))

    def test_expected_uuid_rejects_handle_change_before_provider_io(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = self._make_handle('action-conflict', '/tmp/exact.yaml', False)
        record_uuid = uuid.UUID('11111111-1111-4111-8111-111111111111')
        snapshot = global_user_state.ClusterRecordIdentitySnapshot(
            cluster_name=handle.cluster_name,
            cluster_record_uuid=record_uuid,
            serialized_handle=b'different-handle',
            handle=handle)

        with patch(
                'sky.backends.cloud_vm_ray_backend.requests_lib.'
                'kill_cluster_requests'), patch(
                    'sky.backends.cloud_vm_ray_backend.backend_utils.'
                    'refresh_cluster_status_handle',
                    return_value=(status_lib.ClusterStatus.UP, handle)), patch(
                        'sky.backends.cloud_vm_ray_backend.'
                        'global_user_state.'
                        'get_cluster_record_identity_snapshot',
                        return_value=snapshot), patch(
                            'sky.backends.cloud_vm_ray_backend.pickle.dumps',
                            return_value=b'exact-handle'), patch(
                                'sky.backends.cloud_vm_ray_backend.'
                                'provisioner.teardown_cluster'
                            ) as provider_teardown:
            with pytest.raises(
                    global_user_state.ClusterRecordHandleChangedError,
                    match='handle changed'):
                backend.teardown_no_lock(
                    handle,
                    terminate=True,
                    expected_cluster_record_uuid=str(record_uuid))

        provider_teardown.assert_not_called()

    def test_expected_hash_rejects_replacement_before_any_effect(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = self._make_handle('legacy-conflict', '/tmp/exact.yaml', False)

        with patch(
                'sky.backends.cloud_vm_ray_backend.global_user_state.'
                'get_handle_from_cluster_name',
                return_value=None), patch(
                    'sky.backends.cloud_vm_ray_backend.requests_lib.'
                    'kill_cluster_requests') as kill_requests, patch(
                        'sky.backends.cloud_vm_ray_backend.backend_utils.'
                        'refresh_cluster_status_handle') as refresh, patch(
                            'sky.backends.cloud_vm_ray_backend.provisioner.'
                            'teardown_cluster') as provider_teardown:
            with pytest.raises(
                    global_user_state.ClusterRecordIdentityConflictError,
                    match='no longer has expected generation'):
                backend.teardown_no_lock(handle,
                                         terminate=True,
                                         expected_cluster_hash='generation-a')

        handle.close_skylet_ssh_tunnel.assert_not_called()
        kill_requests.assert_not_called()
        refresh.assert_not_called()
        provider_teardown.assert_not_called()

    def test_expected_uuid_rotation_while_waiting_for_locks_has_no_effect(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = self._make_handle('action-rotated', '/tmp/exact.yaml', False)
        record_uuid = '11111111-1111-4111-8111-111111111111'
        status_lock = MagicMock()
        resource_lock = MagicMock()
        conflict = global_user_state.ClusterRecordIdentityConflictError(
            'rotated action UUID')

        with patch('sky.backends.cloud_vm_ray_backend.locks.get_lock',
                   side_effect=[status_lock, resource_lock]), patch(
                       'sky.backends.cloud_vm_ray_backend.global_user_state.'
                       'get_cluster_record_identity_snapshot',
                       side_effect=conflict
                   ), patch(
                       'sky.backends.cloud_vm_ray_backend.requests_lib.'
                       'kill_cluster_requests') as kill_requests, patch(
                           'sky.backends.cloud_vm_ray_backend.backend_utils.'
                           'check_owner_identity') as owner_check, patch.object(
                               backend, 'teardown_no_lock') as teardown_no_lock:
            with pytest.raises(
                    global_user_state.ClusterRecordIdentityConflictError,
                    match='rotated action UUID'):
                backend._teardown(  # pylint: disable=protected-access
                    handle,
                    terminate=True,
                    expected_cluster_record_uuid=record_uuid)

        kill_requests.assert_not_called()
        owner_check.assert_not_called()
        teardown_no_lock.assert_not_called()
        status_lock.force_unlock.assert_not_called()

    def test_guard_rejection_under_locks_has_no_effect(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = self._make_handle('owner-rotated', '/tmp/exact.yaml', False)
        status_lock = MagicMock()
        resource_lock = MagicMock()
        guard = MagicMock(return_value=False)

        with patch('sky.backends.cloud_vm_ray_backend.locks.get_lock',
                   side_effect=[status_lock, resource_lock]), patch(
                       'sky.backends.cloud_vm_ray_backend.global_user_state.'
                       'get_handle_from_cluster_name',
                       return_value=handle
                   ), patch(
                       'sky.backends.cloud_vm_ray_backend.requests_lib.'
                       'kill_cluster_requests') as kill_requests, patch(
                           'sky.backends.cloud_vm_ray_backend.backend_utils.'
                           'check_owner_identity') as owner_check, patch.object(
                               backend, 'teardown_no_lock') as teardown_no_lock:
            with pytest.raises(RuntimeError,
                               match='continuation guard rejected'):
                backend._teardown(  # pylint: disable=protected-access
                    handle,
                    terminate=True,
                    expected_cluster_hash='generation-a',
                    continue_guard=guard)

        guard.assert_called_once_with()
        kill_requests.assert_not_called()
        owner_check.assert_not_called()
        teardown_no_lock.assert_not_called()
        status_lock.force_unlock.assert_not_called()

    def test_hash_rotation_during_refresh_blocks_provider_teardown(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = self._make_handle('refresh-rotated', '/tmp/exact.yaml', False)

        with patch(
                'sky.backends.cloud_vm_ray_backend.global_user_state.'
                'get_handle_from_cluster_name',
                side_effect=[handle, None]), patch(
                    'sky.backends.cloud_vm_ray_backend.backend_utils.'
                    'refresh_cluster_status_handle',
                    return_value=(None, None)), patch(
                        'sky.backends.cloud_vm_ray_backend.requests_lib.'
                        'kill_cluster_requests') as kill_requests, patch(
                            'sky.backends.cloud_vm_ray_backend.provisioner.'
                            'teardown_cluster') as provider_teardown, \
             patch.object(backend,
                          'post_teardown_cleanup') as post_cleanup:
            with pytest.raises(
                    global_user_state.ClusterRecordIdentityConflictError,
                    match='no longer has expected generation'):
                backend.teardown_no_lock(handle,
                                         terminate=True,
                                         expected_cluster_hash='generation-a')

        kill_requests.assert_not_called()
        provider_teardown.assert_not_called()
        post_cleanup.assert_not_called()


class TestCloudVmRayBackendLockedProvision:
    """Regression tests for legacy provisioner result handling."""

    def test_legacy_result_without_config_hash(self, monkeypatch):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        backend.log_dir = '/tmp/sky-test'
        launched_resources = MagicMock(zone='us-east-1a')
        handle = MagicMock(launched_resources=launched_resources,
                           external_ips=MagicMock(return_value=['1.2.3.4']),
                           external_ssh_ports=MagicMock(return_value=[22]))
        to_provision_config = MagicMock(
            resources=MagicMock(),
            num_nodes=1,
            prev_cluster_status=None,
            prev_handle=None,
        )
        provisioner = MagicMock()
        provisioner.release_fresh_provision_evidence_lease.return_value = None
        provisioner.provision_with_retries.return_value = {
            'provisioning_skipped': False,
            'ray': '/tmp/cluster.yaml',
            'handle': handle,
            'cluster_hash': 'generation-hash',
        }

        monkeypatch.setattr(backend, '_check_existing_cluster',
                            MagicMock(return_value=to_provision_config))
        monkeypatch.setattr(backend, '_maybe_clear_external_cluster_failures',
                            MagicMock())
        monkeypatch.setattr(backend, 'check_skylet_running', MagicMock())
        update_after_provisioned = MagicMock()
        monkeypatch.setattr(backend, '_update_after_cluster_provisioned',
                            update_after_provisioned)
        monkeypatch.setattr(cloud_vm_ray_backend.wheel_utils, 'build_sky_wheel',
                            MagicMock(return_value=('/tmp/sky.whl', 'hash')))
        monkeypatch.setattr(cloud_vm_ray_backend, 'RetryingVmProvisioner',
                            MagicMock(return_value=provisioner))
        monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                            'get_cluster_yaml_dict',
                            MagicMock(return_value={'provider': {}}))
        monkeypatch.setattr(cloud_vm_ray_backend.rich_utils,
                            'force_update_status', MagicMock())
        monkeypatch.setattr(cloud_vm_ray_backend.lock_events,
                            'DistributedLockEvent',
                            MagicMock(return_value=MagicMock()))
        monkeypatch.setattr(cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_cluster_resources', MagicMock())
        monkeypatch.setattr(cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_cluster_status', MagicMock())

        task_obj = MagicMock(resources={MagicMock()})
        result = backend._locked_provision(  # pylint: disable=protected-access
            'lock-id', task_obj, MagicMock(), False, False, 'test-cluster')

        assert result == (handle, False)
        update_after_provisioned.assert_called_once_with(
            handle, None, task_obj, None, None, 'generation-hash')

    def test_ready_transition_is_fenced_by_generation(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        handle = MagicMock(cluster_name='test-cluster', launched_nodes=1)
        handle.launched_resources.ports = None
        handle.provision_runtime_metadata.has_job_queue = False
        handle.provision_runtime_metadata.ssh_available = False
        task_obj = MagicMock(resources=set())
        task_obj.to_yaml_config.return_value = {'run': 'echo ok'}

        with patch.object(
                cloud_vm_ray_backend.global_user_state,
                'add_or_update_cluster') as add_or_update, patch.object(
                    cloud_vm_ray_backend.global_user_state,
                    'add_cluster_event') as add_event, patch.object(
                        cloud_vm_ray_backend.usage_lib.messages.usage,
                        'update_cluster_resources'), patch.object(
                            cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_final_cluster_status'):
            backend._update_after_cluster_provisioned(  # pylint: disable=protected-access
                handle,
                prev_handle=None,
                task=task_obj,
                prev_cluster_status=None,
                config_hash=None,
                cluster_hash='generation-hash')

        add_or_update.assert_called_once_with(
            'test-cluster',
            handle,
            set(),
            ready=True,
            config_hash=None,
            task_config={'run': 'echo ok'},
            existing_cluster_hash='generation-hash')
        assert add_event.call_args.kwargs['existing_cluster_hash'] == (
            'generation-hash')

    @pytest.mark.parametrize(
        ('prev_cluster_status', 'prev_ports', 'current_ports',
         'open_ports_version', 'expected_calls'), [
             (None, None, ['8080'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.UPDATABLE, 1),
             (status_lib.ClusterStatus.UP, ['8080'], ['8080'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.RECONCILABLE, 0),
             (status_lib.ClusterStatus.UP, ['8080'], ['8080', '8081'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.RECONCILABLE, 1),
             (status_lib.ClusterStatus.INIT, ['8080'], ['8080'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.RECONCILABLE, 1),
             (status_lib.ClusterStatus.INIT, ['8080'], ['8080'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.UPDATABLE, 0),
             (status_lib.ClusterStatus.STOPPED, ['8080'], ['8080'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.RECONCILABLE, 0),
             (status_lib.ClusterStatus.INIT, [], [],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.RECONCILABLE, 0),
             (status_lib.ClusterStatus.INIT, ['8080'], ['8080'],
              cloud_vm_ray_backend.clouds.OpenPortsVersion.LAUNCH_ONLY, 0),
         ])
    def test_ready_transition_reconciles_ports(self, prev_cluster_status,
                                               prev_ports, current_ports,
                                               open_ports_version,
                                               expected_calls):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        launched_resources = MagicMock(spec=resources.Resources)
        launched_resources.ports = current_ports
        launched_resources.cloud = MagicMock(
            OPEN_PORTS_VERSION=open_ports_version)
        launched_resources.assert_launchable.return_value = launched_resources
        handle = MagicMock(cluster_name='test-cluster',
                           launched_nodes=1,
                           launched_resources=launched_resources)
        handle.provision_runtime_metadata.has_job_queue = False
        handle.provision_runtime_metadata.ssh_available = False

        prev_handle = None
        if prev_ports is not None:
            prev_handle = MagicMock()
            prev_handle.launched_resources.ports = prev_ports

        task_obj = MagicMock(resources=set())
        task_obj.to_yaml_config.return_value = {'run': 'echo ok'}

        with patch.object(backend, '_open_ports') as open_ports, patch.object(
                cloud_vm_ray_backend.global_user_state,
                'add_or_update_cluster'), patch.object(
                    cloud_vm_ray_backend.global_user_state,
                    'add_cluster_event'), patch.object(
                        cloud_vm_ray_backend.usage_lib.messages.usage,
                        'update_cluster_resources'), patch.object(
                            cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_final_cluster_status'):
            backend._update_after_cluster_provisioned(  # pylint: disable=protected-access
                handle,
                prev_handle=prev_handle,
                task=task_obj,
                prev_cluster_status=prev_cluster_status,
                config_hash=None,
                cluster_hash='generation-hash')

        assert open_ports.call_count == expected_calls


class TestPostTeardownCleanupYamlFetch:
    """The teardown double-check loop must not re-read the cluster YAML."""

    def test_yaml_fetched_once_across_status_retries(self):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        # pylint: disable-next=protected-access
        handle = TestCloudVmRayBackendTeardownNoLock._make_handle(
            'test-cluster', '/tmp/cluster.yaml', has_ray=False)

        # Instances still show as UP for two attempts, then are gone.
        query_results = [
            {
                'node-0': (status_lib.ClusterStatus.UP, None)
            },
            {
                'node-0': (status_lib.ClusterStatus.UP, None)
            },
            {},
        ]

        with patch(
                'sky.backends.cloud_vm_ray_backend.cluster_utils.'
                'SSHConfigHelper.remove_cluster'), patch(
                    'sky.backends.cloud_vm_ray_backend.global_user_state.'
                    'get_cluster_yaml_dict',
                    return_value={'provider': {}}) as mock_get_yaml, patch(
                        'sky.backends.cloud_vm_ray_backend.provision_lib.'
                        'query_instances',
                        side_effect=query_results) as mock_query, patch(
                            'sky.backends.cloud_vm_ray_backend.'
                            'global_user_state.remove_cluster'), patch(
                                'sky.backends.cloud_vm_ray_backend.time.sleep'):
            backend.post_teardown_cleanup(handle, terminate=False)

        assert mock_query.call_count == 3
        mock_get_yaml.assert_called_once_with(handle.cluster_yaml)


class TestNewHandleRuntimeMetadata:
    """Runtime metadata a freshly constructed handle starts with."""

    def test_new_handle_has_no_runtime_established(self):
        """A new handle is created before provisioning, so it claims no Ray.

        Otherwise teardown of a cluster that crashed or recovered during
        provisioning attempts ``ray stop`` on a runtime that was never set
        up.
        """
        handle = CloudVmRayResourceHandle(
            cluster_name='test-cluster',
            cluster_name_on_cloud='test-cluster-abc',
            cluster_yaml=None,
            launched_nodes=1,
            launched_resources=MagicMock(),
        )
        metadata = handle.provision_runtime_metadata
        assert (metadata.has_ray, metadata.has_skylet, metadata.has_job_queue,
                metadata.ssh_available) == (False, False, False, False)
