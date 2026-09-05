"""Generation-fence regressions for provisioning failure state cleanup."""

from unittest.mock import MagicMock
import uuid

import pytest

from sky.backends import cloud_vm_ray_backend


class TestProvisionFailureStateFence:
    """Checks failure cleanup against the provisioner's active generation."""

    @staticmethod
    def _run_failed_provision(monkeypatch,
                              cluster_hash,
                              *,
                              retry_until_up=False,
                              cluster_record_uuid=None):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        backend.log_dir = '/tmp/sky-test'
        to_provision_config = MagicMock(
            resources=MagicMock(),
            num_nodes=1,
            prev_cluster_status=None,
        )
        provisioner = MagicMock(active_cluster_hash=cluster_hash)
        provisioner.provision_with_retries.side_effect = (
            cloud_vm_ray_backend.exceptions.ResourcesUnavailableError(
                'capacity unavailable'))

        monkeypatch.setattr(backend, '_check_existing_cluster',
                            MagicMock(return_value=to_provision_config))
        monkeypatch.setattr(backend, '_maybe_clear_external_cluster_failures',
                            MagicMock())
        monkeypatch.setattr(cloud_vm_ray_backend.wheel_utils, 'build_sky_wheel',
                            MagicMock(return_value=('/tmp/sky.whl', 'hash')))
        monkeypatch.setattr(cloud_vm_ray_backend, 'RetryingVmProvisioner',
                            MagicMock(return_value=provisioner))
        monkeypatch.setattr(cloud_vm_ray_backend.rich_utils,
                            'force_update_status', MagicMock())
        monkeypatch.setattr(cloud_vm_ray_backend.lock_events,
                            'DistributedLockEvent',
                            MagicMock(return_value=MagicMock()))
        monkeypatch.setattr(cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_cluster_resources', MagicMock())
        monkeypatch.setattr(cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_cluster_status', MagicMock())
        monkeypatch.setattr(cloud_vm_ray_backend.usage_lib.messages.usage,
                            'update_final_cluster_status', MagicMock())
        add_event = MagicMock()
        remove_cluster = MagicMock()
        monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                            'add_cluster_event', add_event)
        monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                            'remove_cluster', remove_cluster)
        get_record = MagicMock()
        monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                            'get_cluster_record_identity_snapshot', get_record)
        monkeypatch.setattr(
            cloud_vm_ray_backend, '_bound_paid_cluster_record_identity_kwargs',
            MagicMock(return_value=({} if cluster_record_uuid is None else {
                'cluster_record_uuid': cluster_record_uuid,
            })))

        task_obj = MagicMock(num_nodes=1,
                             resources={MagicMock()},
                             blocked_resources=set())
        expected_error = (
            cloud_vm_ray_backend.exceptions.ExecutionRetryableError
            if retry_until_up else
            cloud_vm_ray_backend.exceptions.ResourcesUnavailableError)
        with pytest.raises(expected_error) as exc_info:
            backend._locked_provision(  # pylint: disable=protected-access
                'lock-id',
                task_obj,
                MagicMock(),
                False,
                False,
                'test-cluster',
                retry_until_up=retry_until_up)
        return add_event, remove_cluster, get_record, exc_info.value

    def test_terminal_failure_cleanup_is_fenced_by_generation(
            self, monkeypatch):
        add_event, remove_cluster, get_record, _ = self._run_failed_provision(
            monkeypatch, 'generation-hash')

        assert add_event.call_args.kwargs['existing_cluster_hash'] == (
            'generation-hash')
        remove_cluster.assert_called_once_with(
            'test-cluster',
            terminate=True,
            existing_cluster_hash='generation-hash')
        get_record.assert_not_called()

    def test_retry_event_is_fenced_by_generation(self, monkeypatch):
        add_event, remove_cluster, get_record, _ = self._run_failed_provision(
            monkeypatch, 'generation-hash', retry_until_up=True)

        assert add_event.call_args.kwargs['existing_cluster_hash'] == (
            'generation-hash')
        remove_cluster.assert_not_called()
        get_record.assert_not_called()

    def test_failure_before_generation_skips_name_based_cleanup(
            self, monkeypatch):
        add_event, remove_cluster, get_record, _ = self._run_failed_provision(
            monkeypatch, None)

        add_event.assert_not_called()
        remove_cluster.assert_not_called()
        get_record.assert_not_called()

    def test_action_aware_failure_retains_row_for_durable_finalization(
            self, monkeypatch):
        record_uuid = uuid.UUID('11111111-1111-4111-8111-111111111111')

        add_event, remove_cluster, get_record, error = (
            self._run_failed_provision(monkeypatch,
                                       'generation-hash',
                                       cluster_record_uuid=record_uuid))

        assert isinstance(
            error, cloud_vm_ray_backend.exceptions.ResourcesUnavailableError)
        assert 'capacity unavailable' in str(error)
        assert add_event.call_args.kwargs['existing_cluster_hash'] == (
            'generation-hash')
        get_record.assert_not_called()
        remove_cluster.assert_not_called()
