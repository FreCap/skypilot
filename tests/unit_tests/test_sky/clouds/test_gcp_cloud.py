"""Test the GCP class."""

from unittest import mock

import pytest

from sky.clouds import gcp as gcp_mod
from sky.utils import resources_utils


def test_failover_disk_tier_rejects_missing_fallback():
    with mock.patch.object(gcp_mod.GCP,
                           'check_disk_tier',
                           return_value=(False, 'unsupported')):
        with pytest.raises(AssertionError,
                           match='Low disk tier should always be supported'):
            gcp_mod.GCP.failover_disk_tier('n1-standard-4', None)


def test_create_image_has_managed_label():
    cluster_name = resources_utils.ClusterName(display_name='cluster',
                                               name_on_cloud='cluster-cloud')
    command_results = [
        (0, '[{"name": "instance-1"}]', ''),
        (0, '', ''),
        (0,
         'https://www.googleapis.com/compute/v1/projects/project/global/images/'
         'skypilot-cluster-123', ''),
    ]

    with mock.patch.object(gcp_mod.time, 'time', return_value=123), \
         mock.patch.object(
             gcp_mod.subprocess_utils,
             'run_with_retries',
             side_effect=command_results) as run_with_retries:
        image_id = gcp_mod.GCP.create_image_from_cluster(cluster_name,
                                                         region=None,
                                                         zone='us-central1-a')

    assert image_id == ('projects/project/global/images/skypilot-cluster-123')
    assert run_with_retries.call_args_list[1] == mock.call(
        'gcloud compute images create skypilot-cluster-123 '
        '--source-disk  instance-1 --source-disk-zone us-central1-a '
        '--labels=skypilot-managed=true',
        retry_returncode=[255],
    )


def test_create_image_checks_create_command_result():
    cluster_name = resources_utils.ClusterName(display_name='cluster',
                                               name_on_cloud='cluster-cloud')
    command_results = [
        (0, '[{"name": "instance-1"}]', ''),
        (17, '', 'missing compute.images.setLabels'),
        (0,
         'https://www.googleapis.com/compute/v1/projects/project/global/images/'
         'skypilot-cluster-123', ''),
    ]

    with mock.patch.object(gcp_mod.time, 'time', return_value=123), \
         mock.patch.object(
             gcp_mod.subprocess_utils,
             'run_with_retries',
             side_effect=command_results), \
         mock.patch.object(gcp_mod.subprocess_utils,
                           'handle_returncode') as handle_returncode:
        gcp_mod.GCP.create_image_from_cluster(cluster_name,
                                              region=None,
                                              zone='us-central1-a')

    create_command = ('gcloud compute images create skypilot-cluster-123 '
                      '--source-disk  instance-1 '
                      '--source-disk-zone us-central1-a '
                      '--labels=skypilot-managed=true')
    assert handle_returncode.call_args_list[1] == mock.call(
        17,
        create_command,
        error_msg="Failed to create image for 'cluster'",
        stderr='missing compute.images.setLabels',
        stream_logs=True)


class TestGetGpuImageId:
    """Tests for GCP._get_gpu_image_id GPU image selection."""

    @pytest.mark.parametrize('acc_name',
                             ['T4', 'A100', 'A100-80GB', 'L4', 'H100', 'B200'])
    def test_turing_and_later_uses_cuda13_default(self, acc_name):
        assert gcp_mod.GCP._get_gpu_image_id(
            acc_name) == gcp_mod._DEFAULT_GPU_IMAGE_ID

    @pytest.mark.parametrize('acc_name', ['V100', 'P100', 'P4', 'M60'])
    def test_pre_turing_uses_legacy_cuda12(self, acc_name):
        assert gcp_mod.GCP._get_gpu_image_id(
            acc_name) == gcp_mod._DEFAULT_GPU_CUDA12_IMAGE_ID

    def test_k80_uses_k80_image(self):
        assert gcp_mod.GCP._get_gpu_image_id(
            'K80') == gcp_mod._DEFAULT_GPU_K80_IMAGE_ID
