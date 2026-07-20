"""Test the GCP class."""

from unittest import mock

import pytest

from sky.clouds import gcp as gcp_mod


def test_failover_disk_tier_rejects_missing_fallback():
    with mock.patch.object(gcp_mod.GCP,
                           'check_disk_tier',
                           return_value=(False, 'unsupported')):
        with pytest.raises(AssertionError,
                           match='Low disk tier should always be supported'):
            gcp_mod.GCP.failover_disk_tier('n1-standard-4', None)


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
