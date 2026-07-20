"""Tests for the RunPod catalog fetcher."""
import pytest

from sky.catalog.data_fetchers import fetch_runpod


@pytest.mark.parametrize(
    'lowest_price, expected_message',
    [
        ({
            'minVcpu': 0,
            'minMemory': 64
        }, 'vCPUs must be a positive number, not 0'),
        ({
            'minVcpu': 4,
            'minMemory': 0
        }, 'Memory must be a positive number, not 0'),
    ],
)
def test_invalid_gpu_resources_render_value(capsys, lowest_price,
                                            expected_message):
    result = fetch_runpod.get_gpu_info('UnknownGPU', {
        'lowestPrice': lowest_price,
        'memoryInGb': 24,
    }, 1)

    assert result is None
    assert expected_message in capsys.readouterr().out
