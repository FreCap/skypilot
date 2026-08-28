"""Tests for the AWS service-catalog fetcher."""

import pandas as pd

from sky.catalog.data_fetchers import fetch_aws


def test_sao_paulo_region_is_fetched() -> None:
    assert 'sa-east-1' in fetch_aws.ALL_REGIONS


def test_zurich_region_is_fetched() -> None:
    assert 'eu-central-2' in fetch_aws.ALL_REGIONS


def test_merge_generated_images_preserves_curated_images() -> None:
    existing = pd.DataFrame([
        {
            'Tag': 'skypilot:custom-gpu-ubuntu-cuda13',
            'Region': 'sa-east-1',
            'ImageId': 'ami-curated',
            'BaseImageId': 'ami-curated-base',
        },
        {
            'Tag': 'skypilot:neuron-ubuntu-2204',
            'Region': 'sa-east-1',
            'ImageId': 'ami-neuron-old',
            'BaseImageId': None,
        },
    ])
    generated = pd.DataFrame([
        {
            'Tag': 'skypilot:neuron-ubuntu-2204',
            'Region': 'sa-east-1',
            'ImageId': 'ami-neuron-new',
        },
    ])

    result = fetch_aws.merge_generated_images(existing, generated)
    image_ids = set(result['ImageId'])

    assert image_ids == {
        'ami-curated',
        'ami-neuron-new',
    }
