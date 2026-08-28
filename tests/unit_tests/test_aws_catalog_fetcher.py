"""Tests for the AWS service-catalog fetcher."""

from pathlib import Path
import runpy
import sys

import pandas as pd

from sky.catalog.data_fetchers import fetch_aws


def test_sao_paulo_region_is_fetched() -> None:
    assert 'sa-east-1' in fetch_aws.ALL_REGIONS


def test_zurich_region_is_fetched() -> None:
    assert 'eu-central-2' in fetch_aws.ALL_REGIONS


def test_malaysia_region_is_fetched() -> None:
    assert 'ap-southeast-5' in fetch_aws.ALL_REGIONS


def test_hyderabad_region_is_fetched() -> None:
    assert 'ap-south-2' in fetch_aws.ALL_REGIONS


def test_new_regions_are_curated_image_copy_targets(monkeypatch) -> None:
    image_gen_path = (Path(__file__).parents[2] / 'sky' / 'catalog' / 'images' /
                      'aws_utils' / 'image_gen.py')
    monkeypatch.setattr(
        sys, 'argv',
        [str(image_gen_path), '--image-id', 'ami-test', '--processor', 'gpu'])

    image_gen_globals = runpy.run_path(str(image_gen_path))

    assert {
        'eu-central-2',
        'sa-east-1',
        'ap-southeast-5',
        'ap-south-2',
    }.issubset(image_gen_globals['ALL_REGIONS'])


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
