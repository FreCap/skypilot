"""Tests for the AWS service-catalog fetcher."""

from unittest import mock

import pandas as pd

from sky.catalog.data_fetchers import fetch_aws


def test_new_gpu_regions_are_fetched() -> None:
    assert 'sa-east-1' in fetch_aws.ALL_REGIONS
    assert 'eu-central-2' in fetch_aws.ALL_REGIONS


def test_get_aws_gpu_fallback_image_row_uses_latest_image() -> None:
    client = mock.Mock()
    client.describe_images.return_value = {
        'Images': [
            {
                'ImageId': 'ami-old',
                'Name': ('Deep Learning Base OSS Nvidia Driver GPU AMI '
                         '(Ubuntu 22.04) 20260701'),
                'CreationDate': '2026-07-01T00:00:00.000Z',
            },
            {
                'ImageId': 'ami-new',
                'Name': ('Deep Learning Base OSS Nvidia Driver GPU AMI '
                         '(Ubuntu 22.04) 20260724'),
                'CreationDate': '2026-07-24T00:00:00.000Z',
            },
        ]
    }
    with mock.patch.object(fetch_aws.aws, 'client', return_value=client):
        row = fetch_aws._get_aws_gpu_fallback_image_row(  # pylint: disable=protected-access
            'sa-east-1')

    assert row == (
        'skypilot:custom-gpu-ubuntu-cuda13',
        'sa-east-1',
        'ubuntu',
        '22.04',
        'ami-new',
        '20260724',
        'ami-new',
    )
    client.describe_images.assert_called_once_with(
        Owners=['amazon'],
        Filters=[
            {
                'Name': 'name',
                'Values': [
                    'Deep Learning Base OSS Nvidia Driver GPU AMI '
                    '(Ubuntu 22.04) *'
                ],
            },
            {
                'Name': 'state',
                'Values': ['available'],
            },
            {
                'Name': 'architecture',
                'Values': ['x86_64'],
            },
        ],
    )


def test_merge_generated_images_prefers_curated_and_refreshes_fallback(
) -> None:
    existing = pd.DataFrame([
        {
            'Tag': 'skypilot:custom-gpu-ubuntu-cuda13',
            'Region': 'eu-central-2',
            'ImageId': 'ami-curated',
            'BaseImageId': 'ami-curated-base',
        },
        {
            'Tag': 'skypilot:custom-gpu-ubuntu-cuda13',
            'Region': 'sa-east-1',
            'ImageId': 'ami-fallback-old',
            'BaseImageId': 'ami-fallback-old',
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
    fallback = pd.DataFrame([
        {
            'Tag': 'skypilot:custom-gpu-ubuntu-cuda13',
            'Region': 'eu-central-2',
            'ImageId': 'ami-aws-eu',
            'BaseImageId': 'ami-aws-eu',
        },
        {
            'Tag': 'skypilot:custom-gpu-ubuntu-cuda13',
            'Region': 'sa-east-1',
            'ImageId': 'ami-fallback-new',
            'BaseImageId': 'ami-fallback-new',
        },
    ])

    result = fetch_aws.merge_generated_images(existing, generated, fallback)
    image_ids = set(result['ImageId'])

    assert image_ids == {
        'ami-curated',
        'ami-neuron-new',
        'ami-fallback-new',
    }
