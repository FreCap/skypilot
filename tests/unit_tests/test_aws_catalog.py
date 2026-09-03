"""Tests for account-specific AWS catalog behavior."""

# pylint: disable=protected-access

import os

import pandas as pd

from sky import clouds
from sky import resources as resources_lib
from sky.catalog import aws_catalog
from sky.catalog import common as catalog_common
from sky.utils import annotations


def _mapping(zone_id: str, zone_name: str) -> pd.DataFrame:
    return pd.DataFrame({
        'AvailabilityZone': [zone_id],
        'AvailabilityZoneName': [zone_name],
    })


def _images(rows: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame({
        'Tag': ['skypilot:custom-gpu-ubuntu-cuda13'] * len(rows),
        'Region': [region for region, _ in rows],
        'ImageId': [image_id for _, image_id in rows],
    })


def test_image_lookup_refreshes_once_and_rejects_placeholder(monkeypatch):
    stale = _images([('placeholder', 'NEED_FALLBACK')])
    fresh = _images([
        ('new-region', 'ami-0123456789abcdef0'),
        ('placeholder', 'NEED_FALLBACK'),
    ])
    refreshes = []
    monkeypatch.setattr(aws_catalog, '_image_df', stale)
    monkeypatch.setattr(
        aws_catalog.common, 'read_catalog',
        lambda filename, pull_frequency_hours: refreshes.append(
            (filename, pull_frequency_hours)) or fresh)
    aws_catalog._fresh_image_catalog.cache_clear()

    try:
        assert aws_catalog.get_image_id_from_tag(
            'skypilot:custom-gpu-ubuntu-cuda13',
            'new-region') == 'ami-0123456789abcdef0'
        assert not aws_catalog.is_image_tag_valid(
            'skypilot:custom-gpu-ubuntu-cuda13', 'placeholder')
        assert aws_catalog.get_image_id_from_tag(
            'skypilot:custom-gpu-ubuntu-cuda13', 'missing') is None
        assert refreshes == [('aws/images.csv', 0)]
    finally:
        aws_catalog._fresh_image_catalog.cache_clear()


def _lazy_images(tmp_path, name: str, rows: list[tuple[str, str]],
                 downloads: list[str]) -> catalog_common.LazyDataFrame:
    """A catalog frame that, like ``pull_frequency_hours=0``, is always stale.

    Every request-scoped load records one simulated download.
    """
    path = tmp_path / f'{name}.csv'
    _images(rows).to_csv(path, index=False)

    def _always_stale() -> bool:
        downloads.append(name)
        return True

    return catalog_common.LazyDataFrame(str(path),
                                        update_if_stale_func=_always_stale)


def test_image_miss_does_not_refetch_on_later_hit_requests(
        tmp_path, monkeypatch):
    tag = 'skypilot:custom-gpu-ubuntu-cuda13'
    downloads: list[str] = []
    stale = _images([('present', 'ami-0123456789abcdef0')])
    fresh_rows = [('present', 'ami-0123456789abcdef0'),
                  ('missing', 'NEED_FALLBACK')]
    monkeypatch.setattr(aws_catalog, '_image_df', stale)
    monkeypatch.setattr(
        aws_catalog.common, 'read_catalog',
        lambda filename, pull_frequency_hours: _lazy_images(
            tmp_path, 'fresh', fresh_rows, downloads))
    annotations.clear_request_level_cache()

    try:
        # Request 1 misses: exactly one forced download.
        assert aws_catalog.get_image_id_from_tag(tag, 'missing') is None
        assert downloads == ['fresh']
        # Requests 2 and 3 only hit the regular catalog: no download.
        for _ in range(2):
            annotations.clear_request_level_cache()
            assert aws_catalog.get_image_id_from_tag(
                tag, 'present') == 'ami-0123456789abcdef0'
        assert downloads == ['fresh']
        # A later miss still refreshes, once per request.
        annotations.clear_request_level_cache()
        assert aws_catalog.get_image_id_from_tag(tag, 'missing') is None
        assert aws_catalog.get_image_id_from_tag(tag, 'absent') is None
        assert downloads == ['fresh', 'fresh']
    finally:
        annotations.clear_request_level_cache()


def test_regionless_tag_is_valid_when_any_region_has_an_ami(monkeypatch):
    tag = 'skypilot:custom-gpu-ubuntu-cuda13'
    refreshes = []
    stale = _images([('us-east-1', 'ami-0123456789abcdef0'),
                     ('us-west-2', 'ami-0fedcba9876543210'),
                     ('placeholder', 'NEED_FALLBACK')])
    monkeypatch.setattr(aws_catalog, '_image_df', stale)
    monkeypatch.setattr(
        aws_catalog.common, 'read_catalog', lambda filename,
        pull_frequency_hours: refreshes.append(filename) or stale)
    aws_catalog._fresh_image_catalog.cache_clear()

    try:
        assert aws_catalog.is_image_tag_valid(tag, None)
        assert aws_catalog.is_image_tag_valid(tag, 'us-west-2')
        assert not aws_catalog.is_image_tag_valid(tag, 'placeholder')
        assert not aws_catalog.is_image_tag_valid('skypilot:unknown', None)
        # Both misses share the one per-request refresh.
        assert refreshes == ['aws/images.csv']
    finally:
        aws_catalog._fresh_image_catalog.cache_clear()


def test_regionless_skypilot_image_tag_validates_on_aws(monkeypatch):
    tag = 'skypilot:custom-gpu-ubuntu-cuda13'
    monkeypatch.setattr(
        aws_catalog, '_image_df',
        _images([('us-east-1', 'ami-0123456789abcdef0'),
                 ('us-west-2', 'ami-0fedcba9876543210')]))
    monkeypatch.setattr(clouds.AWS, 'get_image_size',
                        classmethod(lambda cls, image_id, region: 1.0))

    resources = resources_lib.Resources(cloud=clouds.AWS(), image_id=tag)
    resources.validate()

    assert resources.image_id == {None: tag}


def test_refreshes_az_mapping_after_vm_catalog_update(tmp_path, monkeypatch):
    vms_path = tmp_path / 'vms.csv'
    mapping_path = tmp_path / 'az_mappings-user.csv'
    md5_path = tmp_path / 'az_mappings-user.md5'
    vms_path.write_text('catalog', encoding='utf-8')
    _mapping('use1-az1', 'us-east-1a').to_csv(mapping_path, index=False)
    os.utime(mapping_path, (1, 1))
    os.utime(vms_path, (2, 2))

    def _catalog_path(filename):
        if filename == 'aws/vms.csv':
            return str(vms_path)
        if filename == 'aws/az_mappings-user.csv':
            return str(mapping_path)
        assert filename == '.meta/aws/az_mappings-user.csv.md5'
        return str(md5_path)

    refreshed = _mapping('euc2-az2', 'eu-central-2b')
    monkeypatch.setattr(aws_catalog.common, 'get_catalog_path', _catalog_path)
    monkeypatch.setattr(aws_catalog.fetch_aws,
                        'fetch_availability_zone_mappings', lambda: refreshed)

    result = aws_catalog._get_az_mappings('user')

    pd.testing.assert_frame_equal(result, refreshed)
    pd.testing.assert_frame_equal(pd.read_csv(mapping_path), refreshed)
    assert md5_path.read_text(encoding='utf-8')
    assert mapping_path.stat().st_mtime >= vms_path.stat().st_mtime


def test_keeps_newer_az_mapping_without_refetch(tmp_path, monkeypatch):
    vms_path = tmp_path / 'vms.csv'
    mapping_path = tmp_path / 'az_mappings-user.csv'
    md5_path = tmp_path / 'az_mappings-user.md5'
    vms_path.write_text('catalog', encoding='utf-8')
    existing = _mapping('euc2-az2', 'eu-central-2b')
    existing.to_csv(mapping_path, index=False)
    os.utime(vms_path, (1, 1))
    os.utime(mapping_path, (2, 2))

    def _catalog_path(filename):
        if filename == 'aws/vms.csv':
            return str(vms_path)
        if filename == 'aws/az_mappings-user.csv':
            return str(mapping_path)
        assert filename == '.meta/aws/az_mappings-user.csv.md5'
        return str(md5_path)

    monkeypatch.setattr(aws_catalog.common, 'get_catalog_path', _catalog_path)

    def _unexpected_fetch():
        raise AssertionError('fresh mapping must not be fetched')

    monkeypatch.setattr(aws_catalog.fetch_aws,
                        'fetch_availability_zone_mappings', _unexpected_fetch)

    result = aws_catalog._get_az_mappings('user')

    pd.testing.assert_frame_equal(result, existing)
