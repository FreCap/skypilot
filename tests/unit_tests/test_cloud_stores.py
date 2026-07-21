"""Tests for cloud storage URL dispatch."""

import pytest

from sky import cloud_stores


def test_get_storage_from_path_rejects_unknown_scheme():
    with pytest.raises(AssertionError, match='Scheme unsupported not found'):
        cloud_stores.get_storage_from_path('unsupported://bucket/key')
