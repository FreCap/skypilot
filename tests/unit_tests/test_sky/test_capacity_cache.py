"""Tests for the short-lived AWS capacity hints."""
# pylint: disable=protected-access
import sqlalchemy

from sky.provision import capacity_cache
from sky.utils.db import kv_cache


def _key(**overrides):
    values = {
        'account': '0001',
        'region': 'us-east-1',
        'zone': 'us-east-1a',
        'instance_type': 'g6.4xlarge',
        'num_nodes': 1,
    }
    values.update(overrides)
    return capacity_cache.ResourceKey(**values)


def test_resource_dimensions_are_part_of_key():
    base = capacity_cache._cache_key(_key())
    assert base != capacity_cache._cache_key(_key(account='0002'))
    assert base != capacity_cache._cache_key(_key(zone='us-east-1b'))
    assert base != capacity_cache._cache_key(_key(instance_type='g6.8xlarge'))
    assert base != capacity_cache._cache_key(_key(num_nodes=8))


def test_round_trip_and_ttl_expiry(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "capacity_cache.db"}')
    monkeypatch.setattr(kv_cache._db_manager, '_engine', engine)
    kv_cache.Base.metadata.create_all(engine)

    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])

    key = _key()
    capacity_cache.mark_exhausted(key)
    assert capacity_cache.active_exhausted_keys([key]) == {key}

    now['value'] += capacity_cache._CAPACITY_TTL_SECONDS + 1
    assert capacity_cache.active_exhausted_keys([key]) == set()


def test_active_filters_candidates(monkeypatch):
    active = _key(zone='us-east-1a')
    inactive = _key(zone='us-east-1b')
    active_cache_key = capacity_cache._cache_key(active)
    monkeypatch.setattr(capacity_cache.kv_cache, 'get_cache_entry',
                        lambda key: '1' if key == active_cache_key else None)
    assert capacity_cache.active_exhausted_keys([active, inactive]) == {active}
