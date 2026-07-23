"""Tests for the short-lived capacity hints."""
# pylint: disable=protected-access
import json

import pytest
import sqlalchemy

from sky.provision import capacity_cache
from sky.utils.db import kv_cache


def _key(**overrides):
    values = {
        'cloud': 'aws',
        'account': '0001',
        'region': 'us-east-1',
        'zone': 'us-east-1a',
        'instance_type': 'g6.4xlarge',
        'accelerators': 'L4:1',
        'num_nodes': 1,
    }
    values.update(overrides)
    return capacity_cache.ResourceKey(**values)


def _quota_key(**overrides):
    values = {
        'cloud': 'aws',
        'account': '0001',
        'region': 'us-east-1',
        'instance_type': 'g6.4xlarge',
        'accelerators': 'L4:1',
        'num_nodes': 1,
    }
    values.update(overrides)
    return capacity_cache.QuotaCooldownKey(**values)


@pytest.fixture(name='cache_db')
def _cache_db(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "capacity_cache.db"}')
    monkeypatch.setattr(kv_cache._db_manager, '_engine', engine)
    kv_cache.Base.metadata.create_all(engine)
    return engine


def test_resource_dimensions_are_part_of_key():
    base = capacity_cache._cache_key(_key())
    assert base != capacity_cache._cache_key(_key(account='0002'))
    assert base != capacity_cache._cache_key(_key(zone='us-east-1b'))
    assert base != capacity_cache._cache_key(_key(instance_type='g6.8xlarge'))
    assert base != capacity_cache._cache_key(_key(num_nodes=8))
    # A machine type does not always determine the accelerator, and one
    # cloud's exhaustion says nothing about another's.
    assert base != capacity_cache._cache_key(_key(accelerators='V100:1'))
    assert base != capacity_cache._cache_key(_key(cloud='gcp'))


def test_round_trip_and_ttl_expiry(cache_db, monkeypatch):
    del cache_db
    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])

    key = _key()
    capacity_cache.mark_exhausted(key)
    assert capacity_cache.active_exhausted_keys([key]) == {key}

    now['value'] += capacity_cache._CAPACITY_TTL_SECONDS + 1
    assert capacity_cache.active_exhausted_keys([key]) == set()


def test_mark_never_shortens_expiry(cache_db, monkeypatch):
    del cache_db
    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])

    key = _key()
    capacity_cache.mark_exhausted(key)
    now['value'] = 1100.0
    capacity_cache.mark_exhausted(key)
    # Simulate an older observation committing after the newer mark.
    now['value'] = 1050.0
    capacity_cache.mark_exhausted(key)

    now['value'] = 1200.0
    assert capacity_cache.active_exhausted_keys([key]) == {key}


def test_clear_removes_only_exact_capacity_key(cache_db):
    del cache_db
    zone_a = _key(zone='us-east-1a')
    zone_b = _key(zone='us-east-1b')
    capacity_cache.mark_exhausted(zone_a)
    capacity_cache.mark_exhausted(zone_b)

    capacity_cache.clear(zone_a)

    assert capacity_cache.active_exhausted_keys([zone_a, zone_b]) == {zone_b}


def test_quota_cooldown_round_trip_clear_and_ttl(cache_db, monkeypatch):
    del cache_db
    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])

    key = _quota_key()
    capacity_cache.mark_quota_failure(key)
    assert capacity_cache.is_quota_cooldown_active(key)

    capacity_cache.clear_quota_cooldown(key)
    assert not capacity_cache.is_quota_cooldown_active(key)

    capacity_cache.mark_quota_failure(key)
    now['value'] += capacity_cache._QUOTA_COOLDOWN_TTL_SECONDS + 1
    assert not capacity_cache.is_quota_cooldown_active(key)


def test_quota_dimensions_are_isolated():
    base = capacity_cache._quota_cooldown_cache_key(_quota_key())
    assert base != capacity_cache._quota_cooldown_cache_key(
        _quota_key(account='0002'))
    assert base != capacity_cache._quota_cooldown_cache_key(
        _quota_key(region='us-west-2'))
    assert base != capacity_cache._quota_cooldown_cache_key(
        _quota_key(instance_type='g6.8xlarge'))
    assert base != capacity_cache._quota_cooldown_cache_key(
        _quota_key(num_nodes=8))
    assert base != capacity_cache._quota_cooldown_cache_key(
        _quota_key(accelerators='V100:1'))
    assert base != capacity_cache._quota_cooldown_cache_key(
        _quota_key(cloud='gcp'))


def test_active_filters_candidates(monkeypatch):
    active = _key(zone='us-east-1a')
    inactive = _key(zone='us-east-1b')
    active_cache_key = capacity_cache._cache_key(active)
    monkeypatch.setattr(capacity_cache.kv_cache, 'get_cache_entry',
                        lambda key: '1' if key == active_cache_key else None)
    assert capacity_cache.active_exhausted_keys([active, inactive]) == {active}


def test_service_observation_is_exact_and_redacted(cache_db, monkeypatch):
    del cache_db
    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])
    key = _key()
    observation = capacity_cache.ServiceObservation('svc', 'hash-a')

    capacity_cache.mark_exhausted(key, observation)

    result = capacity_cache.active_service_observations('svc', 'hash-a')
    assert result == {
        'available': True,
        'hints': [{
            'kind': 'capacity',
            'cloud': 'aws',
            'region': 'us-east-1',
            'zone': 'us-east-1a',
            'instance_type': 'g6.4xlarge',
            'accelerators': 'L4:1',
            'num_nodes': 1,
            'observed_at': 1000.0,
            'expires_at': 1120.0,
        }],
        'truncated': False,
    }
    assert not capacity_cache.active_service_observations('svc',
                                                          'hash-b')['hints']
    assert not capacity_cache.active_service_observations(
        'other-svc', 'hash-a')['hints']
    assert '0001' not in json.dumps(result)


def test_service_observation_requires_active_canonical_hint(
        cache_db, monkeypatch):
    del cache_db
    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])
    key = _key()
    observation = capacity_cache.ServiceObservation('svc', 'hash-a')
    capacity_cache.mark_exhausted(key, observation)

    capacity_cache.clear(key)

    result = capacity_cache.active_service_observations('svc', 'hash-a')
    assert not result['hints']


def test_quota_observation_is_regional(cache_db, monkeypatch):
    del cache_db
    now = {'value': 1000.0}
    monkeypatch.setattr(capacity_cache.time, 'time', lambda: now['value'])
    monkeypatch.setattr(kv_cache.time, 'time', lambda: now['value'])
    observation = capacity_cache.ServiceObservation('svc', 'hash-a')

    capacity_cache.mark_quota_failure(_quota_key(), observation)

    hints = capacity_cache.active_service_observations('svc', 'hash-a')['hints']
    assert len(hints) == 1
    hint = hints[0]
    assert hint['kind'] == 'quota'
    assert hint['region'] == 'us-east-1'
    assert hint['zone'] is None
    assert hint['instance_type'] == 'g6.4xlarge'


def test_observation_never_carries_the_account(cache_db):
    """The account is absent from the stored value, not stripped on read."""
    del cache_db
    key = _key(account='secret-project')
    capacity_cache.mark_exhausted(
        key, capacity_cache.ServiceObservation('svc', 'hash-a'))

    stored = kv_cache.list_active_cache_entries_by_prefix(
        capacity_cache._service_observation_prefix(
            capacity_cache._CAPACITY_OBSERVATION_KEY_PREFIX, 'svc'), 10)
    assert stored
    for _, value, _ in stored:
        payload = json.loads(value)
        assert 'secret-project' not in json.dumps(payload['resource'])
        assert payload['resource']['cloud'] == 'aws'


def test_hints_from_multiple_clouds_are_returned_together(cache_db):
    """One prefix scan must surface every provider's hints for a service."""
    del cache_db
    observation = capacity_cache.ServiceObservation('svc', 'hash-a')
    capacity_cache.mark_exhausted(_key(cloud='aws'), observation)
    capacity_cache.mark_exhausted(
        _key(cloud='gcp', region='asia-northeast3', zone='asia-northeast3-b'),
        observation)

    hints = capacity_cache.active_service_observations('svc', 'hash-a')['hints']
    assert sorted(hint['cloud'] for hint in hints) == ['aws', 'gcp']


def test_no_stored_value_or_key_contains_the_account(cache_db):
    """The account must not appear in any stored key or value."""
    del cache_db
    secret = 'secret-project-1234'
    observation = capacity_cache.ServiceObservation('svc', 'hash-a')
    capacity_cache.mark_exhausted(_key(account=secret), observation)
    capacity_cache.mark_quota_failure(_quota_key(account=secret), observation)

    # Canonical keys are digests, so the identifier is absent from the key.
    assert secret not in capacity_cache._cache_key(_key(account=secret))
    assert secret not in capacity_cache._quota_cooldown_cache_key(
        _quota_key(account=secret))

    for prefix in (capacity_cache._CAPACITY_OBSERVATION_KEY_PREFIX,
                   capacity_cache._QUOTA_OBSERVATION_KEY_PREFIX):
        rows = kv_cache.list_active_cache_entries_by_prefix(
            capacity_cache._service_observation_prefix(prefix, 'svc'), 10)
        assert rows
        for key, value, _ in rows:
            assert secret not in key
            # canonical_key is embedded in the value, so this also covers it.
            assert secret not in value

    result = capacity_cache.active_service_observations('svc', 'hash-a')
    assert secret not in json.dumps(result)
    assert len(result['hints']) == 2
