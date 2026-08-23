"""Spec contract for the logical service-wide paid GPU-unit cap."""
from typing import Any, Dict

import pytest

from sky.serve import placement_policy
from sky.serve import service_spec as service_spec_lib


def _make_spec(**kwargs: Any) -> service_spec_lib.SkyServiceSpec:
    base: Dict[str, Any] = {
        'readiness_path': '/health',
        'initial_delay_seconds': 60,
        'readiness_timeout_seconds': 30,
        'endpoint_probe_interval_seconds': 10,
        'lb_stream_timeout_seconds': 60,
        'min_replicas': 0,
        'max_replicas': 10,
        'target_concurrency_per_replica': 1,
        'graceful_drain_async_occupancy': True,
    }
    base.update(kwargs)
    return service_spec_lib.SkyServiceSpec(**base)


def test_absent_paid_gpu_cap_is_unlimited_and_has_no_yaml_footprint():
    spec = _make_spec()

    assert spec.max_live_paid_gpu_units is None
    config = spec.to_yaml_config()
    assert 'max_live_paid_gpu_units' not in config['replica_policy']
    assert (service_spec_lib.SkyServiceSpec.from_yaml_config(
        config).max_live_paid_gpu_units is None)


@pytest.mark.parametrize('limit', [0, 1, 64])
def test_paid_gpu_cap_round_trips_and_copy_preserves_it(limit):
    spec = _make_spec(spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
                      max_live_paid_gpu_units=limit)

    config = spec.to_yaml_config()
    assert config['replica_policy']['max_live_paid_gpu_units'] == limit
    assert (service_spec_lib.SkyServiceSpec.from_yaml_config(
        config).max_live_paid_gpu_units == limit)
    assert spec.copy().max_live_paid_gpu_units == limit
    assert spec.copy(max_live_paid_gpu_units=7).max_live_paid_gpu_units == 7


@pytest.mark.parametrize('bad_value', [-1, True, 1.5, '1'])
def test_paid_gpu_cap_rejects_non_nonnegative_integers(bad_value):
    with pytest.raises(ValueError, match='must be an integer >= 0'):
        _make_spec(spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
                   max_live_paid_gpu_units=bad_value)


@pytest.mark.parametrize('spot_placer',
                         [None, placement_policy.SPOT_HEDGE_PLACER])
def test_paid_gpu_cap_requires_logical_per_gpu_placer(spot_placer):
    with pytest.raises(ValueError, match='dynamic_fallback_per_gpu'):
        _make_spec(spot_placer=spot_placer, max_live_paid_gpu_units=0)


@pytest.mark.parametrize('bad_value', [-1, True, 1.5, '1'])
def test_paid_gpu_cap_yaml_schema_rejects_invalid_values(bad_value):
    config = _make_spec(spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
                        max_live_paid_gpu_units=0).to_yaml_config()
    config['replica_policy']['max_live_paid_gpu_units'] = bad_value

    with pytest.raises(ValueError):
        service_spec_lib.SkyServiceSpec.from_yaml_config(config)


def test_old_pickled_spec_defaults_to_unlimited_paid_gpu_capacity():
    spec = _make_spec(spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
                      max_live_paid_gpu_units=0)
    old_state = dict(spec.__dict__)
    del old_state['_max_live_paid_gpu_units']
    restored = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)

    restored.__setstate__(old_state)

    assert restored.max_live_paid_gpu_units is None
