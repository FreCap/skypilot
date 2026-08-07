"""Spec plumbing for target_concurrency_per_replica.

The knob is the third autoscaling signal (after target_qps_per_replica and
pool queue_length_threshold): per-GPU target concurrency, replica capacity =
knob * gpu_count. These tests pin the spec-level contract only — validation
gates, yaml/copy/pickle round-trips, and the status string path that runs on
every service record build.
"""
from typing import Any, Dict

import pytest

from sky.serve import placement_policy
from sky.serve import service_spec as service_spec_lib
from sky.serve import spot_placer


def _make_spec(**kwargs: Any) -> service_spec_lib.SkyServiceSpec:
    """Construct a spec with the minimal required fields."""
    base: Dict[str, Any] = {
        'readiness_path': '/health',
        'initial_delay_seconds': 60,
        'readiness_timeout_seconds': 30,
        'endpoint_probe_interval_seconds': 10,
        'lb_stream_timeout_seconds': 60,
        'min_replicas': 1,
    }
    base.update(kwargs)
    return service_spec_lib.SkyServiceSpec(**base)


def _remove_versioned_placement_contract(state: Dict[str, Any]) -> None:
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field, None)


def test_concurrency_knob_enables_autoscaling():
    # min != max without target_qps_per_replica used to be rejected; the
    # concurrency knob must open the same gate.
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=2.0)
    assert spec.target_concurrency_per_replica == 2.0
    assert spec.target_qps_per_replica is None


def test_both_knobs_rejected():
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_qps_per_replica=1.0,
                   target_concurrency_per_replica=2.0)


@pytest.mark.parametrize('bad_value', [0, -1, -0.5])
def test_non_positive_knob_rejected(bad_value):
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_concurrency_per_replica=bad_value)


def test_knob_requires_max_replicas():
    # Mirrors the target_qps_per_replica analog: without a ceiling the
    # autoscaler has no clip bound.
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1, target_concurrency_per_replica=2.0)


def test_knob_requires_load_tracking_policy():
    # round_robin carries no in-flight gauge, so the concurrency autoscaler
    # would be blind; must be rejected at spec load.
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_concurrency_per_replica=2.0,
                   load_balancing_policy='round_robin')
    # The default (None -> least_load) and explicit least_load both track.
    _make_spec(min_replicas=1,
               max_replicas=5,
               target_concurrency_per_replica=2.0,
               load_balancing_policy='least_load')


def test_knob_rejected_for_pool():
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_concurrency_per_replica=2.0,
                   pool=True)


def test_min_neq_max_without_any_knob_still_rejected():
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1, max_replicas=5)


@pytest.mark.parametrize('fallback_field', [
    {
        'dynamic_ondemand_fallback': True
    },
    {
        'base_ondemand_fallback_replicas': 1
    },
])
def test_knob_rejected_with_ondemand_fallback(fallback_field):
    # from_spec routes on-demand fallback to FallbackRequestRateAutoscaler,
    # which would silently ignore the concurrency knob (or the knob branch
    # would silently drop the spot-safety fallback). Reject at load, same
    # as the dict-qps + fallback combination.
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=2.0)
    config = spec.to_yaml_config()
    config['replica_policy'].update(fallback_field)
    with pytest.raises(ValueError):
        service_spec_lib.SkyServiceSpec.from_yaml_config(config)


def test_yaml_round_trip_preserves_knob():
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=2.0)
    config = spec.to_yaml_config()
    # from_yaml_config also runs the JSON schema, so this covers schema
    # acceptance of the new field.
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.target_concurrency_per_replica == 2.0
    assert restored.target_qps_per_replica is None
    assert restored.max_replicas == 5


def test_yaml_round_trip_preserves_demand_and_wave_policy():
    spec = _make_spec(
        min_replicas=1,
        max_replicas=1000,
        target_concurrency_per_replica=1,
        spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
        graceful_drain_async_occupancy=True,
        target_utilization_percentage=90,
        expected_request_duration_seconds=30,
        initial_provision_lead_time_seconds=540,
        adaptive_demand_estimation=False,
        max_scale_up_rate_percentage=20,
        scale_up_rate_min_replicas=10,
        scale_up_rate_period_seconds=60,
        adaptive_scale_up={
            'max_scale_up_rate_percentage': 100,
            'scale_up_rate_min_replicas': 50,
            'pressure_observations': 2,
            'hold_seconds': 120,
        },
        max_scale_down_rate_percentage=50,
    )

    config = spec.to_yaml_config()
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)

    assert restored.target_utilization_percentage == 90
    assert restored.expected_request_duration_seconds == 30
    assert restored.initial_provision_lead_time_seconds == 540
    assert restored.adaptive_demand_estimation is False
    assert restored.max_scale_up_rate_percentage == 20
    assert restored.scale_up_rate_min_replicas == 10
    assert restored.scale_up_rate_period_seconds == 60
    assert restored.adaptive_scale_up == {
        'max_scale_up_rate_percentage': 100,
        'scale_up_rate_min_replicas': 50,
        'pressure_observations': 2,
        'hold_seconds': 120,
    }
    assert restored.max_scale_down_rate_percentage == 50
    assert restored.copy().to_yaml_config() == config


def test_new_defaults_preserve_utilization_and_bound_downscale():
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=1,
                      spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
                      graceful_drain_async_occupancy=True)

    assert spec.target_utilization_percentage == 100
    assert spec.max_scale_down_rate_percentage == 50
    policy = spec.to_yaml_config()['replica_policy']
    assert 'target_utilization_percentage' not in policy
    assert 'max_scale_down_rate_percentage' not in policy


@pytest.mark.parametrize('field,bad_value', [
    ('target_utilization_percentage', 0),
    ('target_utilization_percentage', 101),
    ('target_utilization_percentage', True),
    ('expected_request_duration_seconds', 0),
    ('expected_request_duration_seconds', float('inf')),
    ('initial_provision_lead_time_seconds', -1),
    ('initial_provision_lead_time_seconds', float('inf')),
    ('initial_provision_lead_time_seconds', True),
    ('initial_provision_lead_time_seconds', 'later'),
    ('adaptive_demand_estimation', 1),
    ('adaptive_demand_estimation', 'yes'),
    ('max_scale_up_rate_percentage', 0),
    ('scale_up_rate_min_replicas', 0),
    ('scale_up_rate_period_seconds', True),
    ('max_scale_down_rate_percentage', 101),
])
def test_invalid_demand_and_wave_policy_rejected(field, bad_value):
    kwargs = {
        'min_replicas': 1,
        'max_replicas': 5,
        'target_concurrency_per_replica': 1,
        'spot_placer': spot_placer.CAPACITY_AWARE_SPOT_PLACER,
        'graceful_drain_async_occupancy': True,
        field: bad_value,
    }
    with pytest.raises(ValueError):
        _make_spec(**kwargs)


def test_partial_scale_up_wave_policy_rejected():
    with pytest.raises(ValueError, match='must be set together'):
        _make_spec(
            min_replicas=1,
            max_replicas=5,
            target_concurrency_per_replica=1,
            spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
            graceful_drain_async_occupancy=True,
            max_scale_up_rate_percentage=20,
        )


def test_adaptive_scale_up_requires_normal_wave_policy():
    with pytest.raises(ValueError, match='requires max_scale_up_rate'):
        _make_spec(
            min_replicas=1,
            max_replicas=5,
            target_concurrency_per_replica=1,
            spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
            graceful_drain_async_occupancy=True,
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            },
        )


@pytest.mark.parametrize('field,bad_value', [
    ('max_scale_up_rate_percentage', 0),
    ('scale_up_rate_min_replicas', 0),
    ('pressure_observations', 0),
    ('hold_seconds', float('inf')),
])
def test_invalid_adaptive_scale_up_rejected(field, bad_value):
    adaptive = {
        'max_scale_up_rate_percentage': 100,
        'scale_up_rate_min_replicas': 50,
        'pressure_observations': 2,
        'hold_seconds': 120,
    }
    adaptive[field] = bad_value
    with pytest.raises(ValueError):
        _make_spec(
            min_replicas=1,
            max_replicas=5,
            target_concurrency_per_replica=1,
            spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
            graceful_drain_async_occupancy=True,
            max_scale_up_rate_percentage=20,
            scale_up_rate_min_replicas=10,
            scale_up_rate_period_seconds=60,
            adaptive_scale_up=adaptive,
        )


def test_demand_and_wave_policy_requires_logical_replicas():
    with pytest.raises(ValueError, match='require logical replicas'):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_concurrency_per_replica=1,
                   target_utilization_percentage=90)


def test_yaml_round_trip_omits_unset_knob():
    # An unset knob must not appear in the yaml (older controllers would
    # reject unknown fields during serve update).
    spec = _make_spec(min_replicas=2)
    config = spec.to_yaml_config()
    assert 'target_concurrency_per_replica' not in config.get(
        'replica_policy', {})
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.target_concurrency_per_replica is None


def test_copy_preserves_knob():
    # copy() threads every field manually; omission would silently drop the
    # knob on `sky serve update`.
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=2.0)
    copied = spec.copy()
    assert copied.target_concurrency_per_replica == 2.0
    overridden = spec.copy(target_concurrency_per_replica=3.0)
    assert overridden.target_concurrency_per_replica == 3.0


def test_autoscaling_policy_str_renders_for_concurrency_service():
    # Called on every service record build; without a concurrency branch it
    # asserts target_qps_per_replica is not None and crashes status.
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=2.0)
    result = spec.autoscaling_policy_str()
    assert isinstance(result, str)
    assert result


def test_setstate_defaults_knob_for_old_rows():
    # Specs pickled before the field existed must unpickle with the knob
    # unset instead of raising AttributeError on property access.
    spec = _make_spec(min_replicas=2)
    old_state = dict(spec.__dict__)
    del old_state['_target_concurrency_per_replica']
    restored = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    restored.__setstate__(old_state)
    assert restored.target_concurrency_per_replica is None


def test_setstate_preserves_unbounded_downscale_for_old_rows():
    spec = _make_spec(min_replicas=2)
    old_state = dict(spec.__dict__)
    del old_state['_max_scale_down_rate_percentage']
    restored = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)

    restored.__setstate__(old_state)

    assert restored.max_scale_down_rate_percentage == 100


def test_per_gpu_placer_enables_logical_replicas_without_yaml_unit():
    spec = _make_spec(min_replicas=1,
                      max_replicas=17,
                      target_concurrency_per_replica=1,
                      spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
                      graceful_drain_async_occupancy=True)

    config = spec.to_yaml_config()
    assert 'replica_unit' not in config['replica_policy']
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.replica_unit == 'logical'
    assert restored.uses_logical_replicas
    assert restored.copy().replica_unit == 'logical'


def test_legacy_per_gpu_spec_stays_physical_until_explicit_update():
    current = _make_spec(min_replicas=1,
                         max_replicas=17,
                         target_concurrency_per_replica=1,
                         spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
                         graceful_drain_async_occupancy=True)
    legacy_state = dict(current.__dict__)
    _remove_versioned_placement_contract(legacy_state)
    legacy_state.pop('_uses_logical_replicas', None)
    legacy = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    legacy.__setstate__(legacy_state)

    assert legacy.spot_placer == spot_placer.CAPACITY_AWARE_SPOT_PLACER
    assert legacy.replica_unit == 'physical_backend'
    assert not legacy.uses_logical_replicas
    assert not legacy.copy().uses_logical_replicas

    updated = service_spec_lib.SkyServiceSpec.from_yaml_config(
        legacy.to_yaml_config())
    assert updated.uses_logical_replicas


def test_legacy_per_gpu_copy_does_not_apply_new_logical_validation():
    current = _make_spec(min_replicas=1,
                         max_replicas=17,
                         target_concurrency_per_replica=2,
                         spot_placer=spot_placer.SPOT_HEDGE_PLACER)
    legacy_state = dict(current.__dict__)
    _remove_versioned_placement_contract(legacy_state)
    legacy_state['_spot_placer'] = spot_placer.CAPACITY_AWARE_SPOT_PLACER
    legacy_state['_uses_logical_replicas'] = False
    legacy = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    legacy.__setstate__(legacy_state)

    copied = legacy.copy(min_replicas=2)

    assert copied.min_replicas == 2
    assert copied.target_concurrency_per_replica == 2
    assert copied.spot_placer == spot_placer.CAPACITY_AWARE_SPOT_PLACER
    assert not copied.uses_logical_replicas


def test_old_pickled_per_gpu_spec_copy_preserves_unbounded_downscale():
    current = _make_spec(
        min_replicas=1,
        max_replicas=17,
        target_concurrency_per_replica=1,
        spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
        graceful_drain_async_occupancy=True,
    )
    old_state = dict(current.__dict__)
    _remove_versioned_placement_contract(old_state)
    for field in ('_uses_logical_replicas', '_target_utilization_percentage',
                  '_expected_request_duration_seconds',
                  '_max_scale_up_rate_percentage',
                  '_scale_up_rate_min_replicas',
                  '_scale_up_rate_period_seconds',
                  '_max_scale_down_rate_percentage'):
        old_state.pop(field, None)
    legacy = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    legacy.__setstate__(old_state)

    copied = legacy.copy()

    assert not copied.uses_logical_replicas
    assert copied.max_scale_down_rate_percentage == 100


def test_other_placer_keeps_legacy_physical_semantics():
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=1.0,
                      spot_placer=spot_placer.SPOT_HEDGE_PLACER)
    assert spec.replica_unit == 'physical_backend'
    assert not spec.uses_logical_replicas
    assert 'replica_unit' not in spec.to_yaml_config()['replica_policy']


@pytest.mark.parametrize('kwargs,match', [
    ({
        'target_concurrency_per_replica': 1.0,
        'graceful_drain_async_occupancy': True,
    }, 'positive integer'),
    ({
        'target_concurrency_per_replica': 2.5,
        'graceful_drain_async_occupancy': True,
    }, 'positive integer'),
    ({
        'target_concurrency_per_replica': True,
        'graceful_drain_async_occupancy': True,
    }, 'positive integer'),
    ({
        'target_concurrency_per_replica': 1,
    }, 'graceful_drain_async_occupancy: true'),
])
def test_per_gpu_placer_rejects_ambiguous_capacity_contract(kwargs, match):
    with pytest.raises(ValueError, match=match):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
                   **kwargs)


def test_per_gpu_placer_accepts_outstanding_work_saturation():
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=2,
                      graceful_drain_async_occupancy=True,
                      spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER)

    assert spec.uses_logical_replicas
    assert spec.target_concurrency_per_replica == 2


def test_per_gpu_placer_accepts_reserved_fill_at_spec_level():
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=1,
                      graceful_drain_async_occupancy=True,
                      spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
                      reserved_capacity_fill=True)
    assert spec.uses_logical_replicas
    assert spec.reserved_capacity_fill


def test_replica_unit_is_not_a_user_facing_yaml_field():
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_concurrency_per_replica=1,
                      spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
                      graceful_drain_async_occupancy=True)
    config = spec.to_yaml_config()
    config['replica_policy']['replica_unit'] = 'logical'

    with pytest.raises(ValueError, match='Invalid service YAML'):
        service_spec_lib.SkyServiceSpec.from_yaml_config(config)
