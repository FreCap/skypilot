"""Spec plumbing for reserved_capacity_fill.

Opt-in field under replica_policy that will later let the autoscaler
scale up onto free zero-cost capacity. Bool form (plain enable) or object
form ({floor_replicas, weight}, which implies enabled). These tests pin the
spec-level contract only: absent means False with no yaml footprint, both
forms round-trip (an all-defaults object canonicalizes to the bool form),
and the flag is orthogonal to the autoscaling knobs.
"""
from typing import Any, Dict

import pytest

from sky.serve import service_spec as service_spec_lib


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


def test_flag_absent_defaults_false_and_omitted_from_yaml():
    # Absent must be indistinguishable from disabled, and must not appear
    # in the yaml (older controllers would reject unknown fields during
    # serve update).
    spec = _make_spec(min_replicas=2)
    assert spec.reserved_capacity_fill is False
    config = spec.to_yaml_config()
    assert 'reserved_capacity_fill' not in config.get('replica_policy', {})
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.reserved_capacity_fill is False


def test_flag_true_round_trips():
    # No autoscaling fields set: the flag alone must parse and round-trip.
    spec = _make_spec(min_replicas=2, reserved_capacity_fill=True)
    assert spec.reserved_capacity_fill is True
    config = spec.to_yaml_config()
    assert config['replica_policy']['reserved_capacity_fill'] is True
    # from_yaml_config also runs the JSON schema, so this covers schema
    # acceptance of the new field.
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.reserved_capacity_fill is True


def test_flag_with_plain_qps_autoscaling():
    # Orthogonal to the demand knobs: plain float QPS autoscaling with the
    # flag set must parse without any concurrency fields.
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_qps_per_replica=1.0,
                      reserved_capacity_fill=True)
    assert spec.reserved_capacity_fill is True
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(
        spec.to_yaml_config())
    assert restored.reserved_capacity_fill is True
    assert restored.target_qps_per_replica == 1.0


def test_copy_preserves_flag():
    # copy() threads every field manually; omission would silently drop the
    # flag on `sky serve update`.
    spec = _make_spec(min_replicas=2, reserved_capacity_fill=True)
    assert spec.copy().reserved_capacity_fill is True
    assert spec.copy(
        reserved_capacity_fill=False).reserved_capacity_fill is False


def test_setstate_defaults_flag_for_old_rows():
    # Specs pickled before the field existed must unpickle with the flag
    # off instead of raising AttributeError on property access.
    spec = _make_spec(min_replicas=2)
    old_state = dict(spec.__dict__)
    del old_state['_reserved_capacity_fill']
    restored = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    restored.__setstate__(old_state)
    assert restored.reserved_capacity_fill is False


@pytest.mark.parametrize('fallback_field', [
    {
        'dynamic_ondemand_fallback': True
    },
    {
        'base_ondemand_fallback_replicas': 1
    },
])
def test_flag_rejected_with_ondemand_fallback(fallback_field):
    # Fill launches land as NON-spot replicas on zero-cost locations,
    # indistinguishable (via is_spot) from paid on-demand fallback
    # capacity: FallbackRequestRateAutoscaler would count them toward
    # the fallback quota and pick them as excess-on-demand victims.
    # Reject at load.
    spec = _make_spec(min_replicas=1,
                      max_replicas=5,
                      target_qps_per_replica=1.0,
                      reserved_capacity_fill=True)
    config = spec.to_yaml_config()
    config['replica_policy'].update(fallback_field)
    with pytest.raises(ValueError):
        service_spec_lib.SkyServiceSpec.from_yaml_config(config)


def test_flag_rejected_with_fallback_at_constructor():
    # The YAML-path check alone is bypassable by programmatic
    # construction; the constructor is the single enforcement point.
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_qps_per_replica=1.0,
                   reserved_capacity_fill=True,
                   dynamic_ondemand_fallback=True)


# ---------------------------------------------------------------------------
# Object form: reserved_capacity_fill: {floor_replicas, weight}
# ---------------------------------------------------------------------------


def test_object_form_parses_and_exposes_knobs():
    spec = _make_spec(min_replicas=2,
                      reserved_capacity_fill={
                          'floor_replicas': 10,
                          'weight': 3,
                      })
    assert spec.reserved_capacity_fill is True
    assert spec.reserved_fill_floor_replicas == 10
    assert spec.reserved_fill_weight == 3.0
    config = spec.to_yaml_config()
    assert config['replica_policy']['reserved_capacity_fill'] == {
        'floor_replicas': 10,
        'weight': 3.0,
    }
    # from_yaml_config also runs the JSON schema, so this covers schema
    # acceptance of the object form.
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.reserved_capacity_fill is True
    assert restored.reserved_fill_floor_replicas == 10
    assert restored.reserved_fill_weight == 3.0


def test_bool_form_exposes_default_knobs():
    # Callers of the new knob properties must not need to care which form
    # the user wrote.
    assert _make_spec(min_replicas=2).reserved_fill_floor_replicas == 0
    assert _make_spec(min_replicas=2).reserved_fill_weight == 1.0
    enabled = _make_spec(min_replicas=2, reserved_capacity_fill=True)
    assert enabled.reserved_fill_floor_replicas == 0
    assert enabled.reserved_fill_weight == 1.0


@pytest.mark.parametrize('obj', [
    {},
    {
        'floor_replicas': 0
    },
    {
        'weight': 1
    },
    {
        'floor_replicas': 0,
        'weight': 1
    },
])
def test_object_form_with_defaults_still_enables_and_canonicalizes(obj):
    # An all-defaults object is the same opt-in as plain True (note {} is
    # falsy: truthiness must not decide enablement), and to_yaml_config
    # canonicalizes it to the bool form so older controllers keep seeing
    # the shape they understand.
    spec = _make_spec(min_replicas=2, reserved_capacity_fill=obj)
    assert spec.reserved_capacity_fill is True
    config = spec.to_yaml_config()
    assert config['replica_policy']['reserved_capacity_fill'] is True
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.reserved_capacity_fill is True
    assert restored.reserved_fill_floor_replicas == 0
    assert restored.reserved_fill_weight == 1.0


def test_object_form_partial_round_trips_only_non_defaults():
    spec = _make_spec(min_replicas=2,
                      reserved_capacity_fill={'floor_replicas': 4})
    config = spec.to_yaml_config()
    assert config['replica_policy']['reserved_capacity_fill'] == {
        'floor_replicas': 4
    }
    restored = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
    assert restored.reserved_fill_floor_replicas == 4
    assert restored.reserved_fill_weight == 1.0


def test_copy_preserves_object_form():
    spec = _make_spec(min_replicas=2,
                      reserved_capacity_fill={
                          'floor_replicas': 7,
                          'weight': 2.5,
                      })
    copied = spec.copy()
    assert copied.reserved_capacity_fill is True
    assert copied.reserved_fill_floor_replicas == 7
    assert copied.reserved_fill_weight == 2.5
    # Override still works and downgrades cleanly to the bool form.
    downgraded = spec.copy(reserved_capacity_fill=False)
    assert downgraded.reserved_capacity_fill is False
    assert downgraded.reserved_fill_floor_replicas == 0
    assert downgraded.reserved_fill_weight == 1.0


def test_setstate_pre_object_bool_pickles_expose_default_knobs():
    # Specs pickled while the field was still a plain bool must keep
    # working once the knob properties exist.
    spec = _make_spec(min_replicas=2, reserved_capacity_fill=True)
    old_state = dict(spec.__dict__)
    old_state['_reserved_capacity_fill'] = True  # pre-object representation
    restored = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    restored.__setstate__(old_state)
    assert restored.reserved_capacity_fill is True
    assert restored.reserved_fill_floor_replicas == 0
    assert restored.reserved_fill_weight == 1.0


@pytest.mark.parametrize(
    'bad_obj',
    [
        {
            'floor_replicas': -1
        },
        {
            'weight': 0
        },
        {
            'weight': -2
        },
        # Non-finite weights pass naive sign checks (inf > 0; NaN compares
        # False to everything) and would poison the broker's weighted
        # water-fill (inf/inf -> NaN) every round for the whole pool.
        {
            'weight': float('inf')
        },
        {
            'weight': float('nan')
        },
    ])
def test_object_form_rejects_bad_knobs_at_constructor(bad_obj):
    # The schema guards the YAML path; the constructor guards programmatic
    # construction.
    with pytest.raises(ValueError):
        _make_spec(min_replicas=2, reserved_capacity_fill=bad_obj)


def test_floor_above_max_replicas_rejected():
    # A floor beyond max_replicas can never be materialized (the fill
    # target is clamped to max_replicas); it would sit as a permanent
    # phantom claim on the broker.
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=3,
                   target_qps_per_replica=1.0,
                   reserved_capacity_fill={'floor_replicas': 4})


def test_floor_at_max_replicas_accepted():
    spec = _make_spec(min_replicas=1,
                      max_replicas=3,
                      target_qps_per_replica=1.0,
                      reserved_capacity_fill={'floor_replicas': 3})
    assert spec.reserved_fill_floor_replicas == 3


def test_object_form_rejected_with_ondemand_fallback():
    # The object form is still the same opt-in: the fallback exclusivity
    # must apply to it too, including the falsy-{} spelling.
    with pytest.raises(ValueError):
        _make_spec(min_replicas=1,
                   max_replicas=5,
                   target_qps_per_replica=1.0,
                   reserved_capacity_fill={},
                   dynamic_ondemand_fallback=True)
