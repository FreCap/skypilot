"""Spec plumbing for reserved_capacity_fill.

Opt-in boolean under replica_policy that will later let the autoscaler
scale up onto free zero-cost capacity. These tests pin the spec-level
contract only: absent means False with no yaml footprint, True round-trips,
and the flag is orthogonal to the autoscaling knobs.
"""
from typing import Any, Dict

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
