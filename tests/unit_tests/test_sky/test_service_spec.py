"""Tests for SkyServiceSpec, specifically pool configuration validation."""
import pickle

import pytest

from sky.serve import constants as serve_constants
from sky.serve import service_spec


class TestLoadBalancerHighAvailability:
    """Default-on and explicit compatibility semantics."""

    def test_new_service_defaults_to_high_availability(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({})

        assert spec.lb_high_availability
        assert not spec.lb_high_availability_specified

    def test_explicit_opt_out_round_trips(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config(
            {'load_balancer': {
                'high_availability': False,
            }})

        assert not spec.lb_high_availability
        assert spec.lb_high_availability_specified
        assert spec.to_yaml_config(
        )['load_balancer']['high_availability'] is False

    def test_explicit_opt_in_round_trips_with_specified_bit(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config(
            {'load_balancer': {
                'high_availability': True,
            }})

        rendered = spec.to_yaml_config()
        restored = service_spec.SkyServiceSpec.from_yaml_config(rendered)

        assert rendered['load_balancer']['high_availability'] is True
        assert restored.lb_high_availability
        assert restored.lb_high_availability_specified

    def test_pool_does_not_enable_unused_load_balancer(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'pool': {},
            'workers': 1,
        })

        assert not spec.lb_high_availability

    def test_pool_rejects_explicit_high_availability(self):
        with pytest.raises(ValueError, match='pools have no inference'):
            service_spec.SkyServiceSpec.from_yaml_config({
                'pool': {},
                'workers': 1,
                'load_balancer': {
                    'high_availability': True,
                },
            })

    def test_old_pickled_spec_stays_legacy(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({})
        state = spec.__dict__.copy()
        state.pop('_lb_high_availability')
        state.pop('_lb_high_availability_specified')
        restored = object.__new__(service_spec.SkyServiceSpec)
        restored.__setstate__(state)

        assert not restored.lb_high_availability
        assert not restored.lb_high_availability_specified


class TestPoolConfiguration:
    """Test pool configuration validation in SkyServiceSpec."""

    def test_pool_with_min_and_max_workers_without_workers(self):
        """Test that pool can be specified with min_workers and max_workers
        without workers set.

        This is a valid autoscaling configuration.
        """
        config = {
            'pool': {
                'min_workers': 1,
                'max_workers': 5,
            },
            'readiness_probe': '/',
        }

        # Should not raise any error
        spec = service_spec.SkyServiceSpec.from_yaml_config(config)

        # Verify the values were properly set
        assert spec.min_replicas == 1
        assert spec.max_replicas == 5

    def test_pool_with_only_workers(self):
        """Test that pool can be specified with just workers (fixed workers)."""
        config = {
            'pool': {},
            'workers': 3,
            'readiness_probe': '/',
        }

        spec = service_spec.SkyServiceSpec.from_yaml_config(config)

        assert spec.min_replicas == 3
        # max_replicas is None for fixed workers
        assert spec.max_replicas is None

    def test_pool_with_min_max_workers_and_queue_length_threshold(self):
        """Test pool with autoscaling and queue_length_threshold."""
        config = {
            'pool': {
                'min_workers': 2,
                'max_workers': 10,
                'queue_length_threshold': 5,
            },
            'readiness_probe': '/',
        }

        spec = service_spec.SkyServiceSpec.from_yaml_config(config)

        assert spec.min_replicas == 2
        assert spec.max_replicas == 10
        assert spec.queue_length_threshold == 5

    def test_pool_with_min_max_workers_and_delays(self):
        """Test pool with autoscaling and delay settings."""
        config = {
            'pool': {
                'min_workers': 1,
                'max_workers': 8,
                'upscale_delay_seconds': 30,
                'downscale_delay_seconds': 60,
            },
            'readiness_probe': '/',
        }

        spec = service_spec.SkyServiceSpec.from_yaml_config(config)

        assert spec.min_replicas == 1
        assert spec.max_replicas == 8
        assert spec.upscale_delay_seconds == 30
        assert spec.downscale_delay_seconds == 60

    def test_pool_without_workers_and_without_min_max_fails(self):
        """Test that pool without workers or min/max_workers fails."""
        config = {
            'pool': {},
            'readiness_probe': '/',
        }

        with pytest.raises(ValueError,
                           match='One of workers, or both min_workers and '
                           'max_workers must be set'):
            service_spec.SkyServiceSpec.from_yaml_config(config)

    def test_pool_with_min_workers_but_no_max_workers_fails(self):
        """Test that pool with min_workers but no max_workers fails."""
        config = {
            'pool': {
                'min_workers': 2,
            },
            'readiness_probe': '/',
        }

        with pytest.raises(ValueError,
                           match='max_workers must be set when min_workers is '
                           'specified'):
            service_spec.SkyServiceSpec.from_yaml_config(config)

    def test_pool_with_min_workers_greater_than_max_workers_fails(self):
        """Test that pool with min_workers > max_workers fails."""
        config = {
            'pool': {
                'min_workers': 10,
                'max_workers': 5,
            },
            'readiness_probe': '/',
        }

        with pytest.raises(ValueError,
                           match=r'min_workers \(10\) must be <= max_workers '
                           r'\(5\)'):
            service_spec.SkyServiceSpec.from_yaml_config(config)

    def test_pool_with_queue_length_threshold_but_no_max_workers_fails(self):
        """Test that pool with queue_length_threshold but no max_workers fails.
        """
        config = {
            'pool': {
                'queue_length_threshold': 5,
            },
            'workers': 3,
            'readiness_probe': '/',
        }

        with pytest.raises(ValueError,
                           match='max_workers must be set when '
                           'queue_length_threshold is specified'):
            service_spec.SkyServiceSpec.from_yaml_config(config)

    def test_pool_with_zero_min_workers(self):
        """Test that pool can have min_workers=0 (scale to zero)."""
        config = {
            'pool': {
                'min_workers': 0,
                'max_workers': 5,
            },
            'readiness_probe': '/',
        }

        spec = service_spec.SkyServiceSpec.from_yaml_config(config)

        assert spec.min_replicas == 0
        assert spec.max_replicas == 5

    def test_pool_with_equal_min_and_max_workers(self):
        """Test that pool can have min_workers == max_workers."""
        config = {
            'pool': {
                'min_workers': 3,
                'max_workers': 3,
            },
            'readiness_probe': '/',
        }

        spec = service_spec.SkyServiceSpec.from_yaml_config(config)

        assert spec.min_replicas == 3
        assert spec.max_replicas == 3


class TestReadinessProbeConfiguration:
    """Test readiness probe configuration parsing."""

    def test_readiness_probe_uses_default_endpoint_probe_interval(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'readiness_probe': '/',
        })

        assert (spec.endpoint_probe_interval_seconds ==
                serve_constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS)
        assert spec.consecutive_failure_threshold_timeout is None

    def test_readiness_probe_accepts_probe_overrides(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'readiness_probe': {
                'path': '/health',
                'endpoint_probe_interval_seconds': 7,
                'consecutive_failure_threshold_timeout': 45,
            },
        })

        assert spec.endpoint_probe_interval_seconds == 7
        assert spec.consecutive_failure_threshold_timeout == 45

    def test_unpickle_old_spec_backfills_new_probe_fields(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'readiness_probe': {
                'path': '/health',
            },
            'replicas': 1,
        })
        del spec._endpoint_probe_interval_seconds
        del spec._lb_stream_timeout_seconds
        del spec._consecutive_failure_threshold_timeout

        restored = pickle.loads(pickle.dumps(spec))

        assert (restored.endpoint_probe_interval_seconds ==
                serve_constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS)
        assert (restored.lb_stream_timeout_seconds ==
                serve_constants.DEFAULT_LB_STREAM_TIMEOUT)
        assert restored.consecutive_failure_threshold_timeout is None


class TestLoadBalancerConfiguration:
    """Tests load balancer service-spec defaults and compatibility."""

    def test_default_load_balancer_settings(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({})
        assert (spec.lb_stream_timeout_seconds ==
                serve_constants.DEFAULT_LB_STREAM_TIMEOUT)

    def test_request_queue_supports_ten_thousand_waiters(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'load_balancer': {
                'request_queue': {
                    'min_size': 0,
                    'size_per_replica': 10,
                    'max_size': 10000,
                },
            },
        })

        assert spec.lb_request_queue is not None
        assert spec.lb_request_queue['size_per_replica'] == 10
        assert spec.lb_request_queue['max_size'] == 10000

    def test_to_yaml_config_omits_default_new_fields_for_compatibility(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'readiness_probe': {
                'path': '/health',
            },
            'replicas': 1,
        })

        config = spec.to_yaml_config()

        assert 'load_balancer' not in config
        assert 'endpoint_probe_interval_seconds' not in config[
            'readiness_probe']
        assert 'consecutive_failure_threshold_timeout' not in config[
            'readiness_probe']

    def test_load_balancer_stream_timeout_seconds_override(self):

        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'load_balancer': {
                'stream_timeout_seconds': 240,
            },
        })

        assert spec.lb_stream_timeout_seconds == 240

    def test_to_yaml_config_keeps_non_default_new_fields(self):
        spec = service_spec.SkyServiceSpec.from_yaml_config({
            'readiness_probe': {
                'path': '/health',
                'endpoint_probe_interval_seconds': 7,
                'consecutive_failure_threshold_timeout': 45,
            },
            'load_balancer': {
                'stream_timeout_seconds': 240,
            },
            'replicas': 1,
        })

        config = spec.to_yaml_config()

        assert (
            config['readiness_probe']['endpoint_probe_interval_seconds'] == 7)
        assert (config['readiness_probe']
                ['consecutive_failure_threshold_timeout'] == 45)
        assert config['load_balancer']['stream_timeout_seconds'] == 240
