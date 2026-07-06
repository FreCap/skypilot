"""target_qps_per_replica keys must accept real accelerator names.

The old key regex rejected hyphenated catalog names like 'A100-80GB',
which blocked heterogeneous services from declaring per-type QPS.
"""
import jsonschema
import pytest

from sky.utils import schemas


def _validate(qps_value):
    service_schema = schemas.get_service_schema()
    jsonschema.validate(
        {
            'readiness_probe': '/health',
            'replica_policy': {
                'min_replicas': 1,
                'target_qps_per_replica': qps_value,
            },
        }, service_schema)


def test_hyphenated_accelerator_keys_accepted():
    _validate({'L4': 0.1, 'A100-80GB': 0.1, 'H100-MEGA-80GB:8': 0.4})


def test_plain_and_counted_keys_still_accepted():
    _validate({'A100': 0.1, 'H100:1': 0.2})
    _validate(0.1)


def test_garbage_keys_still_rejected():
    with pytest.raises(jsonschema.ValidationError):
        _validate({'not a gpu name!': 0.1})
