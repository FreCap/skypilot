"""Characterization for storage and volume schema construction."""

import copy
import inspect

import jsonschema
import pytest

from sky.utils import schemas
from sky.utils import storage_schemas


def test_historical_schema_function_contracts() -> None:
    functions = (
        schemas.get_storage_schema,
        schemas.get_volume_schema,
        schemas.get_volume_mount_schema,
    )
    assert [function.__name__ for function in functions] == [
        'get_storage_schema',
        'get_volume_schema',
        'get_volume_mount_schema',
    ]
    assert all(
        function.__module__ == 'sky.utils.schemas' for function in functions)
    assert all(
        not inspect.signature(function).parameters for function in functions)
    assert schemas.get_storage_schema is storage_schemas.get_storage_schema
    assert schemas.get_volume_schema is storage_schemas.get_volume_schema
    assert (schemas.get_volume_mount_schema
            is storage_schemas.get_volume_mount_schema)
    assert (schemas._LABELS_SCHEMA is  # pylint: disable=protected-access
            storage_schemas._LABELS_SCHEMA)  # pylint: disable=protected-access


def test_schema_construction_returns_independent_values() -> None:
    for constructor in (schemas.get_storage_schema, schemas.get_volume_schema,
                        schemas.get_volume_mount_schema):
        first = constructor()
        expected = copy.deepcopy(first)
        first['properties'].clear()
        assert constructor() == expected


@pytest.mark.parametrize(
    ('constructor', 'valid', 'invalid'),
    [
        (
            schemas.get_storage_schema,
            {
                'name': 'dataset',
                'store': 's3',
                'mode': 'MOUNT',
                'config': {
                    'mount_cached': {
                        'transfers': 4,
                        'buffer_size': '64M',
                        'vfs_cache_max_age': '1h30m',
                    },
                    'mount': {
                        'read_only': True,
                        'hf_mount_args': ['--cache-dir', '/tmp/cache'],
                    },
                },
            },
            {
                'config': {
                    'mount_cached': {
                        'transfers': 0,
                        'buffer_size': '64MiB',
                        'vfs_cache_max_age': 'tomorrow',
                    },
                },
            },
        ),
        (
            schemas.get_volume_schema,
            {
                'name': 'training-data',
                'type': 'kubernetes',
                'infra': 'k8s/my/context',
                'size': '100Gi',
                'labels': {
                    'team': 'research'
                },
                'config': {
                    'access_mode': 'ReadWriteMany',
                    'storage_class_name': 'efs',
                },
            },
            {
                'name': 'training-data',
                'type': 'kubernetes',
                'infra': '*/us-east-1',
            },
        ),
        (
            schemas.get_volume_mount_schema,
            {
                'path': '/data',
                'volume_name': 'training-data',
                'sub_path': 'datasets/v1',
                'volume_config': {
                    'cloud': 'aws',
                    'region': None,
                    'zone': 'us-east-1a',
                },
            },
            {
                'path': '/data',
                'sub_path': '/secrets',
            },
        ),
    ],
)
def test_representative_validation_contract(constructor, valid,
                                            invalid) -> None:
    schema = constructor()
    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, schema)
