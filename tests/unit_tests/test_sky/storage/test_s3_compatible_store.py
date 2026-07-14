"""Characterization tests for the S3-compatible storage family."""
# pylint: disable=protected-access

import pickle

from sky.data import storage as storage_lib
from sky.data import storage_s3

_PUBLIC_STORE_CONFIGS = {
    'S3Store': ('S3', 's3://', 'AWS', 'us-east-1'),
    'R2Store': ('R2', 'r2://', 'Cloudflare', 'auto'),
    'NebiusStore': ('NEBIUS', 'nebius://', 'Nebius', None),
    'CoreWeaveStore': ('COREWEAVE', 'cw://', 'CoreWeave', 'US-EAST-01A'),
    'VastDataStore': ('VASTDATA', 'vastdata://', 'VastData', 'auto'),
}


def test_public_classes_preserve_identity_and_pickle_paths():
    public_classes = (
        storage_lib.S3CompatibleConfig,
        storage_lib.S3CompatibleStore,
        *(getattr(storage_lib, name) for name in _PUBLIC_STORE_CONFIGS),
    )

    for store_cls in public_classes:
        assert store_cls is getattr(storage_s3, store_cls.__name__)
        assert store_cls.__module__ == storage_lib.__name__
        assert pickle.loads(pickle.dumps(store_cls)) is store_cls

    register = storage_lib.register_s3_compatible_store
    assert register is storage_s3.register_s3_compatible_store
    assert register.__module__ == storage_lib.__name__
    assert pickle.loads(pickle.dumps(register)) is register


def test_registry_owns_the_complete_builtin_provider_family():
    expected = {
        config[0]: getattr(storage_lib, name)
        for name, config in _PUBLIC_STORE_CONFIGS.items()
    }

    assert storage_lib._S3_COMPATIBLE_STORES == expected
    assert 'OCI' not in storage_lib._S3_COMPATIBLE_STORES
    for store_cls in expected.values():
        assert issubclass(store_cls, storage_lib.S3CompatibleStore)


def test_provider_configs_preserve_dispatch_fields():
    for name, expected in _PUBLIC_STORE_CONFIGS.items():
        config = getattr(storage_lib, name).get_config()
        assert (config.store_type, config.url_prefix, config.cloud_name,
                config.default_region) == expected


def test_provider_prefixes_cover_compatible_and_external_sources():
    store = object.__new__(storage_lib.S3Store)

    assert store.provider_prefixes == {
        's3://',
        'r2://',
        'nebius://',
        'cw://',
        'vastdata://',
        'gs://',
        'https://',
        'cos://',
        'oci://',
    }
