"""Characterization tests for the GCS storage backend facade."""

# pylint: disable=protected-access
import pickle
from unittest import mock

import pytest

from sky import exceptions
from sky.data import storage as storage_lib


def _gcs_store(**attributes) -> storage_lib.GcsStore:
    store = object.__new__(storage_lib.GcsStore)
    store.name = attributes.pop('name', 'test-bucket')
    store.source = attributes.pop('source', None)
    store.region = attributes.pop('region', 'us-central1')
    store.is_sky_managed = attributes.pop('is_sky_managed', False)
    store._bucket_sub_path = attributes.pop('_bucket_sub_path', None)
    for name, value in attributes.items():
        setattr(store, name, value)
    return store


def test_gcs_store_public_identity_and_type_mapping():
    assert storage_lib.GcsStore.__module__ == storage_lib.__name__
    assert storage_lib.StoreType.from_store(
        _gcs_store()) is (storage_lib.StoreType.GCS)


def test_gcs_store_pickle_round_trip_uses_public_facade():
    store = _gcs_store(source='gs://test-bucket', _bucket_sub_path='prefix')

    restored = pickle.loads(pickle.dumps(store))

    assert type(restored) is storage_lib.GcsStore
    assert restored.source == 'gs://test-bucket'
    assert restored._bucket_sub_path == 'prefix'


@pytest.mark.parametrize('name', [
    'abc',
    'bucket-name',
    'bucket_name',
    'bucket.with.components',
])
def test_gcs_store_validate_name_accepts_valid_names(name):
    assert storage_lib.GcsStore.validate_name(name) == name


@pytest.mark.parametrize('name, expected', [
    ('ab', '3-222 characters'),
    ('Uppercase', 'lowercase letters'),
    ('goog-bucket', 'cannot begin with'),
    ('bucket..name', 'adjacent periods'),
    ('192.168.1.1', 'IP address'),
])
def test_gcs_store_validate_name_rejects_invalid_names(name, expected):
    with pytest.raises(exceptions.StorageNameError, match=expected):
        storage_lib.GcsStore.validate_name(name)


@pytest.mark.parametrize('source, expected_call', [
    (['a', 'b'], ('rsync', (['a', 'b'],), {
        'create_dirs': True
    })),
    ('/tmp/source', ('rsync', (['/tmp/source'],), {})),
    ('s3://test-bucket', ('transfer', (), {})),
    ('r2://test-bucket', ('transfer', (), {})),
    ('oci://test-bucket', ('transfer', (), {})),
    ('gs://test-bucket', None),
])
def test_gcs_store_upload_dispatch(source, expected_call):
    store = _gcs_store(source=source)
    store.batch_gsutil_rsync = mock.Mock()
    store._transfer_to_gcs = mock.Mock()

    store.upload()

    if expected_call is None:
        store.batch_gsutil_rsync.assert_not_called()
        store._transfer_to_gcs.assert_not_called()
    else:
        method, args, kwargs = expected_call
        mocked = (store.batch_gsutil_rsync
                  if method == 'rsync' else store._transfer_to_gcs)
        mocked.assert_called_once_with(*args, **kwargs)


def test_gcs_store_delete_preserves_external_bucket_with_sub_path():
    store = _gcs_store(_bucket_sub_path='prefix', is_sky_managed=False)
    store._delete_sub_path = mock.Mock()
    store._delete_gcs_bucket = mock.Mock()

    store.delete()

    store._delete_sub_path.assert_called_once_with()
    store._delete_gcs_bucket.assert_not_called()


def test_gcs_store_mount_command_delegates_provider_configuration():
    store = _gcs_store(_bucket_sub_path='prefix')
    store.bucket = mock.Mock()
    store.bucket.name = 'test-bucket'

    with mock.patch.multiple(storage_lib.mounting_utils,
                             get_gcs_mount_install_cmd=mock.DEFAULT,
                             get_gcs_mount_cmd=mock.DEFAULT,
                             get_mounting_command=mock.DEFAULT) as mounting:
        mounting['get_gcs_mount_install_cmd'].return_value = 'install'
        mounting['get_gcs_mount_cmd'].return_value = 'mount'
        mounting['get_mounting_command'].return_value = 'wrapped'

        result = store.mount_command('/mnt/data', read_only=True)

    assert result == 'wrapped'
    mounting['get_gcs_mount_cmd'].assert_called_once_with('test-bucket',
                                                          '/mnt/data',
                                                          'prefix',
                                                          read_only=True)
    mounting['get_mounting_command'].assert_called_once_with(
        '/mnt/data', 'install', 'mount', mock.ANY)
