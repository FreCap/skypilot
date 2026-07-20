"""Characterization tests for the Azure Blob storage backend facade."""

# pylint: disable=protected-access
import contextlib
import pickle
import shlex
from unittest import mock

import pytest

from sky import exceptions
from sky.data import storage as storage_lib
from sky.data import storage_azure


def _azure_store(**attributes) -> storage_lib.AzureBlobStore:
    store = object.__new__(storage_lib.AzureBlobStore)
    store.name = attributes.pop('name', 'test-container')
    store.source = attributes.pop('source', None)
    store.region = attributes.pop('region', 'eastus')
    store.is_sky_managed = attributes.pop('is_sky_managed', False)
    store._bucket_sub_path = attributes.pop('_bucket_sub_path', None)
    store.storage_account_name = attributes.pop('storage_account_name',
                                                'testaccount')
    store.storage_account_key = attributes.pop('storage_account_key',
                                               'test-key')
    store.resource_group_name = attributes.pop('resource_group_name',
                                               'test-group')
    store.container_name = attributes.pop('container_name', 'test-container')
    for name, value in attributes.items():
        setattr(store, name, value)
    return store


def test_azure_blob_store_public_identity_and_type_mapping():
    assert storage_lib.AzureBlobStore.__module__ == storage_lib.__name__
    assert storage_lib.AzureBlobStore.AzureBlobStoreMetadata.__module__ == (
        storage_lib.__name__)
    assert storage_lib.StoreType.from_store(
        _azure_store()) is storage_lib.StoreType.AZURE


def test_azure_blob_store_pickle_round_trip_uses_public_facade():
    store = _azure_store(source='https://testaccount.blob.core.windows.net/'
                         'test-container',
                         _bucket_sub_path='prefix')

    restored = pickle.loads(pickle.dumps(store))

    assert type(restored) is storage_lib.AzureBlobStore
    assert restored.storage_account_name == 'testaccount'
    assert restored._bucket_sub_path == 'prefix'


def test_azure_blob_store_metadata_pickle_round_trip_uses_public_facade():
    metadata = storage_lib.AzureBlobStore.AzureBlobStoreMetadata(
        name='test-container',
        storage_account_name='testaccount',
        source=None,
        region='eastus',
        is_sky_managed=True)

    restored = pickle.loads(pickle.dumps(metadata))

    assert type(restored) is (storage_lib.AzureBlobStore.AzureBlobStoreMetadata)
    assert restored.storage_account_name == 'testaccount'


@pytest.mark.parametrize('name', [
    'abc',
    'container-name',
    'container123',
])
def test_azure_blob_store_validate_name_accepts_valid_names(name):
    assert storage_lib.AzureBlobStore.validate_name(name) == name


@pytest.mark.parametrize('name, expected', [
    ('ab', 'between 3'),
    ('Uppercase', 'lowercase letters'),
    ('container--name', 'adjacent hyphens'),
    ('-container', 'begin and end'),
])
def test_azure_blob_store_validate_name_rejects_invalid_names(name, expected):
    with pytest.raises(exceptions.StorageNameError, match=expected):
        storage_lib.AzureBlobStore.validate_name(name)


@pytest.mark.parametrize('source, expected_call', [
    (['a', 'b'], ((['a', 'b'],), {
        'create_dirs': True
    })),
    ('/tmp/source', ((['/tmp/source'],), {})),
    ('https://testaccount.blob.core.windows.net/test-container', None),
])
def test_azure_blob_store_upload_dispatch(source, expected_call):
    store = _azure_store(source=source)
    store.batch_az_blob_sync = mock.Mock()

    store.upload()

    if expected_call is None:
        store.batch_az_blob_sync.assert_not_called()
    else:
        args, kwargs = expected_call
        store.batch_az_blob_sync.assert_called_once_with(*args, **kwargs)


@pytest.mark.parametrize('scheme', ['s3', 'gs', 'r2', 'oci', 'nebius', 'cw'])
def test_azure_blob_store_upload_preserves_cross_cloud_error_wrapping(scheme):
    store = _azure_store(source=f'{scheme}://test-bucket')
    store.batch_az_blob_sync = mock.Mock()

    with pytest.raises(exceptions.StorageUploadError) as exc_info:
        store.upload()

    assert isinstance(exc_info.value.__cause__, KeyError)
    assert exc_info.value.__cause__.args == ('cloud',)
    store.batch_az_blob_sync.assert_not_called()


def test_azure_blob_store_delete_preserves_external_container_with_sub_path():
    store = _azure_store(_bucket_sub_path='prefix', is_sky_managed=False)
    store._delete_sub_path = mock.Mock()
    store._delete_az_bucket = mock.Mock()

    store.delete()

    store._delete_sub_path.assert_called_once_with()
    store._delete_az_bucket.assert_not_called()


def test_azure_blob_sync_quotes_paths_and_credentials():
    store = _azure_store(
        _bucket_sub_path='prefix $(echo INJECTED)',
        storage_account_name='account $(echo INJECTED)',
        storage_account_key='key $(echo INJECTED)',
    )
    commands = {}

    def capture_commands(_source_paths, file_command_generator,
                         dir_command_generator, *_args, **_kwargs):
        commands['file'] = file_command_generator(
            '/tmp/base dir', ['file $(echo INJECTED)', 'other file'])
        commands['dir'] = dir_command_generator('/tmp/source dir',
                                                'dest $(echo INJECTED)')

    with mock.patch.object(
            storage_azure.rich_utils,
            'safe_status',
            return_value=contextlib.nullcontext()), mock.patch.object(
                storage_azure.data_utils,
                'parallel_upload',
                side_effect=capture_commands), mock.patch.object(
                    storage_azure.storage_utils,
                    'get_excluded_files',
                    return_value=[]), mock.patch.object(
                        storage_azure.sky_logging,
                        'generate_tmp_logging_file_path',
                        return_value='/tmp/storage.log'):
        store.batch_az_blob_sync(['/tmp/ignored'])

    common_prefix = [
        'az',
        'storage',
        'blob',
        'sync',
        '--account-name',
        store.storage_account_name,
        '--account-key',
        store.storage_account_key,
    ]
    common_suffix = [
        '--delete-destination',
        'false',
        '--source',
    ]
    container_path = f'{store.container_name}/{store._bucket_sub_path}'
    assert shlex.split(commands['file']) == common_prefix + [
        '--include-pattern',
        'file $(echo INJECTED);other file',
    ] + common_suffix + [
        '/tmp/base dir',
        '--container',
        container_path,
    ]
    assert shlex.split(commands['dir']) == common_prefix + [
        '--exclude-path',
        '.git/',
    ] + common_suffix + [
        '/tmp/source dir',
        '--container',
        f'{container_path}/dest $(echo INJECTED)',
    ]


def test_azure_blob_sync_rejects_missing_account_key():
    store = _azure_store(storage_account_key=None)

    with pytest.raises(RuntimeError, match='key is not initialized'):
        store.batch_az_blob_sync(['/tmp/source'])


def test_azure_blob_store_mount_command_delegates_provider_configuration():
    store = _azure_store(_bucket_sub_path='prefix')

    with mock.patch.multiple(storage_lib.mounting_utils,
                             get_az_mount_install_cmd=mock.DEFAULT,
                             get_az_mount_cmd=mock.DEFAULT,
                             get_mounting_command=mock.DEFAULT) as mounting:
        mounting['get_az_mount_install_cmd'].return_value = 'install'
        mounting['get_az_mount_cmd'].return_value = 'mount'
        mounting['get_mounting_command'].return_value = 'wrapped'

        result = store.mount_command('/mnt/data', read_only=True)

    assert result == 'wrapped'
    mounting['get_az_mount_cmd'].assert_called_once_with('test-container',
                                                         'testaccount',
                                                         '/mnt/data',
                                                         'test-key',
                                                         'prefix',
                                                         read_only=True)
    mounting['get_mounting_command'].assert_called_once_with(
        '/mnt/data', 'install', 'mount')
