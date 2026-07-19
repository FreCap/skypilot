"""Characterization tests for the IBM COS storage backend facade."""

# pylint: disable=protected-access
import contextlib
import pickle
import shlex
from unittest import mock

import pytest

from sky import exceptions
from sky.data import storage as storage_lib
from sky.data import storage_ibm


def _ibm_store(**attributes) -> storage_lib.IBMCosStore:
    store = object.__new__(storage_lib.IBMCosStore)
    store.name = attributes.pop('name', 'test-bucket')
    store.source = attributes.pop('source', None)
    store.region = attributes.pop('region', 'us-east')
    store.is_sky_managed = attributes.pop('is_sky_managed', False)
    store.sync_on_reconstruction = attributes.pop('sync_on_reconstruction',
                                                  True)
    store._bucket_sub_path = attributes.pop('_bucket_sub_path', None)
    store.rclone_profile_name = attributes.pop('rclone_profile_name',
                                               'ibm-test-bucket')
    for name, value in attributes.items():
        setattr(store, name, value)
    return store


def test_ibm_cos_store_public_identity_and_type_mapping():
    assert storage_lib.IBMCosStore.__module__ == storage_lib.__name__
    assert storage_lib.StoreType.from_store(
        _ibm_store()) is storage_lib.StoreType.IBM


def test_ibm_cos_store_pickle_round_trip_uses_public_facade():
    store = _ibm_store(source='cos://us-east/test-bucket',
                       _bucket_sub_path='prefix')

    restored = pickle.loads(pickle.dumps(store))

    assert type(restored) is storage_lib.IBMCosStore
    assert restored.source == 'cos://us-east/test-bucket'
    assert restored._bucket_sub_path == 'prefix'


@pytest.mark.parametrize('name', [
    'abc',
    'bucket-name',
    'bucket.with.components',
])
def test_ibm_cos_store_validate_name_accepts_valid_names(name):
    assert storage_lib.IBMCosStore.validate_name(name) == name


@pytest.mark.parametrize('name, expected', [
    ('ab', 'between 3'),
    ('Uppercase', 'lowercase letters'),
    ('bucket..name', 'adjacent periods/dashes'),
    ('bucket--name', 'adjacent periods/dashes'),
    ('192.168.1.1', 'IP address'),
    ('bucket.-name', 'not allow substrings'),
])
def test_ibm_cos_store_validate_name_rejects_invalid_names(name, expected):
    with pytest.raises(exceptions.StorageNameError, match=expected):
        storage_lib.IBMCosStore.validate_name(name)


@pytest.mark.parametrize('source, expected_call', [
    (['a', 'b'], ((['a', 'b'],), {
        'create_dirs': True
    })),
    ('/tmp/source', ((['/tmp/source'],), {})),
    ('cos://us-east/test-bucket', None),
])
def test_ibm_cos_store_upload_dispatch(source, expected_call):
    store = _ibm_store(source=source)
    store.batch_ibm_rsync = mock.Mock()

    store.upload()

    if expected_call is None:
        store.batch_ibm_rsync.assert_not_called()
    else:
        args, kwargs = expected_call
        store.batch_ibm_rsync.assert_called_once_with(*args, **kwargs)


@pytest.mark.parametrize('scheme', ['s3', 'gs', 'r2', 'nebius', 'cw'])
def test_ibm_cos_store_upload_preserves_cross_cloud_error_wrapping(scheme):
    store = _ibm_store(source=f'{scheme}://test-bucket')
    store.batch_ibm_rsync = mock.Mock()

    with pytest.raises(exceptions.StorageUploadError) as exc_info:
        store.upload()

    assert isinstance(exc_info.value.__cause__, Exception)
    assert 'currently not supporting' in str(exc_info.value.__cause__)
    store.batch_ibm_rsync.assert_not_called()


def test_ibm_cos_store_delete_preserves_external_bucket_with_sub_path():
    store = _ibm_store(_bucket_sub_path='prefix', is_sky_managed=False)
    store._delete_sub_path = mock.Mock()
    store._delete_cos_bucket = mock.Mock()

    store.delete()

    store._delete_sub_path.assert_called_once_with()
    store._delete_cos_bucket.assert_not_called()


def test_ibm_cos_upload_quotes_remote_paths():
    store = _ibm_store(_bucket_sub_path='prefix $(echo INJECTED)')
    commands = {}

    def capture_commands(_source_paths, file_command_generator,
                         dir_command_generator, *_args, **_kwargs):
        commands['file'] = file_command_generator('/tmp/base dir',
                                                  ['file $(echo INJECTED)'])
        commands['dir'] = dir_command_generator('/tmp/source dir',
                                                'dest $(echo INJECTED)')

    with mock.patch.object(
            storage_ibm.rich_utils,
            'safe_status',
            return_value=contextlib.nullcontext()), mock.patch.object(
                storage_ibm.data_utils,
                'parallel_upload',
                side_effect=capture_commands), mock.patch.object(
                    storage_ibm.sky_logging,
                    'generate_tmp_logging_file_path',
                    return_value='/tmp/storage.log'):
        store.batch_ibm_rsync(['/tmp/ignored'])

    remote_prefix = (
        f'{store.rclone_profile_name}:{store.name}/{store._bucket_sub_path}')
    assert shlex.split(commands['file']) == [
        'rclone',
        'copy',
        '--include',
        'file $(echo INJECTED)',
        '/tmp/base dir',
        remote_prefix,
    ]
    assert shlex.split(commands['dir']) == [
        'rclone',
        'copy',
        '--exclude',
        '.git/*',
        '/tmp/source dir',
        f'{remote_prefix}/dest $(echo INJECTED)',
    ]


def test_ibm_cos_store_download_file_preserves_sdk_argument_order():
    store = _ibm_store(client=mock.Mock())

    store._download_file('remote/key', '/tmp/local')

    store.client.download_file.assert_called_once_with('test-bucket',
                                                       '/tmp/local',
                                                       'remote/key')


def test_ibm_cos_store_get_bucket_creates_missing_bucket():
    store = _ibm_store(source='/tmp/source', sync_on_reconstruction=True)
    store._create_cos_bucket = mock.Mock(return_value='created-bucket')

    with mock.patch.object(storage_lib.data_utils,
                           'get_ibm_cos_bucket_region',
                           return_value=''), mock.patch.object(
                               storage_lib.data_utils.Rclone,
                               'store_rclone_config') as store_config:
        result = store._get_bucket()

    assert result == ('created-bucket', True)
    store._create_cos_bucket.assert_called_once_with('test-bucket', 'us-east')
    store_config.assert_called_once_with(
        'test-bucket', storage_lib.data_utils.Rclone.RcloneStores.IBM,
        'us-east')


def test_ibm_cos_store_get_bucket_rejects_region_mismatch():
    store = _ibm_store(source='cos://us-south/test-bucket',
                       region='us-east',
                       sync_on_reconstruction=True)

    with mock.patch.object(storage_lib.data_utils,
                           'get_ibm_cos_bucket_region',
                           return_value='us-east'), pytest.raises(
                               exceptions.StorageBucketGetError,
                               match='region us-east'):
        store._get_bucket()


def test_ibm_cos_store_mount_command_delegates_provider_configuration():
    store = _ibm_store(_bucket_sub_path='prefix')
    store.bucket = mock.Mock()
    store.bucket.name = 'test-bucket'

    with mock.patch.object(
            storage_lib.data_utils.Rclone.RcloneStores.IBM,
            'get_config',
            return_value='rclone-config') as get_config, mock.patch.multiple(
                storage_lib.mounting_utils,
                get_rclone_install_cmd=mock.DEFAULT,
                get_cos_mount_cmd=mock.DEFAULT,
                get_mounting_command=mock.DEFAULT) as mounting:
        mounting['get_rclone_install_cmd'].return_value = 'install'
        mounting['get_cos_mount_cmd'].return_value = 'mount'
        mounting['get_mounting_command'].return_value = 'wrapped'

        result = store.mount_command('/mnt/data', read_only=True)

    assert result == 'wrapped'
    get_config.assert_called_once_with(rclone_profile_name='ibm-test-bucket',
                                       region='us-east')
    mounting['get_cos_mount_cmd'].assert_called_once_with('rclone-config',
                                                          'ibm-test-bucket',
                                                          'test-bucket',
                                                          '/mnt/data',
                                                          'prefix',
                                                          read_only=True)
    mounting['get_mounting_command'].assert_called_once_with(
        '/mnt/data', 'install', 'mount')
