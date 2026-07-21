"""Characterization tests for the GCS storage backend facade."""

# pylint: disable=protected-access
import contextlib
import pickle
import shlex
from unittest import mock

import pytest

from sky import cloud_stores
from sky import exceptions
from sky.data import storage as storage_lib
from sky.data import storage_gcs


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


def test_gcs_store_rejects_missing_cross_cloud_source(monkeypatch):
    store = _gcs_store(name='missing-bucket', source='s3://missing-bucket/path')
    verify_source = mock.Mock(return_value=False)
    monkeypatch.setattr(storage_gcs.data_utils, 'verify_s3_bucket',
                        verify_source)

    with pytest.raises(AssertionError, match='S3 Bucket should exist'):
        store._validate()

    verify_source.assert_called_once_with('missing-bucket')


def test_gcs_store_rejects_mismatched_source_name(monkeypatch):
    store = _gcs_store(name='destination-bucket',
                       source='s3://source-bucket/path')
    verify_source = mock.Mock(return_value=True)
    monkeypatch.setattr(storage_gcs.data_utils, 'verify_s3_bucket',
                        verify_source)

    with pytest.raises(AssertionError, match='name should be the same'):
        store._validate()

    verify_source.assert_not_called()


def test_gcs_directory_probe_quotes_ordinary_object_url(monkeypatch):
    url = 'gs://test-bucket/research team\'s "final"; results #1.csv'
    run = mock.Mock(return_value=mock.Mock(stdout=f'{url}\n'.encode()))
    monkeypatch.setattr(cloud_stores.GcsCloudStorage, '_INSTALL_GSUTIL', 'true')
    monkeypatch.setattr(cloud_stores.GcsCloudStorage, '_gsutil_command',
                        property(lambda _: 'gsutil'))
    monkeypatch.setattr(cloud_stores.subprocess, 'run', run)

    assert cloud_stores.GcsCloudStorage().is_directory(url) is False
    command = run.call_args.args[0]
    assert command == f'true && gsutil ls -d {shlex.quote(url)}'


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


def test_gcs_store_sub_path_delete_quotes_target_uri():
    store = _gcs_store()
    store.client = mock.Mock()
    sub_path = 'prefix;echo_INJECTED'
    target_uri = f'gs://{store.name}/{sub_path}'

    with mock.patch.object(
            storage_gcs.rich_utils,
            'safe_status',
            return_value=contextlib.nullcontext()), mock.patch.object(
                storage_gcs.data_utils,
                'get_gsutil_command',
                return_value=('gsutil', 'true')), mock.patch.object(
                    storage_gcs.subprocess, 'check_output') as check_output:
        assert store._delete_gcs_bucket(store.name, sub_path)

    command = check_output.call_args.args[0]
    assert command.endswith(f'gsutil rm -r {shlex.quote(target_uri)}')


def test_gcs_store_cp_quotes_source_paths_and_target_uri():
    store = _gcs_store(_bucket_sub_path='prefix;echo_INJECTED')
    source_paths = ['/tmp/file $(echo INJECTED)', '/tmp/other file']
    commands = []

    with mock.patch.object(
            storage_gcs.rich_utils,
            'safe_status',
            return_value=contextlib.nullcontext()), mock.patch.object(
                storage_gcs.data_utils,
                'get_gsutil_command',
                return_value=('gsutil', 'true')), mock.patch.object(
                    storage_gcs.data_utils,
                    'run_upload_cli',
                    side_effect=lambda command, *_args, **_kwargs: commands.
                    append(command)), mock.patch.object(
                        storage_gcs.sky_logging,
                        'generate_tmp_logging_file_path',
                        return_value='/tmp/storage.log'):
        store.batch_gsutil_cp(source_paths)

    expected_sources = ' '.join(shlex.quote(path) for path in source_paths)
    target_uri = f'gs://{store.name}/{store._bucket_sub_path}'
    assert commands == [
        f"true; printf '%s\\n' {expected_sources} | gsutil "
        f'cp -e -n -r -I {shlex.quote(target_uri)}'
    ]


def test_gcs_store_rsync_quotes_patterns_and_target_uris():
    store = _gcs_store(_bucket_sub_path='prefix;echo_INJECTED')
    commands = {}

    def capture_commands(_source_paths, file_command_generator,
                         dir_command_generator, *_args, **_kwargs):
        commands['file'] = file_command_generator(
            '/tmp/base dir', ['file[1]', 'file;echo_INJECTED'])
        commands['dir'] = dir_command_generator('/tmp/source dir',
                                                'dest;echo_INJECTED')

    with mock.patch.object(
            storage_gcs.rich_utils,
            'safe_status',
            return_value=contextlib.nullcontext()), mock.patch.object(
                storage_gcs.data_utils,
                'get_gsutil_command',
                return_value=('gsutil', 'true')), mock.patch.object(
                    storage_gcs.data_utils,
                    'parallel_upload',
                    side_effect=capture_commands), mock.patch.object(
                        storage_gcs.storage_utils,
                        'get_excluded_files',
                        return_value=[]), mock.patch.object(
                            storage_gcs.sky_logging,
                            'generate_tmp_logging_file_path',
                            return_value='/tmp/storage.log'):
        store.batch_gsutil_rsync(['/tmp/ignored'])

    file_pattern = r'^(?!(?:file\[1\]|file;echo_INJECTED)$).*'
    base_target = f'gs://{store.name}/{store._bucket_sub_path}'
    assert commands['file'].endswith(
        f'rsync -e -x {shlex.quote(file_pattern)} '
        f"'/tmp/base dir' {shlex.quote(base_target)}")
    assert commands['dir'].endswith(
        f"rsync -e -r -x '(^\\.git/.*$)' '/tmp/source dir' "
        f"{shlex.quote(base_target + '/dest;echo_INJECTED')}")


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
