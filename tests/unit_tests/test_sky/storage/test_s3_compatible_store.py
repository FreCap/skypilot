"""Characterization tests for the S3-compatible storage family."""
# pylint: disable=protected-access

import contextlib
import inspect
import os
import pickle
import shlex
import types
from unittest import mock

import pytest

from sky.data import storage as storage_lib
from sky.data import storage_s3

_PUBLIC_STORE_CONFIGS = {
    'S3Store': ('S3', 's3://', 'AWS', 'us-east-1'),
    'R2Store': ('R2', 'r2://', 'Cloudflare', 'auto'),
    'NebiusStore': ('NEBIUS', 'nebius://', 'Nebius', None),
    'CoreWeaveStore': ('COREWEAVE', 'cw://', 'CoreWeave', 'US-EAST-01A'),
    'VastDataStore': ('VASTDATA', 'vastdata://', 'VastData', 'auto'),
}


def _store_with_shell_special_cli_config():
    store = object.__new__(storage_lib.S3Store)
    store.name = 'test-bucket'
    store._bucket_sub_path = 'prefix; echo INJECTED'
    store.config = types.SimpleNamespace(
        access_denied_message='Access Denied',
        aws_profile='profile; echo INJECTED',
        config_file='${HOME}/config file; echo INJECTED',
        credentials_file='~/credentials file; echo INJECTED',
        extra_cli_args=['--checksum-algorithm', 'CRC32'],
        extra_cli_env={'CHECKSUM_MODE': 'when required; echo INJECTED'},
        get_endpoint_url=lambda: 'https://endpoint; echo INJECTED',
        store_type='S3',
        url_prefix='s3://',
    )
    return store


def test_s3_compatible_base_enforces_provider_config_contract():
    assert inspect.isabstract(storage_lib.S3CompatibleStore)
    with pytest.raises(TypeError, match='abstract class'):
        storage_lib.S3CompatibleStore(  # pylint: disable=abstract-class-instantiated
            'bucket', 's3://bucket')
    for store_name in _PUBLIC_STORE_CONFIGS:
        assert not inspect.isabstract(getattr(storage_lib, store_name))


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


def test_s3_compatible_upload_quotes_paths_and_cli_config():
    store = _store_with_shell_special_cli_config()
    commands = {}

    def capture_commands(_source_paths, file_command_generator,
                         dir_command_generator, *_args, **_kwargs):
        commands['file'] = file_command_generator('/tmp/base dir',
                                                  ['file; echo INJECTED'])
        commands['dir'] = dir_command_generator('/tmp/source dir',
                                                'dest; echo INJECTED')

    with mock.patch.object(
            storage_s3.rich_utils,
            'safe_status',
            return_value=contextlib.nullcontext()), mock.patch.object(
                storage_s3.data_utils,
                'parallel_upload',
                side_effect=capture_commands), mock.patch.object(
                    storage_s3.storage_utils,
                    'get_excluded_files',
                    return_value=[]), mock.patch.object(
                        storage_s3.sky_logging,
                        'generate_tmp_logging_file_path',
                        return_value='/tmp/storage.log'):
        store.batch_aws_rsync(['/tmp/ignored'])

    common_prefix = [
        'CHECKSUM_MODE=when required; echo INJECTED',
        f'AWS_CONFIG_FILE={os.path.expandvars(store.config.config_file)}',
        f'AWS_SHARED_CREDENTIALS_FILE='
        f'{os.path.expanduser(store.config.credentials_file)}',
        'aws',
        's3',
        'sync',
        '--no-follow-symlinks',
    ]
    common_suffix = [
        '--endpoint-url',
        'https://endpoint; echo INJECTED',
        '--profile',
        'profile; echo INJECTED',
        '--checksum-algorithm',
        'CRC32',
    ]
    assert shlex.split(commands['file']) == common_prefix + [
        '--exclude=*',
        '--include',
        'file; echo INJECTED',
        '/tmp/base dir',
        f's3://{store.name}/{store._bucket_sub_path}',
    ] + common_suffix
    assert shlex.split(commands['dir']) == common_prefix + [
        '--exclude',
        '.git/*',
        '/tmp/source dir',
        f's3://{store.name}/{store._bucket_sub_path}/dest; echo INJECTED',
    ] + common_suffix


@pytest.mark.parametrize('method_name, expected_operation, expected_target', [
    ('_delete_bucket', 'rb', 's3://test-bucket'),
    ('_delete_bucket_sub_path', 'rm',
     's3://test-bucket/prefix; echo INJECTED/'),
])
def test_s3_compatible_delete_quotes_targets_and_cli_config(
        method_name, expected_operation, expected_target):
    store = _store_with_shell_special_cli_config()
    store._execute_remove_command = mock.Mock(return_value=True)

    if method_name == '_delete_bucket':
        store._delete_bucket(store.name)
    else:
        store._delete_bucket_sub_path(store.name, store._bucket_sub_path)

    command = store._execute_remove_command.call_args.args[0]
    assert shlex.split(command) == [
        f'AWS_CONFIG_FILE={os.path.expandvars(store.config.config_file)}',
        f'AWS_SHARED_CREDENTIALS_FILE='
        f'{os.path.expanduser(store.config.credentials_file)}',
        'aws',
        's3',
        expected_operation,
        expected_target,
        *(['--force'] if expected_operation == 'rb' else ['--recursive']),
        '--profile',
        'profile; echo INJECTED',
        '--endpoint-url',
        'https://endpoint; echo INJECTED',
    ]
