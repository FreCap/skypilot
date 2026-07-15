"""Characterization tests for rclone configuration utilities."""

import pickle
import subprocess
import textwrap
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import exceptions
from sky.data import data_utils


@pytest.mark.parametrize(('store', 'prefix'), [
    (data_utils.Rclone.RcloneStores.S3, 'sky-s3'),
    (data_utils.Rclone.RcloneStores.GCS, 'sky-gcs'),
    (data_utils.Rclone.RcloneStores.IBM, 'sky-ibm'),
    (data_utils.Rclone.RcloneStores.R2, 'sky-r2'),
    (data_utils.Rclone.RcloneStores.AZURE, 'sky-azure'),
    (data_utils.Rclone.RcloneStores.NEBIUS, 'sky-nebius'),
    (data_utils.Rclone.RcloneStores.COREWEAVE, 'sky-coreweave'),
    (data_utils.Rclone.RcloneStores.VASTDATA, 'sky-vastdata'),
    (data_utils.Rclone.RcloneStores.OCI, 'sky-oci'),
])
def test_rclone_profile_names(store, prefix):
    assert store.get_profile_name('bucket') == f'{prefix}-bucket'


def test_rclone_public_identity_and_pickle():
    assert data_utils.Rclone.__module__ == 'sky.data.data_utils'
    assert data_utils.Rclone.RcloneStores.__module__ == 'sky.data.data_utils'
    store = data_utils.Rclone.RcloneStores.S3
    assert pickle.loads(pickle.dumps(store)) is store


def test_rclone_s3_environment_auth_config(monkeypatch):
    monkeypatch.setattr(data_utils.clouds.AWS, 'should_use_env_auth_for_s3',
                        lambda: True)

    config = data_utils.Rclone.RcloneStores.S3.get_config(
        rclone_profile_name='profile')

    assert config == textwrap.dedent("""\
        [profile]
        type = s3
        provider = AWS
        env_auth = true
        acl = private
        """)


def test_rclone_s3_static_credentials_config(monkeypatch):
    monkeypatch.setattr(data_utils.clouds.AWS, 'should_use_env_auth_for_s3',
                        lambda: False)
    credentials = SimpleNamespace(access_key='access', secret_key='secret')
    session = mock.Mock()
    session.get_credentials.return_value.get_frozen_credentials.return_value = (
        credentials)
    monkeypatch.setattr(data_utils.aws, 'session', lambda: session)

    config = data_utils.Rclone.RcloneStores.S3.get_config(
        rclone_profile_name='profile')

    assert config == textwrap.dedent("""\
        [profile]
        type = s3
        provider = AWS
        access_key_id = access
        secret_access_key = secret
        acl = private
        """)


def test_rclone_gcs_config(monkeypatch):
    monkeypatch.setattr(data_utils.clouds.GCP, 'get_project_id',
                        lambda: 'project')

    config = data_utils.Rclone.RcloneStores.GCS.get_config(
        rclone_profile_name='profile')

    assert config == textwrap.dedent("""\
        [profile]
        type = google cloud storage
        project_number = project
        bucket_policy_only = true
        """)


def test_rclone_ibm_config(monkeypatch):
    monkeypatch.setattr(data_utils.ibm, 'get_hmac_keys', lambda:
                        ('access', 'secret'))

    config = data_utils.Rclone.RcloneStores.IBM.get_config(
        rclone_profile_name='profile', region='eu-de')

    assert config == textwrap.dedent("""\
        [profile]
        type = s3
        provider = IBMCOS
        access_key_id = access
        secret_access_key = secret
        region = eu-de
        endpoint = s3.eu-de.cloud-object-storage.appdomain.cloud
        location_constraint = eu-de-smart
        acl = private
        """)


def test_rclone_r2_config(monkeypatch):
    credentials = SimpleNamespace(access_key='access', secret_key='secret')
    monkeypatch.setattr(data_utils.cloudflare, 'session',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(data_utils.cloudflare, 'get_r2_credentials',
                        lambda _: credentials)
    monkeypatch.setattr(data_utils.cloudflare, 'create_endpoint',
                        lambda: 'https://r2.example')

    config = data_utils.Rclone.RcloneStores.R2.get_config(
        rclone_profile_name='profile')

    assert config == textwrap.dedent("""\
        [profile]
        type = s3
        provider = Cloudflare
        access_key_id = access
        secret_access_key = secret
        endpoint = https://r2.example
        region = auto
        acl = private
        """)


def test_rclone_azure_config():
    config = data_utils.Rclone.RcloneStores.AZURE.get_config(
        rclone_profile_name='profile',
        storage_account_name='account',
        storage_account_key='key')

    assert config == textwrap.dedent("""\
        [profile]
        type = azureblob
        account = account
        key = key
        """)


@pytest.mark.parametrize(
    ('store', 'adapter_name', 'force_path_style', 'region'), [
        (data_utils.Rclone.RcloneStores.NEBIUS, 'nebius', None, None),
        (data_utils.Rclone.RcloneStores.COREWEAVE, 'coreweave', 'false',
         'auto'),
        (data_utils.Rclone.RcloneStores.VASTDATA, 'vastdata', None, 'auto'),
        (data_utils.Rclone.RcloneStores.OCI, 'oci_s3', 'true', 'us-phoenix-1'),
    ])
def test_rclone_s3_compatible_configs(monkeypatch, store, adapter_name,
                                      force_path_style, region):
    adapter = getattr(data_utils, adapter_name)
    credentials = SimpleNamespace(access_key='access', secret_key='secret')
    monkeypatch.setattr(adapter, 'session', mock.Mock(return_value=object()))
    credential_getter = {
        'nebius': 'get_nebius_credentials',
        'coreweave': 'get_coreweave_credentials',
        'vastdata': 'get_vastdata_credentials',
        'oci_s3': 'get_oci_s3_credentials',
    }[adapter_name]
    monkeypatch.setattr(adapter, credential_getter, lambda _: credentials)
    if adapter_name == 'nebius':
        client = SimpleNamespace(meta=SimpleNamespace(
            endpoint_url='https://objects.example'))
        monkeypatch.setattr(adapter, 'client', lambda _: client)
    else:
        monkeypatch.setattr(adapter, 'get_endpoint',
                            lambda: 'https://objects.example')
    if adapter_name == 'oci_s3':
        monkeypatch.setattr(adapter, 'get_region', lambda: region)

    config = store.get_config(rclone_profile_name='profile')

    assert '[profile]\n' in config
    assert 'access_key_id = access\n' in config
    assert 'secret_access_key = secret\n' in config
    assert 'endpoint = https://objects.example\n' in config
    if force_path_style is not None:
        assert f'force_path_style = {force_path_style}\n' in config
    if region is not None:
        assert f'region = {region}\n' in config


def test_rclone_profile_file_queries_and_removal(tmp_path, monkeypatch):
    config_path = tmp_path / 'rclone.conf'
    config_path.write_text(textwrap.dedent("""\
        # top-level comment
        [sky-ibm-target]
        type = s3
        region = eu-de
        # profile comment
        [keep]
        type = s3
        region = us-east
        """),
                           encoding='utf-8')
    monkeypatch.setattr(data_utils.constants, 'RCLONE_CONFIG_PATH',
                        str(config_path))

    store = data_utils.Rclone.RcloneStores.IBM
    assert data_utils.Rclone.get_region_from_rclone('target', store) == 'eu-de'
    remove_profile = getattr(data_utils.Rclone, '_remove_bucket_profile_rclone')
    assert remove_profile('target', store) == [
        '# top-level comment\n', '[keep]\n', 'type = s3\n', 'region = us-east\n'
    ]


def test_store_rclone_config_replaces_existing_profile(tmp_path, monkeypatch):
    config_path = tmp_path / 'rclone.conf'
    config_path.write_text('[sky-ibm-target]\nold = value\n[keep]\nx = y\n',
                           encoding='utf-8')
    monkeypatch.setattr(data_utils.constants, 'RCLONE_CONFIG_PATH',
                        str(config_path))
    monkeypatch.setattr(data_utils.subprocess, 'run', mock.Mock())
    monkeypatch.setattr(data_utils.Rclone.RcloneStores, 'get_config',
                        lambda self, **_: '[sky-ibm-target]\nnew = value\n')

    result = data_utils.Rclone.store_rclone_config(
        'target', data_utils.Rclone.RcloneStores.IBM, 'eu-de')

    assert result == '[sky-ibm-target]\nnew = value\n\n'
    assert config_path.read_text(encoding='utf-8') == (
        '[keep]\nx = y\n[sky-ibm-target]\nnew = value\n\n')


def test_store_rclone_config_reports_missing_binary(tmp_path, monkeypatch):
    monkeypatch.setattr(data_utils.constants, 'RCLONE_CONFIG_PATH',
                        str(tmp_path / 'rclone.conf'))
    monkeypatch.setattr(data_utils.Rclone.RcloneStores, 'get_config',
                        lambda self, **_: '[profile]\n')
    monkeypatch.setattr(
        data_utils.subprocess, 'run',
        mock.Mock(side_effect=subprocess.CalledProcessError(1, 'rclone')))

    with pytest.raises(exceptions.StorageError, match="rclone wasn't detected"):
        data_utils.Rclone.store_rclone_config(
            'target', data_utils.Rclone.RcloneStores.IBM, 'eu-de')
