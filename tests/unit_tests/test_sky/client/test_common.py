"""Unit tests for sky/client/common.py."""
import os
import re
from unittest import mock

import pytest

from sky.client import common as client_common
from sky.client.common import _compute_zip_blob_id
from sky.data import storage_utils


def test_file_upload_timeout_bounds_every_network_phase():
    timeout = client_common._file_upload_http_timeout()  # pylint: disable=protected-access

    assert timeout.connect == client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS
    assert timeout.read == client_common.FILE_UPLOAD_HTTP_TIMEOUT_SECONDS
    assert timeout.write == client_common.FILE_UPLOAD_HTTP_TIMEOUT_SECONDS
    assert timeout.pool == client_common.FILE_UPLOAD_HTTP_TIMEOUT_SECONDS


def test_setup_upload_logger_preserves_file_handler_error(monkeypatch):

    def raise_file_handler_error(*args, **kwargs):
        del args, kwargs
        raise OSError('disk full')

    monkeypatch.setattr(client_common.logging, 'FileHandler',
                        raise_file_handler_error)

    with pytest.raises(OSError, match='disk full'):
        with client_common._setup_upload_logger(  # pylint: disable=protected-access
                '/tmp/upload.log'):
            pass


def test_download_logs_resolves_remote_prefix_per_call():
    response = mock.MagicMock(status_code=200,
                              headers={'X-Home-Path': '/server-home'})
    response.iter_content.return_value = []
    zip_file = mock.MagicMock()
    zip_file.__enter__.return_value.namelist.return_value = []

    with mock.patch.object(
            client_common.server_common,
            'api_server_user_logs_dir_prefix',
            return_value='/fresh-user/sky_logs') as mock_prefix, \
         mock.patch.object(client_common.server_common,
                           'make_authenticated_request',
                           return_value=response) as mock_request, \
         mock.patch.object(client_common.zipfile,
                           'ZipFile',
                           return_value=zip_file):
        result = client_common.download_logs_from_api_server(
            ['/fresh-user/sky_logs/job-1'], local_machine_prefix='/local-logs')

    assert result == {'/fresh-user/sky_logs/job-1': '/local-logs/job-1'}
    mock_prefix.assert_called_once_with()
    assert mock_request.call_args.kwargs['timeout'] == (
        client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS, None)
    response.close.assert_called_once_with()


def test_download_logs_preserves_explicit_remote_prefix():
    response = mock.MagicMock(status_code=200,
                              headers={'X-Home-Path': '/server-home'})
    response.iter_content.return_value = []
    zip_file = mock.MagicMock()
    zip_file.__enter__.return_value.namelist.return_value = []

    with mock.patch.object(
            client_common.server_common,
            'api_server_user_logs_dir_prefix') as mock_prefix, \
         mock.patch.object(client_common.server_common,
                           'make_authenticated_request',
                           return_value=response), \
         mock.patch.object(client_common.zipfile,
                           'ZipFile',
                           return_value=zip_file):
        result = client_common.download_logs_from_api_server(
            ['/explicit/sky_logs/job-1'],
            remote_machine_prefix='/explicit/sky_logs',
            local_machine_prefix='/local-logs')

    assert result == {'/explicit/sky_logs/job-1': '/local-logs/job-1'}
    mock_prefix.assert_not_called()


def test_download_logs_rejects_missing_remote_home_header():
    response = mock.MagicMock(status_code=200, headers={})

    with mock.patch.object(client_common.server_common,
                           'make_authenticated_request',
                           return_value=response):
        with pytest.raises(
                RuntimeError,
                match='/download response missing X-Home-Path header'):
            client_common.download_logs_from_api_server(
                ['/server-home/sky_logs/job-1'],
                remote_machine_prefix='/server-home/sky_logs',
                local_machine_prefix='/local-logs')

    response.iter_content.assert_not_called()
    response.close.assert_called_once_with()


def test_download_logs_closes_response_when_streaming_fails():
    response = mock.MagicMock(status_code=200,
                              headers={'X-Home-Path': '/server-home'})
    response.iter_content.side_effect = OSError('connection reset')

    with mock.patch.object(client_common.server_common,
                           'make_authenticated_request',
                           return_value=response):
        with pytest.raises(OSError, match='connection reset'):
            client_common.download_logs_from_api_server(
                ['/server-home/sky_logs/job-1'],
                remote_machine_prefix='/server-home/sky_logs',
                local_machine_prefix='/local-logs')

    response.close.assert_called_once_with()


def test_download_logs_closes_failed_response():
    response = mock.MagicMock(status_code=503, text='service unavailable')

    with mock.patch.object(client_common.server_common,
                           'make_authenticated_request',
                           return_value=response):
        with pytest.raises(
                Exception,
                match='Failed to download logs: 503 service unavailable'):
            client_common.download_logs_from_api_server(
                ['/server-home/sky_logs/job-1'],
                remote_machine_prefix='/server-home/sky_logs',
                local_machine_prefix='/local-logs')

    response.iter_content.assert_not_called()
    response.close.assert_called_once_with()


def test_blob_id_determinism(tmp_path):
    f = tmp_path / 'hello.txt'
    f.write_text('hello world')

    zip1 = str(tmp_path / 'a.zip')
    zip2 = str(tmp_path / 'b.zip')
    storage_utils.zip_files_and_folders([str(f)], zip1)
    storage_utils.zip_files_and_folders([str(f)], zip2)

    result1 = _compute_zip_blob_id(zip1)
    result2 = _compute_zip_blob_id(zip2)

    assert result1 == result2
    assert len(result1) == 64
    assert re.fullmatch(r'[0-9a-f]{64}', result1)


def test_blob_id_content_sensitivity(tmp_path):
    f = tmp_path / 'data.txt'
    f.write_text('version 1')
    zip1 = str(tmp_path / 'v1.zip')
    storage_utils.zip_files_and_folders([str(f)], zip1)
    hash1 = _compute_zip_blob_id(zip1)

    f.write_text('version 2')
    zip2 = str(tmp_path / 'v2.zip')
    storage_utils.zip_files_and_folders([str(f)], zip2)
    hash2 = _compute_zip_blob_id(zip2)

    assert hash1 != hash2


def test_blob_id_order_independence(tmp_path):
    dir_a = tmp_path / 'a'
    dir_a.mkdir()
    (dir_a / 'file_a.txt').write_text('aaa')

    dir_b = tmp_path / 'b'
    dir_b.mkdir()
    (dir_b / 'file_b.txt').write_text('bbb')

    zip1 = str(tmp_path / 'ab.zip')
    storage_utils.zip_files_and_folders([str(dir_a), str(dir_b)], zip1)
    hash1 = _compute_zip_blob_id(zip1)

    zip2 = str(tmp_path / 'ba.zip')
    storage_utils.zip_files_and_folders([str(dir_b), str(dir_a)], zip2)
    hash2 = _compute_zip_blob_id(zip2)

    assert hash1 == hash2


def test_blob_id_symlink_handling(tmp_path):
    content_dir = tmp_path / 'content'
    content_dir.mkdir()
    target = content_dir / 'target.txt'
    target.write_text('real content')
    link = content_dir / 'link.txt'
    link.symlink_to(target)

    zip1 = str(tmp_path / 'z1.zip')
    storage_utils.zip_files_and_folders([str(content_dir)], zip1)
    hash_val = _compute_zip_blob_id(zip1)
    assert len(hash_val) == 64
    assert re.fullmatch(r'[0-9a-f]{64}', hash_val)

    # Changing the symlink target name changes the hash, even if content is
    # the same, because symlinks hash the target path not the content.
    target2 = content_dir / 'target2.txt'
    target2.write_text('real content')
    os.remove(str(link))
    link.symlink_to(target2)

    zip2 = str(tmp_path / 'z2.zip')
    storage_utils.zip_files_and_folders([str(content_dir)], zip2)
    hash_val2 = _compute_zip_blob_id(zip2)
    assert hash_val2 != hash_val


def test_blob_id_directory_handling(tmp_path):
    content_dir = tmp_path / 'content'
    content_dir.mkdir()
    empty_dir = content_dir / 'empty'
    empty_dir.mkdir()

    zip1 = str(tmp_path / 'z1.zip')
    zip2 = str(tmp_path / 'z2.zip')
    storage_utils.zip_files_and_folders([str(content_dir)], zip1)
    storage_utils.zip_files_and_folders([str(content_dir)], zip2)

    hash1 = _compute_zip_blob_id(zip1)
    hash2 = _compute_zip_blob_id(zip2)

    assert hash1 == hash2
    assert len(hash1) == 64


def test_blob_id_empty_input(tmp_path):
    zip_path = str(tmp_path / 'empty.zip')
    storage_utils.zip_files_and_folders([], zip_path)
    hash_val = _compute_zip_blob_id(zip_path)
    assert len(hash_val) == 64
    assert re.fullmatch(r'[0-9a-f]{64}', hash_val)
