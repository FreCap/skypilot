"""Tests for vSphere Content Library HTTP transfers."""

from types import SimpleNamespace
from unittest import mock

import pytest

from sky.provision.vsphere.common import cls_api_helper


class _RequestSentinel(Exception):
    """Stops a transfer immediately after it opens the HTTP request."""


@pytest.mark.parametrize('skip_verification', [False, True])
def test_upload_transfer_has_finite_timeout(tmp_path, skip_verification):
    upload_file = tmp_path / 'template.ovf'
    upload_file.write_text('template', encoding='utf-8')
    client = mock.MagicMock()
    client.upload_file_service.add.return_value = SimpleNamespace(
        upload_endpoint=SimpleNamespace(uri='https://example.com/upload'))
    helper = cls_api_helper.ClsApiHelper(client, skip_verification)

    updatesession_client = mock.MagicMock()
    with mock.patch.object(
            cls_api_helper.vsphere_adaptor,
            'get_updatesession_client',
            return_value=updatesession_client), mock.patch.object(
                cls_api_helper.urllib2, 'urlopen',
                side_effect=_RequestSentinel) as urlopen, pytest.raises(
                    _RequestSentinel):
        helper.upload_files_in_session({'template.ovf': str(upload_file)},
                                       'session-id')

    assert urlopen.call_args.kwargs[
        'timeout'] == cls_api_helper.TRANSFER_HTTP_TIMEOUT_SECONDS
    assert ('context' in urlopen.call_args.kwargs) is skip_verification


def test_failed_upload_deletes_session():
    client = mock.MagicMock()
    client.upload_service.create.return_value = 'session-id'
    helper = cls_api_helper.ClsApiHelper(client, skip_verification=False)
    helper.upload_files_in_session = mock.MagicMock(
        side_effect=RuntimeError('upload failed'))

    with mock.patch.object(cls_api_helper.vsphere_adaptor,
                           'get_item_client',
                           return_value=mock.MagicMock()), pytest.raises(
                               RuntimeError, match='upload failed'):
        helper.upload_files('library-item-id', {})

    client.upload_service.delete.assert_called_once_with('session-id')


@pytest.mark.parametrize('skip_verification', [False, True])
def test_download_transfer_has_finite_timeout(tmp_path, skip_verification):
    client = mock.MagicMock()
    client.download_service.create.return_value = 'session-id'
    client.download_file_service.list.return_value = [
        SimpleNamespace(name='template.ovf')
    ]
    helper = cls_api_helper.ClsApiHelper(client, skip_verification)
    helper.wait_for_prepare = mock.MagicMock(return_value=SimpleNamespace(
        download_endpoint=SimpleNamespace(uri='https://example.com/download')))

    with mock.patch.object(
            cls_api_helper.vsphere_adaptor,
            'get_item_client',
            return_value=mock.MagicMock()), mock.patch.object(
                cls_api_helper.urllib2, 'urlopen',
                side_effect=_RequestSentinel) as urlopen, pytest.raises(
                    _RequestSentinel):
        helper.download_files('library-item-id', str(tmp_path))

    assert urlopen.call_args.kwargs[
        'timeout'] == cls_api_helper.TRANSFER_HTTP_TIMEOUT_SECONDS
    assert ('context' in urlopen.call_args.kwargs) is skip_verification
    client.download_service.delete.assert_called_once_with('session-id')


def test_upload_closes_response(tmp_path):
    upload_file = tmp_path / 'template.ovf'
    upload_file.write_text('template', encoding='utf-8')
    response = mock.MagicMock()
    client = mock.MagicMock()
    client.upload_file_service.add.return_value = SimpleNamespace(
        upload_endpoint=SimpleNamespace(uri='https://example.com/upload'))
    helper = cls_api_helper.ClsApiHelper(client, skip_verification=False)

    with mock.patch.object(cls_api_helper.vsphere_adaptor,
                           'get_updatesession_client',
                           return_value=mock.MagicMock()), mock.patch.object(
                               cls_api_helper.urllib2,
                               'urlopen',
                               return_value=response):
        helper.upload_files_in_session({'template.ovf': str(upload_file)},
                                       'session-id')

    response.close.assert_called_once_with()


def test_download_closes_response(tmp_path):
    response = mock.MagicMock()
    response.read.return_value = b'template'
    client = mock.MagicMock()
    client.download_service.create.return_value = 'session-id'
    client.download_file_service.list.return_value = [
        SimpleNamespace(name='template.ovf')
    ]
    helper = cls_api_helper.ClsApiHelper(client, skip_verification=False)
    helper.wait_for_prepare = mock.MagicMock(return_value=SimpleNamespace(
        download_endpoint=SimpleNamespace(uri='https://example.com/download')))

    with mock.patch.object(cls_api_helper.vsphere_adaptor,
                           'get_item_client',
                           return_value=mock.MagicMock()), mock.patch.object(
                               cls_api_helper.urllib2,
                               'urlopen',
                               return_value=response):
        helper.download_files('library-item-id', str(tmp_path))

    assert (tmp_path / 'template.ovf').read_bytes() == b'template'
    response.close.assert_called_once_with()
