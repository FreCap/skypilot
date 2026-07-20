"""Tests for Shadeform API utilities."""

from unittest import mock

from sky.provision.shadeform import shadeform_utils


def test_make_request_has_default_timeout() -> None:
    response = mock.Mock()
    response.text = ''

    with mock.patch.object(shadeform_utils, 'get_api_key', return_value='key'), \
         mock.patch.object(shadeform_utils.requests,
                           'request',
                           return_value=response) as request:
        shadeform_utils.make_request('GET', '/instances')

    request.assert_called_once_with(
        'GET',
        f'{shadeform_utils.SHADEFORM_API_BASE}/instances',
        headers={
            'X-API-KEY': 'key',
            'Content-Type': 'application/json',
        },
        timeout=shadeform_utils.DEFAULT_HTTP_TIMEOUT_SECONDS)


def test_make_request_preserves_explicit_timeout() -> None:
    response = mock.Mock()
    response.text = ''

    with mock.patch.object(shadeform_utils, 'get_api_key', return_value='key'), \
         mock.patch.object(shadeform_utils.requests,
                           'request',
                           return_value=response) as request:
        shadeform_utils.make_request('GET', '/instances', timeout=15)

    assert request.call_args.kwargs['timeout'] == 15
