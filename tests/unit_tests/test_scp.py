"""Unit tests for the Samsung Cloud Platform API client."""

from unittest import mock

import pytest

from sky.clouds.utils import scp_utils


class _RequestSentinel(Exception):
    """Stops a client method immediately after it issues its request."""


def _make_client():
    credentials = ('access_key = access\nsecret_key = secret\n'
                   'project_id = project\n')
    with mock.patch.object(scp_utils.os.path, 'exists', return_value=True), \
         mock.patch('builtins.open', mock.mock_open(read_data=credentials)):
        client = scp_utils.SCPClient()
    client.access_key = 'access'
    client.secret_key = 'secret'
    client.project_id = 'project'
    client.client_type = 'OpenApi'
    client._signed_headers = mock.MagicMock(  # pylint: disable=protected-access
        return_value={})
    return client


@pytest.mark.parametrize(('client_method', 'request_method', 'args'), [
    ('_get', 'get', ('https://example.com',)),
    ('_post', 'post', ('https://example.com', {})),
    ('_delete', 'delete', ('https://example.com',)),
    ('_delete', 'delete', ('https://example.com', {
        'id': 'resource'
    })),
    ('get_catalog', 'get', ()),
])
def test_scp_requests_have_finite_timeout(client_method, request_method, args):
    """Every provider request must have a finite liveness bound."""
    client = _make_client()

    with mock.patch.object(
            scp_utils.requests, request_method,
            side_effect=_RequestSentinel) as request, pytest.raises(
                _RequestSentinel):
        getattr(client, client_method)(*args)

    assert request.call_args.kwargs[
        'timeout'] == scp_utils.DEFAULT_HTTP_TIMEOUT_SECONDS
