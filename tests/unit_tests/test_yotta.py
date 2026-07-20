"""Unit tests for the Yotta API client."""

from unittest import mock

import pytest

from sky.provision.yotta import yotta_utils


class _RequestSentinel(Exception):
    """Stops a client method immediately after it issues its request."""


@pytest.mark.parametrize(('client_method', 'request_method', 'args'), [
    ('check_api_key', 'get', ()),
    ('list_instances', 'post', ('cluster',)),
    ('create_cluster', 'post', ('cluster', 'gpu-type', 'region', 'image', None,
                                100, 'public-key', 'root', 1)),
    ('get_cluster_status', 'get', ('cluster-id',)),
    ('launch', 'post',
     ('cluster', 'cluster-id', 'pod', 'image', None, None, 'public-key')),
    ('terminate_instances', 'post', ('cluster',)),
])
def test_yotta_requests_have_finite_timeout(client_method, request_method,
                                            args):
    """Every provider request must have a finite liveness bound."""
    client = yotta_utils.YottaClient()
    client._org_id = 'org-id'  # pylint: disable=protected-access
    client._api_key = 'api-key'  # pylint: disable=protected-access

    with mock.patch.object(
            yotta_utils.requests, request_method,
            side_effect=_RequestSentinel) as request, pytest.raises(
                _RequestSentinel):
        getattr(client, client_method)(*args)

    assert request.call_args.kwargs[
        'timeout'] == yotta_utils.DEFAULT_HTTP_TIMEOUT_SECONDS
