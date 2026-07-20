"""Unit tests for the Fluidstack API client."""

from unittest import mock

import pytest

from sky.provision.fluidstack import fluidstack_utils


class _RequestSentinel(Exception):
    """Stops a client method immediately after it issues its request."""


@pytest.mark.parametrize(('client_method', 'request_method', 'args'), [
    ('get_plans', 'get', ()),
    ('list_instances', 'get', ()),
    ('create_instance', 'post',
     ('A100::1', 'cluster', 'us-east', 'ssh-rsa public-key', 1)),
    ('list_ssh_keys', 'get', ()),
    ('get_or_add_ssh_key', 'post', ('ssh-rsa public-key',)),
    ('delete', 'delete', ('instance-id',)),
    ('stop', 'put', ('instance-id',)),
    ('rename', 'put', ('instance-id', 'new-name')),
])
def test_fluidstack_requests_have_finite_timeout(client_method, request_method,
                                                 args):
    """Every provider request must have a finite liveness bound."""
    client = fluidstack_utils.FluidstackClient.__new__(
        fluidstack_utils.FluidstackClient)
    client.api_key = 'api-key'
    if client_method == 'create_instance':
        client.get_plans = mock.MagicMock(return_value=[{
            'gpu_type': 'A100',
            'gpu_counts': [1],
            'regions': ['us-east'],
        }])
        client.list_regions = mock.MagicMock(
            return_value={'us-east': 'us-east'})
        client.get_or_add_ssh_key = mock.MagicMock(return_value={'name': 'key'})
    elif client_method == 'get_or_add_ssh_key':
        client.list_ssh_keys = mock.MagicMock(return_value=[])

    with mock.patch.object(
            fluidstack_utils.requests, request_method,
            side_effect=_RequestSentinel) as request, pytest.raises(
                _RequestSentinel):
        getattr(client, client_method)(*args)

    assert request.call_args.kwargs[
        'timeout'] == fluidstack_utils.DEFAULT_HTTP_TIMEOUT_SECONDS
