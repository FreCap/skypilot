"""Tests for common provisioning data structures."""
import inspect

import pytest

from sky.provision import common


def test_endpoint_base_enforces_url_contract():
    assert inspect.isabstract(common.Endpoint)
    with pytest.raises(TypeError, match='abstract class'):
        common.Endpoint()  # pylint: disable=abstract-class-instantiated
    endpoint = common.SocketEndpoint(host='127.0.0.1', port=22)
    assert endpoint.url() == '127.0.0.1:22'
