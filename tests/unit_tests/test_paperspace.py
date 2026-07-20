"""Unit tests for the Paperspace API client."""

from unittest import mock

import pytest

from sky.provision.paperspace import utils


class _RequestSentinel(Exception):
    """Stops a client method immediately after it issues its request."""


@pytest.mark.parametrize('method', ['get', 'post', 'put', 'patch', 'delete'])
def test_paperspace_requests_have_finite_timeout(method):
    """Every provider request must have a finite liveness bound."""
    with mock.patch.object(
            utils.requests, method,
            side_effect=_RequestSentinel) as request, pytest.raises(
                _RequestSentinel):
        # pylint: disable=protected-access
        utils._try_request_with_backoff(method, 'https://example.com', {})
        # pylint: enable=protected-access

    assert request.call_args.kwargs[
        'timeout'] == utils.DEFAULT_HTTP_TIMEOUT_SECONDS
