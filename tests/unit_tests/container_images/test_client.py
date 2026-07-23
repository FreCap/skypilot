"""Tests for the managed container image client."""
# pylint: disable=protected-access

from unittest import mock

import pytest

from sky.container_images import api_models
from sky.container_images import client


@pytest.mark.parametrize('idempotency_key', [None, 'operation-key-123'])
def test_request_only_passes_mapping_headers(
        monkeypatch: pytest.MonkeyPatch, idempotency_key: str | None) -> None:
    response = mock.Mock(status_code=200)
    response.json.return_value = {
        'version': 1,
        'items': [],
        'next_cursor': None,
    }
    request = mock.Mock(return_value=response)
    monkeypatch.setattr(client.server_common, 'make_authenticated_request',
                        request)

    result = client._request('GET',
                             '/images/catalog',
                             api_models.Page,
                             idempotency_key=idempotency_key)

    assert result == api_models.Page(items=[])
    if idempotency_key is None:
        assert 'headers' not in request.call_args.kwargs
    else:
        assert request.call_args.kwargs['headers'] == {
            'Idempotency-Key': idempotency_key,
        }
