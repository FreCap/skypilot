"""Contracts for the internal expected-UUID SkyServe down fence."""

from typing import Any, Callable
import unittest.mock as mock

import pytest

from sky.client import sdk
from sky.server import constants as server_constants
from sky.server import versions
from sky.server.requests import payloads

_RECORD_UUID = '11111111-1111-4111-8111-111111111111'


def _raw_sdk_down() -> Callable[..., Any]:
    function = sdk.down
    while hasattr(function, '__wrapped__'):
        function = function.__wrapped__
    return function


def test_stop_or_down_body_omits_default_and_maps_internal_kwarg() -> None:
    ordinary = payloads.StopOrDownBody(cluster_name='cluster')
    assert 'resource_action_expected_cluster_record_uuid' not in (
        ordinary.model_dump())
    assert ordinary.to_kwargs() == {
        'cluster_name': 'cluster',
        'purge': False,
        'graceful': False,
        'graceful_timeout': None,
        '_expected_cluster_record_uuid': None,
    }

    fenced = payloads.StopOrDownBody(
        cluster_name='cluster',
        resource_action_expected_cluster_record_uuid=_RECORD_UUID)
    assert fenced.to_kwargs()['_expected_cluster_record_uuid'] == _RECORD_UUID
    assert fenced.model_dump(
    )['resource_action_expected_cluster_record_uuid'] == _RECORD_UUID


def test_sdk_down_sends_fence_only_to_compatible_server(monkeypatch) -> None:
    response = mock.MagicMock(status_code=200,
                              headers={'X-Skypilot-Request-ID': 'request-id'})
    request = mock.MagicMock(return_value=response)
    monkeypatch.setattr(versions, 'get_remote_api_version',
                        lambda: server_constants.API_VERSION)
    monkeypatch.setattr(sdk.server_common, 'make_authenticated_request',
                        request)

    request_id = _raw_sdk_down()('cluster',
                                 _expected_cluster_record_uuid=_RECORD_UUID)

    assert str(request_id) == 'request-id'
    payload = request.call_args.kwargs['json']
    assert payload['resource_action_expected_cluster_record_uuid'] == (
        _RECORD_UUID)


def test_sdk_down_rejects_unfenced_or_noncanonical_internal_request(
        monkeypatch) -> None:
    monkeypatch.setattr(
        versions, 'get_remote_api_version', lambda: server_constants.
        MIN_RESOURCE_ACTION_EXPECTED_CLUSTER_UUID_API_VERSION - 1)
    request = mock.MagicMock()
    monkeypatch.setattr(sdk.server_common, 'make_authenticated_request',
                        request)
    with pytest.raises(RuntimeError, match='cannot preserve'):
        _raw_sdk_down()('cluster', _expected_cluster_record_uuid=_RECORD_UUID)
    request.assert_not_called()

    monkeypatch.setattr(versions, 'get_remote_api_version',
                        lambda: server_constants.API_VERSION)
    with pytest.raises(ValueError, match='canonical'):
        _raw_sdk_down()('cluster',
                        _expected_cluster_record_uuid=_RECORD_UUID.replace(
                            '-', ''))
