"""Characterization tests for the historical request wire façade."""
# pylint: disable=protected-access

import inspect
from unittest import mock

from sky import models
from sky.server import constants as server_constants
from sky.server.requests import payloads
from sky.server.requests import requests
from sky.server.requests.serializers import encoders


def _parameter_shape(function):
    return [(parameter.name, parameter.kind, parameter.default)
            for parameter in inspect.signature(function).parameters.values()]


def _entrypoint():
    return None


def _request(*, status=requests.RequestStatus.WAITING):
    return requests.Request(
        request_id='wire-contract',
        name='wire-contract-request',
        entrypoint=_entrypoint,
        request_body=payloads.RequestBody(),
        status=status,
        created_at=123.5,
        user_id='user-1',
        cluster_name='cluster-1',
        status_msg='waiting',
        should_retry=True,
        finished_at=None,
        file_mounts_blob_id='blob-1',
    )


def test_wire_facade_identity_and_signatures():
    assert requests.encode_requests.__module__ == requests.__name__
    assert requests.encode_requests.__qualname__ == 'encode_requests'
    assert _parameter_shape(requests.encode_requests) == [
        ('requests', inspect.Parameter.POSITIONAL_OR_KEYWORD,
         inspect.Parameter.empty)
    ]

    expected_methods = {
        'readable_encode': [('self', inspect.Parameter.POSITIONAL_OR_KEYWORD,
                             inspect.Parameter.empty)],
        'encode': [('self', inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.empty)],
        '_decode_entrypoint': [
            ('encoded_entrypoint', inspect.Parameter.POSITIONAL_OR_KEYWORD,
             inspect.Parameter.empty)
        ],
        'decode': [('payload', inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.empty)],
    }
    for name, expected_shape in expected_methods.items():
        method = getattr(requests.Request, name)
        assert method.__module__ == requests.__name__
        assert method.__qualname__ == f'Request.{name}'
        assert _parameter_shape(method) == expected_shape


def test_wire_projection_round_trip_and_late_bound_seams(monkeypatch):
    user_lookup = mock.Mock(
        return_value=[models.User(id='user-1', name='Wire User')])
    version_lookup = mock.Mock(
        return_value=server_constants.MIN_WAITING_STATUS_API_VERSION - 1)
    monkeypatch.setattr(requests.global_user_state, 'get_all_users',
                        user_lookup)
    monkeypatch.setattr(requests.versions, 'get_remote_api_version',
                        version_lookup)

    request = _request()
    display_payload = request.readable_encode()
    full_payload = request.encode()
    decoded = requests.Request.decode(full_payload)

    assert display_payload.status == requests.RequestStatus.RUNNING.value
    assert display_payload.user_name == 'Wire User'
    assert display_payload.entrypoint == '_entrypoint'
    assert display_payload.request_body == request.request_body.model_dump_json(
    )
    assert display_payload.return_value == 'null'
    assert display_payload.error == 'null'
    assert display_payload.pid is None
    assert display_payload.file_mounts_blob_id == 'blob-1'
    assert full_payload.status == requests.RequestStatus.RUNNING.value
    assert decoded.request_id == request.request_id
    assert decoded.entrypoint is _entrypoint
    assert decoded.status is requests.RequestStatus.RUNNING
    assert decoded.schedule_type is requests.ScheduleType.LONG
    assert decoded.request_body == request.request_body
    assert decoded.file_mounts_blob_id == request.file_mounts_blob_id
    user_lookup.assert_called_once_with()
    assert version_lookup.call_count == 2


def test_empty_display_projection_preserves_one_batched_user_lookup(
        monkeypatch):
    user_lookup = mock.Mock(return_value=[])
    monkeypatch.setattr(requests.global_user_state, 'get_all_users',
                        user_lookup)
    assert not requests.encode_requests([])
    user_lookup.assert_called_once_with()


def test_decode_entrypoint_uses_late_bound_decoder_and_placeholder(monkeypatch):
    encoded = encoders.pickle_and_encode(_entrypoint)
    decoder = mock.Mock(side_effect=AttributeError('newer symbol'))
    monkeypatch.setattr(requests.decoders, 'decode_and_unpickle', decoder)

    assert requests.Request._decode_entrypoint(
        encoded) is requests._unresolved_entrypoint
    decoder.assert_called_once_with(encoded)
