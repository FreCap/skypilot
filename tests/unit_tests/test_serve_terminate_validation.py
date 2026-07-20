"""Tests for destructive Serve controller request validation."""
# pylint: disable=protected-access

from unittest import mock

import fastapi
import pytest

from sky.serve import controller
from sky.serve import serve_utils


@pytest.mark.asyncio
async def test_payload_requires_valid_json():
    request = mock.Mock()
    request.json = mock.AsyncMock(side_effect=ValueError('invalid JSON'))

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await controller._read_terminate_replica_payload(request)
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize('request_data', [None, [], 'replica', 1, True])
def test_payload_requires_a_json_object(request_data):
    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._validate_terminate_replica_payload(request_data)
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize('replica_id', [True, False, '1', 1.0, None])
def test_replica_id_requires_an_exact_integer(replica_id):
    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._validate_terminate_replica_payload({
            'replica_id': replica_id,
            'purge': False,
        })
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize('purge', ['false', 0, 1, None])
def test_purge_requires_an_exact_boolean(purge):
    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._validate_terminate_replica_payload({
            'replica_id': 1,
            'purge': purge,
        })
    assert exc_info.value.status_code == 400


def test_valid_terminate_payload_is_preserved():
    assert controller._validate_terminate_replica_payload({
        'replica_id': 1,
        'purge': True,
    }) == (1, True)


def test_missing_replica_returns_not_found(monkeypatch):
    lookup = mock.Mock(return_value=None)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        lookup)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._get_replica_info_for_termination('service', 17)
    assert exc_info.value.status_code == 404
    lookup.assert_called_once_with('service', 17)


@pytest.mark.parametrize('body, status_code', [
    ({
        'detail': 'Replica 17 does not exist.'
    }, 404),
    ({
        'detail': 'replica_id must be an integer.'
    }, 400),
    ({
        'message': 'Internal error.'
    }, 500),
])
def test_client_surfaces_controller_error_bodies(monkeypatch, body,
                                                 status_code):
    """The client must tolerate both FastAPI HTTPException bodies
    ({'detail': ...}) and the controller's generic handler bodies
    ({'message': ...}) without crashing."""
    monkeypatch.setattr(serve_utils, '_get_service_status',
                        mock.Mock(return_value={'hash': 'h'}))
    monkeypatch.setattr(serve_utils.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=object()))
    resp = mock.Mock(status_code=status_code)
    resp.json.return_value = body
    monkeypatch.setattr(serve_utils, '_post_to_controller_with_retry',
                        mock.Mock(return_value=resp))

    with pytest.raises(ValueError):
        serve_utils.terminate_replica('service', 17, purge=False)


def test_client_surfaces_non_json_error_body(monkeypatch):
    monkeypatch.setattr(serve_utils, '_get_service_status',
                        mock.Mock(return_value={'hash': 'h'}))
    monkeypatch.setattr(serve_utils.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=object()))
    resp = mock.Mock(status_code=502, text='bad gateway')
    resp.json.side_effect = ValueError('not json')
    monkeypatch.setattr(serve_utils, '_post_to_controller_with_retry',
                        mock.Mock(return_value=resp))

    with pytest.raises(ValueError):
        serve_utils.terminate_replica('service', 17, purge=False)
