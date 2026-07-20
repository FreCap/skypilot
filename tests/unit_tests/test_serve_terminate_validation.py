"""Tests for destructive Serve controller request validation."""
# pylint: disable=protected-access

from unittest import mock

import fastapi
import pytest

from sky.serve import controller


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
    assert exc_info.value.detail == 'Replica 17 does not exist.'
    lookup.assert_called_once_with('service', 17)
