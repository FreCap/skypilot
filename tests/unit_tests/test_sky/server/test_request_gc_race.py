"""Tests for request-retention races in API routes."""

from fastapi.testclient import TestClient

from sky.server import server
from sky.server.requests import requests as requests_lib


def test_api_get_returns_not_found_if_request_is_garbage_collected(monkeypatch):
    """A terminal request can be deleted between the status and row reads."""
    request_id = 'garbage-collected-request'

    async def fake_expand(request_id_prefix, owner_user_id=None):
        del owner_user_id
        return request_id_prefix

    async def fake_status(request_id_to_check, include_msg=False):
        del request_id_to_check, include_msg
        return requests_lib.StatusWithMsg(
            status=requests_lib.RequestStatus.SUCCEEDED)

    async def fake_get_request(request_id_to_get):
        del request_id_to_get
        return None

    monkeypatch.setattr(server, 'get_expanded_request_id', fake_expand)
    monkeypatch.setattr(requests_lib, 'get_request_status_async', fake_status)
    monkeypatch.setattr(requests_lib, 'get_request_async', fake_get_request)

    client = TestClient(server.app, raise_server_exceptions=False)
    response = client.get('/api/get', params={'request_id': request_id})

    assert response.status_code == 404
    assert response.json() == {'detail': f'Request {request_id!r} not found'}
