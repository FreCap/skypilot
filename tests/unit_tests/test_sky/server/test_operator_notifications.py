"""Tests for the operator notification HTTP contract."""
from types import SimpleNamespace

import fastapi
from fastapi.testclient import TestClient
import pytest

from sky.server import server


def test_notification_history_and_cursor_endpoints(monkeypatch):
    monkeypatch.setattr(server.common_utils, 'get_user_hash',
                        lambda: 'local-operator')
    monkeypatch.setattr(server.time, 'time', lambda: 1_000_000)

    def get_notifications(user_id, since):
        assert user_id == 'local-operator'
        assert since == 1_000_000 - 3 * 24 * 60 * 60
        return {
            'notifications': [],
            'unread_count': 0,
            'latest_sequence': 4,
            'last_seen_sequence': 4,
        }

    monkeypatch.setattr(server.global_user_state, 'get_operator_notifications',
                        get_notifications)
    monkeypatch.setattr(
        server.global_user_state,
        'mark_operator_notifications_read',
        lambda user_id, sequence: sequence
        if user_id == 'local-operator' else 0,
    )

    client = TestClient(server.app)
    response = client.get('/notifications', params={'days': 3})
    assert response.status_code == 200
    assert response.json()['latest_sequence'] == 4

    response = client.post('/notifications/read', json={'through_sequence': 4})
    assert response.status_code == 200
    assert response.json() == {'last_seen_sequence': 4}


def test_notification_endpoints_reject_invalid_input_and_non_admin(monkeypatch):
    client = TestClient(server.app)
    assert client.get('/notifications', params={'days': 0}).status_code == 422
    assert client.post('/notifications/read', json={
        'through_sequence': -1
    }).status_code == 422

    request = SimpleNamespace(state=SimpleNamespace(auth_user=SimpleNamespace(
        id='operator')))
    monkeypatch.setattr(server.permission.permission_service, 'get_user_roles',
                        lambda _: ['user'])
    with pytest.raises(fastapi.HTTPException, match='Only admins') as exc_info:
        # pylint: disable=protected-access
        server._operator_notification_user_id(request)
    assert exc_info.value.status_code == 403
