"""Tests for service-scoped placement observability assembly."""
from unittest import mock

import pytest

from sky.serve.server import core
from sky.server.requests import payloads


def test_placement_uses_exact_service_incarnation(monkeypatch):
    monkeypatch.setattr(core.serve_state, 'get_service_from_name', lambda _: {
        'hash': 'hash-a',
        'pool': False
    })
    placer = mock.Mock(return_value={'available': True, 'locations': []})
    capacity = mock.Mock(return_value={'available': True, 'hints': []})
    history = mock.Mock(return_value={'available': True, 'events': []})
    monkeypatch.setattr(core.serve_utils, 'get_service_placement_state', placer)
    monkeypatch.setattr(core.capacity_cache, 'active_service_observations',
                        capacity)
    monkeypatch.setattr(core.placement_history, 'get_history', history)

    result = core.placement('svc',
                            hours=12,
                            limit=10,
                            cursor='cursor-a',
                            location_limit=25,
                            location_offset=50)

    placer.assert_called_once_with('svc', 'hash-a', limit=25, offset=50)
    capacity.assert_called_once_with('svc', 'hash-a')
    history.assert_called_once_with('svc',
                                    'hash-a',
                                    hours=12,
                                    limit=10,
                                    cursor='cursor-a')
    assert result['service_name'] == 'svc'
    assert 'hash-a' not in str(result)


def test_placement_sections_fail_independently(monkeypatch):
    monkeypatch.setattr(core.serve_state, 'get_service_from_name', lambda _: {
        'hash': 'hash-a',
        'pool': False
    })
    monkeypatch.setattr(core.serve_utils, 'get_service_placement_state',
                        mock.Mock(side_effect=RuntimeError('controller down')))
    monkeypatch.setattr(core.capacity_cache, 'active_service_observations',
                        lambda *_: {
                            'available': True,
                            'hints': []
                        })
    monkeypatch.setattr(
        core.placement_history, 'get_history', lambda *_args, **_kwargs: {
            'available': True,
            'events': []
        })

    result = core.placement('svc')

    assert result['placer_state'] == {
        'available': False,
        'reason': 'controller_unavailable'
    }
    assert result['capacity_hints']['available'] is True
    assert result['history']['available'] is True


def test_placement_rejects_missing_service(monkeypatch):
    monkeypatch.setattr(core.serve_state, 'get_service_from_name',
                        lambda _: None)

    with pytest.raises(ValueError, match='not found'):
        core.placement('missing')


def test_placement_rechecks_server_derived_owner_scope(monkeypatch):
    get_service = mock.Mock(return_value=None)
    monkeypatch.setattr(core.serve_state, 'get_service_from_name', get_service)

    with pytest.raises(ValueError, match='not found'):
        core.placement('recreated', authorized_owner_user_id='owner-a')

    get_service.assert_called_once_with('recreated', owner_user_id='owner-a')


@pytest.mark.parametrize('overrides', [{
    'hours': 0
}, {
    'hours': 25
}, {
    'limit': 0
}, {
    'limit': 101
}, {
    'cursor': ''
}, {
    'cursor': 'x' * 513
}, {
    'location_limit': 0
}, {
    'location_limit': 101
}, {
    'location_offset': -1
}, {
    'location_offset': 100_001
}])
def test_placement_payload_rejects_unbounded_inputs(overrides):
    with pytest.raises(ValueError):
        payloads.ServePlacementBody(service_name='svc', **overrides)
