"""Tests for controller control-plane auth (W6b).

The controller guards every control-plane endpoint -- the destructive ones
(/controller/update_service, terminate_replica) AND the read-only sync/status
paths (/load_balancer_sync, /autoscaler/info) -- with a shared bearer token
(no-op when unset). The LB presents the token on every sync. The expected token
is read fresh per request, so a token rotated after boot is honored without a
controller respawn.
"""
# pylint: disable=invalid-name,protected-access
import asyncio

import fastapi
import pytest

from sky.serve import constants
from sky.serve import controller
from sky.serve import serve_utils


@pytest.fixture(autouse=True)
def _clear_token_file_envs(monkeypatch):
    monkeypatch.delenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                       raising=False)
    monkeypatch.delenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                       raising=False)


def _run(dep, authorization):
    return asyncio.run(dep(authorization=authorization))


def _set_token(monkeypatch, token):
    if token is None:
        monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
                           raising=False)
    else:
        monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, token)


def test_auth_disabled_allows_anything(monkeypatch):
    _set_token(monkeypatch, None)
    dep = controller._make_auth_dependency()
    assert _run(dep, None) is None
    assert _run(dep, 'Bearer anything') is None


def test_auth_correct_token_passes(monkeypatch):
    _set_token(monkeypatch, 's3cret')
    dep = controller._make_auth_dependency()
    assert _run(dep, 'Bearer s3cret') is None


@pytest.mark.parametrize(
    'bad',
    [
        None,
        'Bearer wrong',
        's3cret',  # missing the "Bearer " scheme
        'Bearer ',
        '',
        'Bearer ñ',  # non-ASCII must 401, not crash compare_digest (500)
    ])
def test_auth_wrong_or_missing_rejected(monkeypatch, bad):
    _set_token(monkeypatch, 's3cret')
    dep = controller._make_auth_dependency()
    with pytest.raises(fastapi.HTTPException) as excinfo:
        _run(dep, bad)
    assert excinfo.value.status_code == 401


def test_auth_token_rotation_honored_on_next_request(monkeypatch):
    # The same dependency instance must pick up a token rotated after it was
    # built -- no respawn required.
    _set_token(monkeypatch, 'old')
    dep = controller._make_auth_dependency()
    assert _run(dep, 'Bearer old') is None
    with pytest.raises(fastapi.HTTPException):
        _run(dep, 'Bearer new')

    _set_token(monkeypatch, 'new')
    assert _run(dep, 'Bearer new') is None
    with pytest.raises(fastapi.HTTPException):
        _run(dep, 'Bearer old')


def test_get_controller_auth_token_reads_env(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    assert serve_utils.get_controller_auth_token() is None
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, 'tok')
    assert serve_utils.get_controller_auth_token() == 'tok'
    # Empty string is treated as unset (auth disabled).
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, '')
    assert serve_utils.get_controller_auth_token() is None


def test_sync_and_admin_rings_are_independent_and_accept_overlap(
        monkeypatch, tmp_path):
    sync_ring = tmp_path / 'sync.tokens'
    sync_ring.write_text('sync-new\nsync-old\n', encoding='utf-8')
    admin_ring = tmp_path / 'admin.tokens'
    admin_ring.write_text('admin-new\nadmin-old\n', encoding='utf-8')
    monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                       str(sync_ring))
    monkeypatch.setenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                       str(admin_ring))

    sync_dep = controller._make_auth_dependency(sync=True)
    admin_dep = controller._make_auth_dependency()
    for token in ('sync-new', 'sync-old'):
        assert _run(sync_dep, f'Bearer {token}') is None
        with pytest.raises(fastapi.HTTPException) as excinfo:
            _run(admin_dep, f'Bearer {token}')
        assert excinfo.value.status_code == 401
    for token in ('admin-new', 'admin-old'):
        assert _run(admin_dep, f'Bearer {token}') is None
        with pytest.raises(fastapi.HTTPException) as excinfo:
            _run(sync_dep, f'Bearer {token}')
        assert excinfo.value.status_code == 401


def test_required_dependency_fails_closed_when_ring_missing(monkeypatch):
    _set_token(monkeypatch, None)
    dep = controller._make_auth_dependency(sync=True, required=True)
    with pytest.raises(fastapi.HTTPException) as excinfo:
        _run(dep, None)
    assert excinfo.value.status_code == 503


def test_controller_ring_rotation_is_live(monkeypatch, tmp_path):
    ring = tmp_path / 'admin.tokens'
    ring.write_text('old\n', encoding='utf-8')
    monkeypatch.setenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                       str(ring))
    dep = controller._make_auth_dependency()
    assert _run(dep, 'Bearer old') is None

    ring.write_text('new\nold\n', encoding='utf-8')
    assert _run(dep, 'Bearer new') is None
    assert _run(dep, 'Bearer old') is None
