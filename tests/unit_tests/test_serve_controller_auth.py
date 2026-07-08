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
