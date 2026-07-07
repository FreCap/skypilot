"""Tests for controller destructive-endpoint auth (W6b).

The controller guards /controller/update_service and terminate_replica with a
shared bearer token (no-op when unset). The read-only load_balancer_sync path
is intentionally left open for the credential-free external LB.
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


def test_auth_disabled_allows_anything():
    dep = controller._make_auth_dependency(None)
    assert _run(dep, None) is None
    assert _run(dep, 'Bearer anything') is None


def test_auth_correct_token_passes():
    dep = controller._make_auth_dependency('s3cret')
    assert _run(dep, 'Bearer s3cret') is None


@pytest.mark.parametrize(
    'bad',
    [
        None,
        'Bearer wrong',
        's3cret',  # missing the "Bearer " scheme
        'Bearer ',
        '',
    ])
def test_auth_wrong_or_missing_rejected(bad):
    dep = controller._make_auth_dependency('s3cret')
    with pytest.raises(fastapi.HTTPException) as excinfo:
        _run(dep, bad)
    assert excinfo.value.status_code == 401


def test_get_controller_auth_token_reads_env(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    assert serve_utils.get_controller_auth_token() is None
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, 'tok')
    assert serve_utils.get_controller_auth_token() == 'tok'
    # Empty string is treated as unset (auth disabled).
    monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, '')
    assert serve_utils.get_controller_auth_token() is None
