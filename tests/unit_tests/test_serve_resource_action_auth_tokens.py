"""Purpose-token grammar and trust-domain isolation tests."""

import pathlib

import pytest

from sky.serve import auth_tokens
from sky.serve import constants

_PURPOSE_ENV = constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR
_AUTHORITY_ENABLED_ENV = (constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR)
_OTHER_RING_ENVS = (
    constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
    constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
    constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
)
_LEGACY_ENVS = (constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
                constants.LB_AUTH_TOKEN_ENV_VAR)


@pytest.fixture(autouse=True)
def _clear_auth_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (_AUTHORITY_ENABLED_ENV, _PURPOSE_ENV, *_OTHER_RING_ENVS,
                 *_LEGACY_ENVS):
        monkeypatch.delenv(name, raising=False)


def _ring(tmp_path: pathlib.Path, contents: bytes) -> pathlib.Path:
    path = tmp_path / f'ring-{len(tuple(tmp_path.iterdir()))}'
    path.write_bytes(contents)
    return path


def test_authority_activation_requires_exact_chart_marker(
        monkeypatch: pytest.MonkeyPatch) -> None:
    assert not auth_tokens.is_resource_action_authority_enabled()
    monkeypatch.setenv(_AUTHORITY_ENABLED_ENV, 'true')
    assert auth_tokens.is_resource_action_authority_enabled()
    for invalid in ('false', 'True', '1', ''):
        monkeypatch.setenv(_AUTHORITY_ENABLED_ENV, invalid)
        with pytest.raises(auth_tokens.AuthTokenConfigurationError,
                           match='exactly "true"'):
            auth_tokens.is_resource_action_authority_enabled()


def test_preflight_ring_accepts_one_or_two_tokens_and_rereads_rotation(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _ring(tmp_path, b'a' * 32 + b'\n')
    monkeypatch.setenv(_PURPOSE_ENV, str(path))
    assert auth_tokens.get_resource_action_preflight_auth_tokens(
        required=True) == ('a' * 32,)
    path.write_bytes(b'b' * 32 + b'\n' + b'c' * 256 + b'\n')
    assert auth_tokens.get_resource_action_preflight_auth_tokens(
        required=True) == ('b' * 32, 'c' * 256)


@pytest.mark.parametrize('contents', [
    b'',
    b'a' * 32,
    b'a' * 31 + b'\n',
    b'a' * 257 + b'\n',
    b'a' * 32 + b'\r\n',
    b'a' * 32 + b'\n\n',
    b'a' * 32 + b'\n' + b'a' * 32 + b'\n',
    b'a' * 32 + b'\n' + b'b' * 32 + b'\n' + b'c' * 32 + b'\n',
    b'a' * 31 + b' ' + b'\n',
    b'sky_' + b'a' * 32 + b'\n',
    b'\xff' * 32 + b'\n',
])
def test_preflight_ring_rejects_every_noncanonical_shape(
        contents: bytes, tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    path = _ring(tmp_path, contents)
    monkeypatch.setenv(_PURPOSE_ENV, str(path))
    with pytest.raises(auth_tokens.AuthTokenConfigurationError):
        auth_tokens.get_resource_action_preflight_auth_tokens(required=True)


def test_preflight_ring_is_required_without_legacy_fallback() -> None:
    with pytest.raises(auth_tokens.AuthTokenConfigurationError,
                       match='is not configured'):
        auth_tokens.get_resource_action_preflight_auth_tokens(required=True)
    assert not auth_tokens.get_resource_action_preflight_auth_tokens()


@pytest.mark.parametrize('domain', [
    'api_user', 'sync', 'admin', 'data_plane', 'legacy_admin',
    'legacy_data_plane'
])
def test_preflight_token_must_be_disjoint_from_every_auth_domain(
        domain: str, tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    token = 'p' * 32
    purpose_path = _ring(tmp_path, token.encode() + b'\n')
    monkeypatch.setenv(_PURPOSE_ENV, str(purpose_path))
    api_user_tokens: tuple[str, ...] = ()
    if domain == 'api_user':
        api_user_tokens = (token,)
    elif domain in ('sync', 'admin', 'data_plane'):
        index = ('sync', 'admin', 'data_plane').index(domain)
        other_path = _ring(tmp_path, token.encode() + b'\n')
        monkeypatch.setenv(_OTHER_RING_ENVS[index], str(other_path))
    elif domain == 'legacy_admin':
        monkeypatch.setenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, token)
    else:
        monkeypatch.setenv(constants.LB_AUTH_TOKEN_ENV_VAR, token)
    with pytest.raises(auth_tokens.AuthTokenConfigurationError,
                       match='disjoint'):
        auth_tokens.get_isolated_resource_action_preflight_auth_tokens(
            required=True, api_user_tokens=api_user_tokens)


def test_preflight_isolation_preserves_two_distinct_rotation_tokens(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _ring(tmp_path, b'a' * 32 + b'\n' + b'b' * 32 + b'\n')
    monkeypatch.setenv(_PURPOSE_ENV, str(path))
    assert auth_tokens.get_isolated_resource_action_preflight_auth_tokens(
        required=True, api_user_tokens=('c' * 32,)) == ('a' * 32, 'b' * 32)
