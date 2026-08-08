"""SkyServe bearer-token configuration and trust-domain isolation."""

import os
import pathlib
import re

from sky.serve import constants
from sky.utils import common_utils


class AuthTokenConfigurationError(ValueError):
    """A required Serve auth ring is absent or cannot be parsed safely."""


_AUTH_TOKEN_PATTERN = re.compile(r'[A-Za-z0-9._~+/=-]+')


def is_lb_data_plane_auth_enabled() -> bool:
    """Whether inference requests require the LB-only bearer credential.

    New charts inject an explicit capability value. If it is absent during a
    mixed-version rollout, preserve the legacy behavior by treating configured
    data-plane token material as enabled. An explicit false is authoritative
    even while stale Secret files are being removed from an existing pod.
    """
    configured = os.environ.get(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR)
    if configured is None:
        return bool(
            os.environ.get(constants.LB_AUTH_TOKENS_FILE_ENV_VAR) or
            os.environ.get(constants.LB_AUTH_TOKEN_ENV_VAR))
    if configured == 'true':
        return True
    if configured == 'false':
        return False
    raise AuthTokenConfigurationError(
        f'{constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR} must be exactly '
        '"true" or "false".')


def _get_auth_tokens(file_env_var: str,
                     legacy_token_env_var: str | None,
                     ring_name: str,
                     required: bool = False) -> tuple[str, ...]:
    """Read a newline-delimited bearer-token ring without caching it.

    The configured file is authoritative and is read fresh on every call so a
    projected Secret rotation is live. A final newline is accepted; blank
    lines, whitespace-bearing/non-ASCII tokens, an empty file, and I/O/UTF-8
    errors are rejected instead of silently falling back to the legacy env
    token. When a legacy singleton env name is supplied, it is consulted only
    when no file is configured. Callers can omit that fallback for trust
    domains where sharing the legacy credential would be unsafe.
    """
    token_file = os.environ.get(file_env_var)
    if token_file:
        try:
            contents = pathlib.Path(token_file).expanduser().read_text(
                encoding='utf-8')
        except (OSError, UnicodeError) as e:
            raise AuthTokenConfigurationError(
                f'Cannot read {ring_name} token ring from {token_file!r}: '
                f'{common_utils.format_exception(e)}') from e
        tokens = tuple(contents.splitlines())
        if not tokens:
            raise AuthTokenConfigurationError(
                f'{ring_name} token ring {token_file!r} is empty.')
        for token in tokens:
            if _AUTH_TOKEN_PATTERN.fullmatch(token) is None:
                raise AuthTokenConfigurationError(
                    f'{ring_name} token ring {token_file!r} contains an '
                    'empty or malformed token.')
        return tokens

    legacy_token = (os.environ.get(legacy_token_env_var)
                    if legacy_token_env_var is not None else None)
    if legacy_token:
        if _AUTH_TOKEN_PATTERN.fullmatch(legacy_token) is None:
            assert legacy_token_env_var is not None
            raise AuthTokenConfigurationError(
                f'{legacy_token_env_var} contains a malformed token.')
        return (legacy_token,)
    if required:
        if legacy_token_env_var is not None:
            missing_sources = (f'neither {file_env_var} nor '
                               f'{legacy_token_env_var} is configured')
        else:
            missing_sources = f'{file_env_var} is not configured'
        raise AuthTokenConfigurationError(
            f'{ring_name} authentication is required, but '
            f'{missing_sources}.')
    return ()


def _get_serve_auth_token_rings(
    sync_required: bool = False,
    admin_required: bool = False,
    data_plane_required: bool = False,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Read every mounted Serve ring and reject cross-domain credentials.

    Each ring may contain multiple credentials for an overlap rotation within
    that trust domain. A credential may never appear in both rings: otherwise
    an external LB holding the sync ring could invoke destructive controller
    administration routes. Both files are read on every call so an unsafe
    Secret rotation fails closed immediately, not only at process startup.

    The legacy singleton remains an admin-only fallback. In particular, it is
    never returned as an LB-sync credential.
    """
    sync_tokens = _get_auth_tokens(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                                   None,
                                   'load-balancer sync',
                                   required=sync_required)
    admin_tokens = _get_auth_tokens(
        constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
        constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
        'controller admin',
        required=admin_required)
    data_plane_tokens = _get_auth_tokens(constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                                         constants.LB_AUTH_TOKEN_ENV_VAR,
                                         'load-balancer data plane',
                                         required=data_plane_required)
    rings = (sync_tokens, admin_tokens, data_plane_tokens)
    if any(not set(rings[left]).isdisjoint(rings[right])
           for left in range(len(rings))
           for right in range(left + 1, len(rings))):
        raise AuthTokenConfigurationError(
            'Load-balancer sync, controller-admin, and data-plane token rings '
            'must be pairwise disjoint.')
    return rings


def validate_controller_auth_token_isolation(required: bool = False) -> None:
    """Validate the controller trust-domain boundary without exposing tokens."""
    _get_serve_auth_token_rings(sync_required=required,
                                admin_required=required,
                                data_plane_required=required)


def get_lb_sync_auth_tokens(required: bool = False) -> tuple[str, ...]:
    """Credentials accepted on, and presented to, the LB sync endpoint."""
    sync_tokens, _, _ = _get_serve_auth_token_rings(sync_required=required)
    return sync_tokens


def get_controller_admin_auth_tokens(required: bool = False) -> tuple[str, ...]:
    """Credentials accepted by trusted controller administration callers."""
    _, admin_tokens, _ = _get_serve_auth_token_rings(admin_required=required)
    return admin_tokens


def get_lb_auth_tokens(required: bool = False) -> tuple[str, ...]:
    """Credentials accepted by the external LB inference data plane."""
    _, _, data_plane_tokens = _get_serve_auth_token_rings(
        data_plane_required=required)
    return data_plane_tokens
