"""SkyServe bearer-token configuration and trust-domain isolation."""

from collections.abc import Mapping
import os
import pathlib
import re

from sky.serve import constants
from sky.utils import common_utils


class AuthTokenConfigurationError(ValueError):
    """A required Serve auth ring is absent or cannot be parsed safely."""


_AUTH_TOKEN_PATTERN = re.compile(r'[A-Za-z0-9._~+/=-]+')
_STRICT_RING_MAX_BYTES = 514
_STRICT_TOKEN_MIN_BYTES = 32
_STRICT_TOKEN_MAX_BYTES = 256


def is_resource_action_authority_enabled(
        environ: Mapping[str, str] | None = None) -> bool:
    """Return the chart-owned resource-action authority activation state."""
    source = os.environ if environ is None else environ
    configured = source.get(constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR)
    if configured is None:
        return False
    if type(configured) is str and configured == 'true':
        return True
    raise AuthTokenConfigurationError(
        f'{constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR} must be '
        'exactly "true" when present.')


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


def _read_strict_resource_action_preflight_ring(path: str) -> tuple[str, ...]:
    """Read the closed purpose-token grammar used by private preflight."""

    try:
        contents = pathlib.Path(path).read_bytes()
    except OSError as e:
        raise AuthTokenConfigurationError(
            'Cannot read the resource-action preflight token ring: '
            f'{common_utils.format_exception(e)}') from e
    if not contents or len(contents) > _STRICT_RING_MAX_BYTES:
        raise AuthTokenConfigurationError(
            'Resource-action preflight token ring must contain 1..514 bytes.')
    if not contents.endswith(b'\n') or b'\r' in contents:
        raise AuthTokenConfigurationError(
            'Resource-action preflight token ring must use LF records and one '
            'final LF.')
    raw_tokens = contents[:-1].split(b'\n')
    if len(raw_tokens) not in (1, 2) or any(not token for token in raw_tokens):
        raise AuthTokenConfigurationError(
            'Resource-action preflight token ring requires exactly one or two '
            'nonempty records.')
    try:
        tokens = tuple(token.decode('ascii') for token in raw_tokens)
    except UnicodeDecodeError as e:
        raise AuthTokenConfigurationError(
            'Resource-action preflight tokens must be ASCII.') from e
    for token in tokens:
        if (len(token) < _STRICT_TOKEN_MIN_BYTES or
                len(token) > _STRICT_TOKEN_MAX_BYTES or
                _AUTH_TOKEN_PATTERN.fullmatch(token) is None):
            raise AuthTokenConfigurationError(
                'Resource-action preflight tokens must be 32..256 characters '
                'in the closed ASCII alphabet.')
        if token.startswith('sky_'):
            raise AuthTokenConfigurationError(
                'Resource-action preflight tokens must not use the reserved '
                'SkyPilot API-token namespace.')
    if len(set(tokens)) != len(tokens):
        raise AuthTokenConfigurationError(
            'Resource-action preflight token ring contains a duplicate token.')
    return tokens


def get_resource_action_preflight_auth_tokens(
        required: bool = False) -> tuple[str, ...]:
    """Reread the private preflight purpose ring without caching it."""

    token_file = os.environ.get(
        constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR)
    if not token_file:
        if required:
            raise AuthTokenConfigurationError(
                'Resource-action preflight authentication is required, but '
                f'{constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR} '
                'is not configured.')
        return ()
    return _read_strict_resource_action_preflight_ring(token_file)


def validate_resource_action_preflight_auth_token_isolation(
    required: bool = False, api_user_tokens: tuple[str, ...] = ()) -> None:
    """Reject a purpose token reused in any mounted authentication domain.

    ``api_user_tokens`` is an explicit narrow input because API-user tokens are
    database-backed rather than exposed through a Serve environment variable.
    Startup code with that domain mounted supplies its already-validated byte
    values; this module never reaches into user or database state.
    """

    get_isolated_resource_action_preflight_auth_tokens(
        required=required, api_user_tokens=api_user_tokens)


def get_isolated_resource_action_preflight_auth_tokens(
    required: bool = False,
    api_user_tokens: tuple[str, ...] = ()
) -> tuple[str, ...]:
    """Return one freshly read purpose ring after cross-domain validation."""

    if type(api_user_tokens) is not tuple or any(
            type(token) is not str for token in api_user_tokens):
        raise TypeError('api_user_tokens must be a tuple of text tokens.')
    purpose_tokens = get_resource_action_preflight_auth_tokens(
        required=required)
    sync_tokens, admin_tokens, data_plane_tokens = (
        _get_serve_auth_token_rings())
    other_domains = (api_user_tokens, sync_tokens, admin_tokens,
                     data_plane_tokens)
    purpose_set = set(purpose_tokens)
    if any(not purpose_set.isdisjoint(tokens) for tokens in other_domains):
        raise AuthTokenConfigurationError(
            'Resource-action preflight token ring must be disjoint from every '
            'other authentication trust domain.')
    return purpose_tokens
