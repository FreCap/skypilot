"""Authentication and authorization middleware for the API server."""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time

import fastapi
import jwt as pyjwt
import starlette.middleware.base

from sky import global_user_state
from sky import models
from sky import sky_logging
from sky.serve import serve_utils
from sky.serve.server import controller_proxy as serve_controller_proxy
from sky.server import common
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import middleware_utils
from sky.server.auth import loopback
from sky.server.auth import user_registration
from sky.skylet import constants
from sky.users import permission
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)


def _basic_auth_401_response(content: str):
    """Return a 401 response with basic auth realm."""
    return fastapi.responses.JSONResponse(
        status_code=401,
        headers={
            'WWW-Authenticate': 'Basic realm=\"SkyPilot\"',
            # Prevent CDNs/browsers from caching auth failures on cacheable
            # URLs (e.g. /dashboard/_next/...), which would otherwise poison
            # the dashboard for all subsequent users.
            'Cache-Control': 'no-store',
        },
        content=content)


def _bearer_auth_401_response(content):
    """Return a 401 response for bearer token authentication failures."""
    return fastapi.responses.JSONResponse(
        status_code=401,
        headers={
            # Prevent CDNs/browsers from caching auth failures on cacheable
            # URLs (e.g. /dashboard/_next/...), which would otherwise poison
            # the dashboard for all subsequent users.
            'Cache-Control': 'no-store',
        },
        content=content)


async def _try_set_basic_auth_user(request: fastapi.Request):
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.lower().startswith('basic '):
        return

    # Check username and password
    encoded = auth_header.split(' ', 1)[1]
    try:
        decoded = base64.b64decode(encoded).decode()
        username, password = decoded.split(':', 1)
    except Exception:  # pylint: disable=broad-except
        return

    users = await asyncio.to_thread(global_user_state.get_user_by_name,
                                    username)
    if not users:
        return

    for user in users:
        if not user.name or not user.password:
            continue
        username_encoded = username.encode('utf8')
        db_username_encoded = user.name.encode('utf8')
        if (username_encoded == db_username_encoded and await asyncio.to_thread(
                common.crypt_ctx.verify, password, user.password)):
            request.state.auth_user = user
            break


@middleware_utils.websocket_aware
class RBACMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to handle RBAC."""

    async def dispatch(self, request: fastapi.Request, call_next):
        # TODO(hailong): should have a list of paths
        # that are not checked for RBAC
        if request.url.path.startswith('/dashboard/'):
            return await call_next(request)

        auth_user = request.state.auth_user
        if auth_user is None:
            return await call_next(request)

        permission_service = permission.permission_service
        # Check the role permission
        if permission_service.check_endpoint_permission(auth_user.id,
                                                        request.url.path,
                                                        request.method):
            return fastapi.responses.JSONResponse(
                status_code=403, content={'detail': 'Forbidden'})

        return await call_next(request)


def _extract_identity_from_jwt(jwt_token: str, claim: str) -> str | None:
    """Extract identity claim from a JWT token without verification.

    This is for trusted proxy scenarios where the external proxy has already
    verified the token. We only decode to extract the claim.

    Args:
        jwt_token: The JWT token string.
        claim: The claim name to extract (e.g., 'email', 'sub').

    Returns:
        The claim value if found, None otherwise.
    """
    try:
        # Trusted proxy scenario - skip all verification since the proxy
        # has already authenticated the request
        payload = pyjwt.decode(jwt_token,
                               options={
                                   'verify_signature': False,
                                   'verify_exp': False,
                                   'verify_aud': False,
                               })
        return payload.get(claim)
    except pyjwt.exceptions.DecodeError as e:
        logger.debug(f'Failed to decode JWT from header: {e}')
        return None
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Unexpected error decoding JWT: {e}')
        return None


def _extract_user_from_header(
    request: fastapi.Request,
    proxy_config: server_config.ExternalProxyConfig,
) -> models.User | None:
    """Extract user identity from request header.

    Supports both plaintext headers (e.g., X-Auth-Request-Email) and
    JWT-encoded headers.
    """
    if proxy_config.header_name not in request.headers:
        return None

    header_value = request.headers[proxy_config.header_name]

    if proxy_config.header_format == 'jwt':
        user_name = _extract_identity_from_jwt(header_value,
                                               proxy_config.jwt_identity_claim)
    else:
        user_name = header_value

    if not user_name:
        return None

    # MD5 only derives a stable user id from the (non-secret) user name;
    # not a security use.
    user_hash = hashlib.md5(
        user_name.encode(),
        usedforsecurity=False).hexdigest()[:common_utils.USER_HASH_LENGTH]
    if proxy_config.enabled:
        return models.User(id=user_hash,
                           name=user_name,
                           user_type=models.UserType.LEGACY.value)
    else:
        return models.User(id=user_hash,
                           name=user_name,
                           user_type=models.UserType.SSO.value)


def _get_auth_user_header(request: fastapi.Request) -> models.User | None:
    """Legacy function for backward compatibility.

    This function is used by _generate_auth_token() which does not have
    access to the middleware config. It uses the default configuration
    which is backward compatible.
    """
    proxy_config = server_config.load_external_proxy_config()
    return _extract_user_from_header(request, proxy_config)


def _generate_auth_token(request: fastapi.Request) -> str:
    """Generate an auth token from the request.

    The token contains the user info and cookies, base64 encoded.
    Used by both /token and /api/v1/auth/authorize endpoints.
    """
    user = _get_auth_user_header(request)
    token_data = {
        # Token version number, bump for backwards incompatible changes.
        'v': 1,
        'user': user.id if user is not None else None,
        'cookies': dict(request.cookies),
    }
    json_bytes = json.dumps(token_data).encode('utf-8')
    return base64.b64encode(json_bytes).decode('utf-8')


@middleware_utils.websocket_aware
class InitializeRequestAuthUserMiddleware(
        starlette.middleware.base.BaseHTTPMiddleware):
    """Establish the explicit request-scoped auth and security state."""

    async def dispatch(self, request: fastapi.Request, call_next):
        # Establish the complete request-state interface before any inner
        # middleware or endpoint reads it.  Authentication and HTML/controller
        # middleware may replace these explicit defaults.
        request.state.auth_user = None
        request.state.anonymous_user = False
        request.state.controller_origin = None
        request.state.managed_job_origin = None
        request.state.csp_nonce = None
        return await call_next(request)


@middleware_utils.websocket_aware
class BasicAuthMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to handle HTTP Basic Auth."""

    async def dispatch(self, request: fastapi.Request, call_next):
        if server_constants.is_unauthenticated_public_request(
                request.method, request.url.path):
            request.state.anonymous_user = True
            return await call_next(request)

        # If a previous middleware already authenticated the user, pass through
        if request.state.auth_user is not None:
            return await call_next(request)

        if loopback.is_loopback_request(request):
            return await call_next(request)

        if request.url.path.startswith('/api/health'):
            # Try to set the auth user from basic auth
            await _try_set_basic_auth_user(request)
            return await call_next(request)

        auth_header = request.headers.get('authorization')
        if not auth_header:
            return _basic_auth_401_response('Authentication required')

        # Only handle basic auth
        if not auth_header.lower().startswith('basic '):
            return _basic_auth_401_response('Invalid authentication method')

        # Check username and password
        encoded = auth_header.split(' ', 1)[1]
        try:
            decoded = base64.b64decode(encoded).decode()
            username, password = decoded.split(':', 1)
        except Exception:  # pylint: disable=broad-except
            return _basic_auth_401_response('Invalid basic auth')

        users = await asyncio.to_thread(global_user_state.get_user_by_name,
                                        username)
        if not users:
            return _basic_auth_401_response('Invalid credentials')

        valid_user = False
        for user in users:
            if not user.name or not user.password:
                continue
            username_encoded = username.encode('utf8')
            db_username_encoded = user.name.encode('utf8')
            if (username_encoded == db_username_encoded and
                    await asyncio.to_thread(common.crypt_ctx.verify, password,
                                            user.password)):
                valid_user = True
                request.state.auth_user = user
                break
        if not valid_user:
            return _basic_auth_401_response('Invalid credentials')

        return await call_next(request)


@middleware_utils.websocket_aware
class BearerTokenMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to handle Bearer Token Auth (Service Accounts)."""

    async def dispatch(self, request: fastapi.Request, call_next):
        """Make sure correct bearer token auth is present.

        1. If the request has the X-Skypilot-Auth-Mode: token header, it must
           have a valid bearer token.
        2. For backwards compatibility, if the request has a Bearer token
           beginning with "sky_" (even if X-Skypilot-Auth-Mode is not present),
           it must be a valid token.
        3. If X-Skypilot-Auth-Mode is not set to "token", and there is no Bearer
           token beginning with "sky_", allow the request to continue.

        In conjunction with an auth proxy, the idea is to make the auth proxy
        bypass requests with bearer tokens, instead setting the
        X-Skypilot-Auth-Mode header. The auth proxy should either validate the
        auth or set the header X-Skypilot-Auth-Mode: token.
        """
        if server_constants.is_unauthenticated_public_request(
                request.method, request.url.path):
            request.state.anonymous_user = True
            return await call_next(request)

        # If a previous middleware already authenticated the user, pass through
        if request.state.auth_user is not None:
            return await call_next(request)

        has_skypilot_auth_header = (
            request.headers.get('X-Skypilot-Auth-Mode') == 'token')
        auth_header = request.headers.get('authorization')
        has_bearer_token_starting_with_sky = (
            auth_header and auth_header.lower().startswith('bearer ') and
            auth_header.split(' ', 1)[1].startswith('sky_'))

        if (not has_skypilot_auth_header and
                not has_bearer_token_starting_with_sky):
            # This is case #3 above. We do not need to validate the request.
            # No Bearer token, continue with normal processing (OAuth2 cookies,
            # etc.)
            return await call_next(request)
        # After this point, all requests must be validated.

        if auth_header is None:
            return _bearer_auth_401_response(
                {'detail': 'Authentication required'})

        # Extract token
        split_header = auth_header.split(' ', 1)
        if split_header[0].lower() != 'bearer':
            return _bearer_auth_401_response(
                {'detail': 'Invalid authentication method'})
        sa_token = split_header[1]

        # Handle SkyPilot service account tokens
        return await self._handle_service_account_token(request, sa_token,
                                                        call_next)

    async def _handle_service_account_token(self, request: fastapi.Request,
                                            sa_token: str, call_next):
        """Handle SkyPilot service account tokens."""
        # Check if service account tokens are enabled
        sa_enabled = os.environ.get(constants.ENV_VAR_ENABLE_SERVICE_ACCOUNTS,
                                    'false').lower()
        if sa_enabled != 'true':
            return _bearer_auth_401_response(
                {'detail': 'Service account authentication disabled'})

        try:
            # Import here to avoid circular imports
            # pylint: disable=import-outside-toplevel
            from sky.users.token_service import token_service

            # Verify and decode JWT token
            payload = token_service.verify_token(sa_token)

            if payload is None:
                logger.warning('Service account token verification failed')
                return _bearer_auth_401_response(
                    {'detail': 'Invalid or expired service account token'})

            # Extract user information from JWT payload
            user_id = payload.get('sub')
            user_name = payload.get('name')
            token_id = payload.get('token_id')

            if not user_id or not token_id:
                logger.warning(
                    'Invalid token payload: missing user_id or token_id')
                return _bearer_auth_401_response(
                    {'detail': 'Invalid token payload'})

            # Look up the token row by its sha256 hash. This is what makes
            # revocation (row deleted) and rotation (row's hash replaced)
            # take effect at request time -- the JWT alone cannot be revoked.
            # We match on hash rather than token_id because rotation updates
            # the row's hash but keeps the original token_id, while the new
            # JWT carries a freshly-generated token_id; only the hash is
            # consistent between the live JWT and the live DB row.
            incoming_hash = hashlib.sha256(sa_token.encode()).hexdigest()
            token_row = await asyncio.to_thread(
                global_user_state.get_service_account_token_by_hash,
                incoming_hash)
            if token_row is None:
                logger.warning(
                    f'Service account token {token_id} not found in DB '
                    '(revoked or rotated)')
                return _bearer_auth_401_response(
                    {'detail': 'Service account token revoked or rotated'})

            if (token_row['expires_at'] is not None and
                    token_row['expires_at'] < int(time.time())):
                logger.warning(f'Service account token {token_id} has expired')
                return _bearer_auth_401_response(
                    {'detail': 'Service account token has expired'})

            # Verify user still exists in database
            user_info = await asyncio.to_thread(global_user_state.get_user,
                                                user_id)
            if user_info is None:
                logger.warning(
                    f'Service account user {user_id} no longer exists')
                return _bearer_auth_401_response(
                    {'detail': 'Service account user no longer exists'})

            # Update last used timestamp for token tracking. Use the
            # DB row's token_id (not the JWT's): after rotation the JWT
            # carries a different token_id than the DB row.
            try:
                await asyncio.to_thread(
                    global_user_state.update_service_account_token_last_used,
                    token_row['token_id'])
            except Exception as e:  # pylint: disable=broad-except
                logger.debug(f'Failed to update token last used time: {e}')

            # Set the authenticated user
            auth_user = models.User(id=user_id,
                                    name=user_name or user_info.name,
                                    user_type=user_info.user_type)
            request.state.auth_user = auth_user

            logger.debug(f'Authenticated service account: {user_id}')

        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Service account authentication failed: {e}',
                         exc_info=True)
            return _bearer_auth_401_response(
                {'detail': f'Service account authentication failed: {str(e)}'})

        return await call_next(request)


@middleware_utils.websocket_aware
class InternalServeControllerSyncAuthMiddleware(
        starlette.middleware.base.BaseHTTPMiddleware):
    """Authenticate the external LB's stable controller-sync route.

    This middleware sits outside the normal Bearer, Basic, and OAuth
    middlewares.  Only a request carrying one of the dedicated LB-sync tokens
    is marked as authenticated, which lets that exact internal route bypass
    user-facing authentication without exposing any other API route.
    """

    async def dispatch(self, request: fastapi.Request, call_next):
        if not serve_controller_proxy.is_controller_sync_path(request.url.path):
            return await call_next(request)

        try:
            # Read the ring on every request so a projected Secret rotation is
            # honored without restarting the API server.
            auth_tokens = serve_utils.get_lb_sync_auth_tokens(required=True)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'LB sync authentication is unavailable: {e}')
            return fastapi.responses.JSONResponse(
                status_code=503,
                content={
                    'detail': 'Controller sync authentication is '
                              'unavailable.'
                })

        if not auth_tokens:
            logger.error('LB sync authentication returned an empty token ring.')
            return fastapi.responses.JSONResponse(
                status_code=503,
                content={
                    'detail': 'Controller sync authentication is '
                              'unavailable.'
                })

        authorization = request.headers.get('authorization')
        presented_token: str | None = None
        if authorization is not None:
            scheme, separator, token_value = authorization.partition(' ')
            if (separator and scheme.lower() == 'bearer' and token_value and
                    token_value == token_value.strip() and
                    not any(character.isspace() for character in token_value)):
                presented_token = token_value

        authenticated = False
        if presented_token is not None:
            presented_bytes = presented_token.encode('utf-8')
            # Do not stop on the first match: keep comparison work independent
            # of which token in the overlap ring was presented.
            for expected_token in auth_tokens:
                if not isinstance(expected_token, str) or not expected_token:
                    logger.error('LB sync authentication returned an invalid '
                                 'token ring.')
                    return fastapi.responses.JSONResponse(
                        status_code=503,
                        content={
                            'detail': 'Controller sync authentication is '
                                      'unavailable.'
                        })
                authenticated |= hmac.compare_digest(
                    presented_bytes, expected_token.encode('utf-8'))

        if not authenticated:
            return _bearer_auth_401_response(
                {'detail': 'Invalid controller sync bearer token.'})

        # Normal authentication middlewares use this state as their trusted
        # "already authenticated" signal.  The system principal is not a
        # viewer, so the blocklist-based RBAC policy permits this internal
        # route while the dedicated token remains the authentication gate.
        request.state.auth_user = models.User(
            id='skypilot-system-lb-sync',
            name='SkyServe external load balancer',
            user_type=models.UserType.SYSTEM.value)
        return await call_next(request)


@middleware_utils.websocket_aware
class InternalServeControllerApiAuthMiddleware(
        starlette.middleware.base.BaseHTTPMiddleware):
    """Authenticate only the controller's replica-launch API operations."""

    async def dispatch(self, request: fastapi.Request, call_next):
        dedicated_auth_requested = (request.headers.get(
            server_constants.SERVE_CONTROLLER_API_AUTH_HEADER
        ) == server_constants.SERVE_CONTROLLER_API_AUTH_HEADER_VALUE)
        if (not dedicated_auth_requested or
                not server_constants.is_serve_controller_api_request(
                    request.method, request.url.path)):
            return await call_next(request)

        try:
            auth_tokens = serve_utils.get_controller_admin_auth_tokens(
                required=True)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Controller API authentication is unavailable: {e}')
            return fastapi.responses.JSONResponse(
                status_code=503,
                content={
                    'detail': 'Controller API authentication is unavailable.'
                })

        if not auth_tokens:
            logger.error(
                'Controller API authentication returned an empty token ring.')
            return fastapi.responses.JSONResponse(
                status_code=503,
                content={
                    'detail': 'Controller API authentication is unavailable.'
                })

        authorization = request.headers.get('authorization')
        presented_token: str | None = None
        if authorization is not None:
            scheme, separator, token_value = authorization.partition(' ')
            if (separator and scheme.lower() == 'bearer' and token_value and
                    token_value == token_value.strip() and
                    not any(character.isspace() for character in token_value)):
                presented_token = token_value

        authenticated = False
        if presented_token is not None:
            presented_bytes = presented_token.encode('utf-8')
            for expected_token in auth_tokens:
                if not isinstance(expected_token, str) or not expected_token:
                    logger.error('Controller API authentication returned an '
                                 'invalid token ring.')
                    return fastapi.responses.JSONResponse(
                        status_code=503,
                        content={
                            'detail': 'Controller API authentication is '
                                      'unavailable.'
                        })
                authenticated |= hmac.compare_digest(
                    presented_bytes, expected_token.encode('utf-8'))

        if not authenticated:
            return _bearer_auth_401_response(
                {'detail': 'Invalid controller API bearer token.'})

        # Request execution also uses ``User.id`` as the cloud resource-name
        # suffix.  Keep this temporary bridge principal within the established
        # eight-character user-hash budget; the descriptive 32-character ID
        # leaves no room for a display-name hash under Kubernetes' 42-character
        # cluster-name limit and makes every admitted launch fail before
        # provider submission.
        request.state.auth_user = models.User(
            id='skyserve',
            name='SkyServe controller',
            user_type=models.UserType.SYSTEM.value)
        return await call_next(request)


@middleware_utils.websocket_aware
class AuthProxyMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to handle external auth proxy.

    This middleware extracts user identity from HTTP headers set by an
    external authentication proxy (e.g., oauth2-proxy)
    """

    # pylint: disable=redefined-outer-name
    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self.config = server_config.load_external_proxy_config()
        if self.config.enabled:
            logger.debug('AuthProxyMiddleware enabled with header: '
                         f'{self.config.header_name}, '
                         f'format: {self.config.header_format}')
        else:
            logger.debug('AuthProxyMiddleware disabled via configuration')

    async def dispatch(self, request: fastapi.Request, call_next):
        if server_constants.is_unauthenticated_public_request(
                request.method, request.url.path):
            request.state.anonymous_user = True
            return await call_next(request)

        if not self.config.enabled:
            return await call_next(request)

        auth_user = _extract_user_from_header(request, self.config)

        if request.state.auth_user is not None:
            # Previous middleware is trusted more than this middleware.  For
            # instance, a client could set the Authorization and the
            # X-Auth-Request-Email header. In that case, the auth proxy will be
            # skipped and we should rely on the Bearer token to authenticate the
            # user - but that means the user could set X-Auth-Request-Email to
            # whatever the user wants. We should thus ignore it.
            if auth_user is not None:
                logger.debug('Warning: ignoring auth proxy header since the '
                             'auth user was already set.')
            return await call_next(request)

        # Add user to database if auth_user is present
        if auth_user is not None:
            await user_registration.add_or_update_user_with_default_role(
                auth_user)

        # Store user info in request.state for access by GET endpoints
        if auth_user is not None:
            request.state.auth_user = auth_user

        return await call_next(request)


# The API server remains the public facade for these symbols. Preserve their
# historical module identity for introspection and serialized references.
for _public_symbol in (
        _basic_auth_401_response,
        _bearer_auth_401_response,
        _try_set_basic_auth_user,
        RBACMiddleware,
        _extract_identity_from_jwt,
        _extract_user_from_header,
        _get_auth_user_header,
        _generate_auth_token,
        InitializeRequestAuthUserMiddleware,
        BasicAuthMiddleware,
        BearerTokenMiddleware,
        InternalServeControllerSyncAuthMiddleware,
        InternalServeControllerApiAuthMiddleware,
        AuthProxyMiddleware,
):
    _public_symbol.__module__ = 'sky.server.server'
del _public_symbol
