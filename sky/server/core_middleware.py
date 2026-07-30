"""Core HTTP middleware policies for the SkyPilot API server."""

import fastapi
import starlette.middleware.base

from sky.server import common
from sky.server import constants as server_constants
from sky.server import middleware_utils
from sky.server import state
from sky.server import versions
from sky.server.requests import cutover as request_cutover
from sky.server.requests import requests as requests_lib


class RequestIDMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to add a request ID to each request."""

    async def dispatch(self, request: fastapi.Request, call_next):
        request_id = requests_lib.get_new_request_id()
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers['X-Skypilot-Request-ID'] = request_id
        return response


class SecurityHeadersMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to add security headers to all HTTP responses.

    Adds Content-Security-Policy and other security headers to mitigate
    XSS, clickjacking, and content-type sniffing attacks.

    Reference: OWASP A02:2025 - Security Misconfiguration (CWE-1021).
    """

    # Content-Security-Policy directives:
    # - default-src 'self': Only allow resources from the same origin
    # - script-src: For HTML responses a per-request nonce is used
    #   ('nonce-<value>') so inline scripts are allowed only when they
    #   carry the matching nonce attribute.  Non-HTML responses get a
    #   strict 'self'-only policy (no inline allowance needed).
    # - style-src: Uses 'unsafe-inline' because CSS-in-JS libraries
    #   (Emotion, react-remove-scroll-bar) dynamically create <style>
    #   elements that cannot easily carry nonces.  CSS cannot execute
    #   scripts, so the risk is negligible.
    # - font-src 'self': Only allow same-origin fonts
    # - connect-src 'self' https://usage-v3.skypilot.co
    #   http://localhost:* http://127.0.0.1:*:
    #   Allow same-origin fetch/XHR/WebSocket, analytics traffic via the
    #   usage-v3 reverse proxy, and localhost connections needed by the
    #   /token page's legacy auth callback flow (the page's JavaScript
    #   POSTs the auth token to a local HTTP server started by the CLI
    #   on localhost)
    # - worker-src 'self' blob:: Allow same-origin workers and blob
    #   workers (needed for analytics).
    # - frame-src 'self': Allow same-origin iframes (for Grafana panels)
    # - img-src 'self' data:: Allow same-origin images and data URIs
    # - object-src 'none': Block all plugin content (Flash, Java, etc.)
    # - base-uri 'self': Restrict <base> element to same origin
    # - form-action 'self': Restrict form submissions to same origin
    # - frame-ancestors 'self': Prevent clickjacking via framing
    _CSP_TEMPLATE = ('default-src \'self\'; '
                     'script-src {script_src} '
                     'https://usage-v3.skypilot.co; '
                     'style-src \'self\' \'unsafe-inline\'; '
                     'font-src \'self\'; '
                     'connect-src \'self\' https://usage-v3.skypilot.co '
                     'http://localhost:* http://127.0.0.1:*; '
                     'worker-src \'self\' blob:; '
                     'frame-src \'self\'; '
                     'img-src \'self\' data:; '
                     'object-src \'none\'; '
                     'base-uri \'self\'; '
                     'form-action \'self\'; '
                     'frame-ancestors \'self\'')

    async def dispatch(self, request: fastapi.Request, call_next):
        response = await call_next(request)
        # Endpoints that serve HTML set request.state.csp_nonce so the
        # CSP header can reference the nonce that was injected into the
        # HTML body.  Non-HTML responses get a strict policy with no
        # inline allowance.
        nonce = getattr(request.state, 'csp_nonce', None)
        if nonce:
            script_src = f'\'self\' \'nonce-{nonce}\''
        else:
            script_src = '\'self\''
        csp = self._CSP_TEMPLATE.format(script_src=script_src)
        response.headers['Content-Security-Policy'] = csp
        # X-Frame-Options for legacy browsers that don't support CSP
        # frame-ancestors directive.
        response.headers['X-Frame-Options'] = 'SAMEORIGIN'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = (
            'strict-origin-when-cross-origin')
        response.headers['Permissions-Policy'] = (
            'camera=(), microphone=(), geolocation=()')
        return response


@middleware_utils.websocket_aware
class GracefulShutdownMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to control requests when server is shutting down."""

    async def dispatch(self, request: fastapi.Request, call_next):
        shutting_down = state.get_block_requests()
        cutting_over = request_cutover.legacy_submissions_blocked()
        if shutting_down or cutting_over:
            # Allow /api/ paths to continue, which are critical to operate
            # on-going requests but will not submit new requests.
            if not request.url.path.startswith('/api/'):
                # Client will retry on 503 error.
                detail = ('The API request store is being migrated to '
                          'PostgreSQL, please try again later.'
                          if cutting_over else
                          'Server is shutting down, please try again later.')
                return fastapi.responses.JSONResponse(
                    status_code=503, content={'detail': detail})

        return await call_next(request)


@middleware_utils.websocket_aware
class APIVersionMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Middleware to add API version to the request."""

    async def dispatch(self, request: fastapi.Request, call_next):
        version_info = versions.check_compatibility_at_server(request.headers)
        # Bypass version handling for backward compatibility with clients prior
        # to v0.11.0, the client will check the version in the body of
        # /api/health response and hint an upgrade.
        # TODO(aylei): remove this after v0.13.0 is released.
        if version_info is None:
            return await call_next(request)
        if version_info.error is None:
            versions.set_remote_api_version(version_info.api_version)
            versions.set_remote_version(version_info.version)
            response = await call_next(request)
        else:
            response = fastapi.responses.JSONResponse(
                status_code=400,
                content={
                    'error': common.ApiServerStatus.VERSION_MISMATCH.value,
                    'message': version_info.error,
                })
        response.headers[server_constants.API_VERSION_HEADER] = str(
            server_constants.API_VERSION)
        response.headers[server_constants.VERSION_HEADER] = \
            versions.get_local_readable_version()
        return response
