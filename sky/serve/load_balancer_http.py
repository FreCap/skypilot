"""ASGI runtime adapters for the SkyServe load balancer."""
import asyncio
import contextlib
import hmac
import signal
from typing import Any

import fastapi
import uvicorn

from sky import sky_logging
from sky.serve import constants
from sky.serve import serve_utils

# Keep the historical logger name so moving these adapters does not alter
# operator-visible log attribution.
logger = sky_logging.init_logger('sky.serve.load_balancer')


class _ReleasingStreamingResponse(fastapi.responses.StreamingResponse):
    """Streaming response that releases its upstream owner on every ASGI exit.

    A normal StreamingResponse only runs its body iterator and background task
    after the response-start message succeeds. If the downstream disconnects,
    or the response task is cancelled, before then, neither cleanup path runs.
    Bracketing the complete ASGI call closes that gap; the callback itself is
    idempotent because the iterator/background paths still release eagerly.
    """

    def __init__(self, *args: Any, release: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._release = release

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._release()

    def hold_cleanup_until_complete(self, release: Any) -> None:
        """Chain an idempotent owner cleanup onto the ASGI lifetime."""
        upstream_release = self._release
        released = False

        async def _release_all() -> None:
            nonlocal released
            try:
                await upstream_release()
            finally:
                # The chained owner must release even when upstream cleanup
                # raises, or its resource leaks for the LB process lifetime.
                if not released:
                    released = True
                    await release()

        self._release = _release_all


class _DrainableServer(uvicorn.Server):
    """A uvicorn Server that drains gracefully on SIGTERM.

    uvicorn installs its own SIGTERM/SIGINT handlers inside
    ``Server.serve()`` via ``capture_signals()`` (there is no
    ``install_signal_handlers`` config knob in modern uvicorn), and its default
    handler sets ``should_exit`` immediately -- which would kill in-flight
    requests and skip the deregister step. We instead install our own
    event-loop signal handlers (asyncio-safe) and suppress uvicorn's, so
    SIGTERM begins draining (fail readiness + stop the controller sync) and the
    server only exits after ``LB_DRAIN_GRACE_SECONDS`` -- long enough for k8s to
    pull the pod from the Service and for in-flight requests to finish. A
    second signal / SIGINT exits promptly.
    """

    def __init__(self, config: 'uvicorn.Config', on_drain: 'Any') -> None:
        super().__init__(config)
        self._on_drain = on_drain
        self._own_signals = False
        self._drain_started = False

    def _force_exit(self) -> None:
        """Skip uvicorn's connection wait after a second termination signal."""
        self.should_exit = True
        self.force_exit = True

    def _handle_sigterm(self, loop: asyncio.AbstractEventLoop) -> None:
        """Drain on first SIGTERM; force shutdown on the second."""
        if self._drain_started:
            self._force_exit()
            return
        self._drain_started = True
        self._on_drain()
        loop.call_later(constants.LB_DRAIN_GRACE_SECONDS,
                        lambda: setattr(self, 'should_exit', True))

    @contextlib.contextmanager
    def capture_signals(self):
        # Suppress uvicorn's own signal handlers when we installed ours;
        # otherwise fall back to uvicorn's handling (e.g. platforms without
        # loop.add_signal_handler).
        if self._own_signals:
            yield
        else:
            with super().capture_signals():
                yield

    async def serve_with_drain(self) -> None:
        loop = asyncio.get_running_loop()

        try:
            loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm, loop)
            # SIGINT is the operator's immediate escape hatch, including
            # during an already-started SIGTERM drain.
            loop.add_signal_handler(signal.SIGINT, self._force_exit)
            self._own_signals = True
        except NotImplementedError:
            # add_signal_handler is unavailable (e.g. Windows); let uvicorn
            # manage signals (no graceful drain, but a correct shutdown).
            self._own_signals = False
        await self.serve()


class _InboundAuthMiddleware:
    """Pure-ASGI bearer gate for inbound inference requests (data-plane auth).

    Implemented as raw ASGI rather than ``BaseHTTPMiddleware`` on purpose: it
    consumes the dedicated LB credential header and either short-circuits with
    a 401 or delegates to the app. ``Authorization`` remains available for the
    replica's own auth, while the LB credential cannot leak downstream. The
    middleware NEVER buffers or re-relays the response body, so streaming/SSE
    responses and the catch-all proxy's slot-release pass through untouched.

    When data-plane auth is enabled, missing or unreadable material fails
    closed. It is disabled by default until the platform has a real inference
    caller that can inject the dedicated credential.
    Exempts ONLY GET/HEAD on the readiness route -- any other method there
    falls through to the (authenticated) catch-all proxy. All overlap tokens
    are accepted during rotation. Constant-time compare, ASCII-guarded so a
    malformed header is a clean 401 rather than a 500.
    """

    def __init__(self, app) -> None:
        self._app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope['type'] == 'http':
            try:
                authorized = self._authorized(scope)
            except serve_utils.AuthTokenConfigurationError as e:
                logger.error(
                    'Load-balancer authentication is unavailable: '
                    '%s', e)
                await fastapi.responses.JSONResponse(
                    status_code=503,
                    content={
                        'detail': 'Load-balancer authentication is unavailable.'
                    })(scope, receive, send)
                return
            if not authorized:
                await fastapi.responses.JSONResponse(
                    status_code=401,
                    content={'detail': 'Unauthorized.'})(scope, receive, send)
                return
            scope = self._without_auth_header(scope)
        await self._app(scope, receive, send)

    @staticmethod
    def _authorized(scope) -> bool:
        if (scope['method'] in ('GET', 'HEAD') and
                scope['path'] in (constants.LB_HEALTH_ENDPOINT_PATH,
                                  constants.LB_LIVENESS_ENDPOINT_PATH)):
            return True
        if not serve_utils.is_lb_data_plane_auth_enabled():
            return True
        expected_tokens = serve_utils.get_lb_auth_tokens(required=True)
        authorization = None
        for name, value in scope.get('headers', []):
            if name.lower() == constants.LB_AUTHORIZATION_HEADER_BYTES:
                authorization = value.decode('latin-1')
                break
        if authorization is None or not authorization.isascii():
            return False
        authorized = False
        for expected_token in expected_tokens:
            authorized |= hmac.compare_digest(authorization,
                                              f'Bearer {expected_token}')
        return authorized

    @staticmethod
    def _without_auth_header(scope):
        """Return a scope with the LB-only credential consumed."""
        filtered_headers = [
            (name, value)
            for name, value in scope.get('headers', [])
            if name.lower() != constants.LB_AUTHORIZATION_HEADER_BYTES
        ]
        if len(filtered_headers) == len(scope.get('headers', [])):
            return scope
        filtered_scope = dict(scope)
        filtered_scope['headers'] = filtered_headers
        return filtered_scope
