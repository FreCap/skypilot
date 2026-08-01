# pyright: reportOptionalMemberAccess=error
"""SkyPilot API Server exposing RESTful APIs."""

import asyncio
import contextlib
import datetime
import html
import json
import os
import pathlib
import re
import resource
import selectors
import shutil
import socket
import threading
import time
import traceback
from typing import Any, Literal, ParamSpec
import uuid
import zlib

import fastapi
from fastapi import exception_handlers as fastapi_exception_handlers
from fastapi import exceptions as fastapi_exceptions
from fastapi.middleware import cors
import starlette.background

import sky
from sky import catalog
from sky import check as sky_check
from sky import core
from sky import estimated_spend as estimated_spend_lib
from sky import exceptions
from sky import execution
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.container_images import server as container_images_rest
from sky.data import storage_utils
from sky.jobs.server import server as jobs_rest
from sky.metrics import utils as metrics_utils
from sky.provision import metadata_utils
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.provision.slurm import utils as slurm_utils
from sky.recipes import server as recipes_rest
from sky.schemas.api import responses
from sky.serve import constants as serve_constants
from sky.serve import lb_rbac_preflight
from sky.serve import serve_utils
from sky.serve.server import controller_proxy as serve_controller_proxy
from sky.serve.server import server as serve_rest
from sky.server import common
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import core_middleware
from sky.server import csp_utils
from sky.server import daemons
from sky.server import dashboard as dashboard_app
from sky.server import file_mount_uploads
from sky.server import metrics
from sky.server import plugins
from sky.server import ssh_proxy
from sky.server import state
from sky.server import stream_utils
from sky.server import version_check
from sky.server import versions
from sky.server import websocket_utils
from sky.server.auth import middleware as auth_middleware
from sky.server.auth import oauth2_proxy
from sky.server.auth import sessions as auth_sessions
from sky.server.blob import blob_storage as bs
from sky.server.events import server as events_rest
from sky.server.requests import executor
from sky.server.requests import log_provider
from sky.server.requests import payloads
from sky.server.requests import preconditions
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.server.requests import role_filter
from sky.skylet import constants
from sky.ssh_node_pools import server as ssh_node_pools_rest
from sky.users import permission
from sky.users import rbac
from sky.users import server as users_rest
from sky.utils import admin_policy_utils
from sky.utils import asyncio_utils
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import context
from sky.utils import dag_utils
from sky.utils import debug_utils
from sky.utils import env_options
from sky.utils import interactive_utils
from sky.utils import perf_utils
from sky.utils import ux_utils
from sky.utils.kubernetes import gpu_labeler
from sky.volumes.server import server as volumes_rest
from sky.workspaces import server as workspaces_rest

P = ParamSpec('P')

logger = sky_logging.init_logger(__name__)

# Portable deployment provenance for the code instance serving this process.
# Capturing this at module initialization avoids adding Helm/Kubernetes calls
# to the health endpoint and works for local, VM, container, and K8s servers.
_SERVER_STARTED_AT = datetime.datetime.now(
    datetime.timezone.utc).isoformat(timespec='seconds')

# TODO(zhwu): Streaming requests, such log tailing after sky launch or sky logs,
# need to be detached from the main requests queue. Otherwise, the streaming
# response will block other requests from being processed.

# These aliases preserve the historical sky.server.server import surface.
# pylint: disable=protected-access
_basic_auth_401_response = auth_middleware._basic_auth_401_response
_bearer_auth_401_response = auth_middleware._bearer_auth_401_response
_try_set_basic_auth_user = auth_middleware._try_set_basic_auth_user
RBACMiddleware = auth_middleware.RBACMiddleware
_extract_identity_from_jwt = auth_middleware._extract_identity_from_jwt
_extract_user_from_header = auth_middleware._extract_user_from_header
_get_auth_user_header = auth_middleware._get_auth_user_header
_generate_auth_token = auth_middleware._generate_auth_token
InitializeRequestAuthUserMiddleware = (
    auth_middleware.InitializeRequestAuthUserMiddleware)
BasicAuthMiddleware = auth_middleware.BasicAuthMiddleware
BearerTokenMiddleware = auth_middleware.BearerTokenMiddleware
InternalServeControllerSyncAuthMiddleware = (
    auth_middleware.InternalServeControllerSyncAuthMiddleware)
AuthProxyMiddleware = auth_middleware.AuthProxyMiddleware
# pylint: enable=protected-access

# These aliases preserve the historical sky.server.server import surface while
# the cohesive file-mount upload implementation lives behind its own router.
cleanup_upload_ids = file_mount_uploads.cleanup_upload_ids
cleanup_unreferenced_file_mounts = (
    file_mount_uploads.cleanup_unreferenced_file_mounts)
upload_ids_to_cleanup = file_mount_uploads.upload_ids_to_cleanup
upload_zip_file = file_mount_uploads.upload_zip_file
check_blob_exists = file_mount_uploads.check_blob_exists
upload_blob = file_mount_uploads.upload_blob
unzip_file = file_mount_uploads.unzip_file
_is_relative_to = file_mount_uploads.is_relative_to

# These aliases preserve the historical sky.server.server import surface while
# the dashboard presentation implementation lives behind its own router.
# pylint: disable=protected-access
InternalDashboardPrefixMiddleware = (
    dashboard_app.InternalDashboardPrefixMiddleware)
CacheControlStaticMiddleware = dashboard_app.CacheControlStaticMiddleware
PathCleanMiddleware = dashboard_app.PathCleanMiddleware
_load_dynamic_routes = dashboard_app._load_dynamic_routes
_get_dynamic_routes = dashboard_app._get_dynamic_routes
_resolve_dynamic_route = dashboard_app._resolve_dynamic_route
_serve_html_with_nonce = dashboard_app._serve_html_with_nonce
serve_dashboard = dashboard_app.serve_dashboard
root = dashboard_app.root

# Preserve module and pickle identities for historical imports.
for _dashboard_symbol in (
        InternalDashboardPrefixMiddleware,
        CacheControlStaticMiddleware,
        PathCleanMiddleware,
        _load_dynamic_routes,
        _get_dynamic_routes,
        _resolve_dynamic_route,
        _serve_html_with_nonce,
        serve_dashboard,
        root,
):
    _dashboard_symbol.__module__ = __name__
# pylint: enable=protected-access

RequestIDMiddleware = core_middleware.RequestIDMiddleware
SecurityHeadersMiddleware = core_middleware.SecurityHeadersMiddleware
GracefulShutdownMiddleware = core_middleware.GracefulShutdownMiddleware
ControllerGenerationMiddleware = (
    core_middleware.ControllerGenerationMiddleware)
APIVersionMiddleware = core_middleware.APIVersionMiddleware

# Preserve historical import and pickle identities for the stable server
# facade. The aliases are the exact classes registered with FastAPI.
for _core_middleware_symbol in (
        RequestIDMiddleware,
        SecurityHeadersMiddleware,
        GracefulShutdownMiddleware,
        ControllerGenerationMiddleware,
        APIVersionMiddleware,
):
    _core_middleware_symbol.__module__ = __name__


def _cleanup_download_tmp_once() -> None:
    """Synchronously delete expired download temporary directories."""
    tmp_dir = bs.get_blob_storage().download_tmp_base_dir()
    if tmp_dir is None:
        # Backend shares the persistent log dir; no separate cleanup needed.
        return
    if not os.path.exists(tmp_dir):
        return
    cutoff = time.time() - bs.GC_GRACE_SECONDS
    with os.scandir(tmp_dir) as user_entries:
        for user_entry in user_entries:
            if not user_entry.is_dir():
                continue
            with os.scandir(user_entry.path) as entries:
                for entry in entries:
                    if entry.is_dir():
                        try:
                            if entry.stat().st_mtime < cutoff:
                                shutil.rmtree(entry.path, ignore_errors=True)
                        except OSError:
                            pass


async def cleanup_download_tmp():
    """Delete expired download tmp directories.

    Downloaded logs are transient — synced from the cluster for the client
    to download, then no longer needed.  Clean up anything older than the
    blob GC grace period (1 hour by default).
    """
    while True:
        await asyncio.sleep(3600)
        try:
            await asyncio.to_thread(_cleanup_download_tmp_once)
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Error in cleanup_download_tmp: '
                         f'{common_utils.format_exception(e)}')


async def loop_lag_monitor(loop: asyncio.AbstractEventLoop,
                           interval: float = 0.1) -> None:
    target = loop.time() + interval

    pid = str(os.getpid())
    lag_threshold = perf_utils.get_loop_lag_threshold()
    # Tumbling 30s window peak per process — paired with the pid-less lag
    # histogram so we keep per-worker visibility without histogram cardinality.
    # Uses loop.time() (monotonic) so NTP adjustments cannot warp the window.
    lag_max_window_seconds = 30.0
    lag_max_window_end = loop.time() + lag_max_window_seconds
    lag_max_in_window = 0.0

    def tick():
        nonlocal target, lag_max_window_end, lag_max_in_window
        now = loop.time()
        lag = max(0.0, now - target)
        if lag_threshold is not None and lag > lag_threshold:
            logger.warning(f'Event loop lag {lag} seconds exceeds threshold '
                           f'{lag_threshold} seconds.')
        metrics_utils.SKY_APISERVER_EVENT_LOOP_LAG_SECONDS.observe(lag)
        if now >= lag_max_window_end:
            lag_max_window_end = now + lag_max_window_seconds
            lag_max_in_window = lag
        else:
            lag_max_in_window = max(lag_max_in_window, lag)
        metrics_utils.SKY_APISERVER_EVENT_LOOP_LAG_MAX_SECONDS.labels(
            pid=pid).set(lag_max_in_window)
        target = now + interval
        loop.call_at(target, tick)

    loop.call_at(target, tick)


async def schedule_on_boot_check_async():
    try:
        await executor.schedule_request_async(
            request_id=server_constants.ON_BOOT_CHECK_REQUEST_ID,
            request_name=request_names.RequestName.CHECK,
            request_body=payloads.CheckBody(),
            func=sky_check.check,
            schedule_type=requests_lib.ScheduleType.SHORT,
            is_skypilot_system=True,
        )
    except exceptions.RequestAlreadyExistsError:
        # Lifespan will be executed in each uvicorn worker process, we
        # can safely ignore the error if the task is already scheduled.
        logger.debug(f'Request {server_constants.ON_BOOT_CHECK_REQUEST_ID} '
                     'already exists.')


# Strong references to fire-and-forget lifespan tasks: the event loop only
# keeps weak references to tasks, so an otherwise-unreferenced task can be
# garbage-collected mid-flight.
_lifespan_tasks: set['asyncio.Task'] = set()


def _spawn_lifespan_task(coro) -> None:
    task = asyncio.create_task(coro)
    _lifespan_tasks.add(task)
    task.add_done_callback(_lifespan_tasks.discard)


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):  # pylint: disable=redefined-outer-name
    """FastAPI lifespan context manager."""
    del app  # unused

    # Refuse to publish any API route if the external LB's sync credential can
    # also authenticate destructive controller-admin routes. The same check is
    # repeated on every purpose-specific token read so projected Secret
    # rotations remain fail-closed after startup.
    if serve_utils.is_external_load_balancer_mode():
        serve_utils.validate_controller_auth_token_isolation(required=True)

    # LB RBAC is namespace-wide, not service-specific. Run this once in the
    # API process instead of issuing 11 identical access reviews in every
    # per-service controller child during a large recovery.
    await asyncio.to_thread(lb_rbac_preflight.check_lb_rbac_preflight)

    # Start periodic version check task (runs daily)
    _spawn_lifespan_task(version_check.check_versions_periodically())
    if metrics_utils.METRICS_ENABLED:
        # Start monitoring the event loop lag in each server worker
        # event loop (process).
        _spawn_lifespan_task(loop_lag_monitor(asyncio.get_running_loop()))
    yield


app = fastapi.FastAPI(prefix='/api/v1', debug=True, lifespan=lifespan)
# Middleware wraps in the order defined here. E.g., given
#   app.add_middleware(Middleware1)
#   app.add_middleware(Middleware2)
#   app.add_middleware(Middleware3)
# The effect will be like:
#   Middleware3(Middleware2(Middleware1(request)))
# If MiddlewareN does something like print(n); call_next(); print(n), you'll get
#   3; 2; 1; <request>; 1; 2; 3
# Use environment variable to make the metrics middleware optional.
if os.environ.get(constants.ENV_VAR_SERVER_METRICS_ENABLED):
    app.add_middleware(metrics.PrometheusMiddleware)
app.add_middleware(APIVersionMiddleware)
# The order of all the authentication-related middleware is important.
# RBACMiddleware must precede all the auth middleware, so it can access
# request.state.auth_user.
app.add_middleware(RBACMiddleware)
app.add_middleware(InternalDashboardPrefixMiddleware)
app.add_middleware(GracefulShutdownMiddleware)
app.add_middleware(ControllerGenerationMiddleware)
app.add_middleware(PathCleanMiddleware)
app.add_middleware(CacheControlStaticMiddleware)
app.add_middleware(
    cors.CORSMiddleware,
    # TODO(zhwu): in production deployment, we should restrict the allowed
    # origins to the domains that are allowed to access the API server.
    allow_origins=['*'],  # Specify the correct domains for production
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
    expose_headers=['X-Skypilot-Request-ID'])
# Authentication based on oauth2-proxy.
app.add_middleware(oauth2_proxy.OAuth2ProxyMiddleware)
# AuthProxyMiddleware should precede BasicAuthMiddleware and
# BearerTokenMiddleware, since it should be skipped if either of those set the
# auth user.
app.add_middleware(AuthProxyMiddleware)
enable_basic_auth = os.environ.get(constants.ENV_VAR_ENABLE_BASIC_AUTH, 'false')
disable_basic_auth_middleware = os.environ.get(
    constants.SKYPILOT_DISABLE_BASIC_AUTH_MIDDLEWARE, 'false')
if (str(enable_basic_auth).lower() == 'true' and
        str(disable_basic_auth_middleware).lower() != 'true'):
    app.add_middleware(BasicAuthMiddleware)
# Bearer token middleware should always be present to handle service account
# authentication
app.add_middleware(BearerTokenMiddleware)
# This must be added after the normal auth middlewares (therefore wrapping
# them), but before InitializeRequestAuthUserMiddleware (which wraps this and
# initializes request.state.auth_user first). A valid dedicated sync token can
# then bypass Basic/OAuth; an invalid token never reaches them or the handler.
app.add_middleware(InternalServeControllerSyncAuthMiddleware)
# InitializeRequestAuthUserMiddleware must be the last added middleware so that
# request.state.auth_user is always set, but can be overridden by the auth
# middleware above.
app.add_middleware(InitializeRequestAuthUserMiddleware)
app.add_middleware(RequestIDMiddleware)
# SecurityHeadersMiddleware is the outermost middleware to ensure security
# headers (CSP, X-Content-Type-Options, etc.) are added to all responses.
app.add_middleware(SecurityHeadersMiddleware)

# Load plugins after all the middlewares are added, to keep the core
# middleware stack intact if a plugin adds new middlewares.
# Note: server.py will be imported twice in server process, once as
# the top-level entrypoint module and once imported by uvicorn, we only
# load the plugin when imported by uvicorn for server process.
# TODO(aylei): move uvicorn app out of the top-level module to avoid
# duplicate app initialization.
if __name__ == 'sky.server.server':
    plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.UVICORN,
                                 app=app))

app.include_router(jobs_rest.router, prefix='/jobs', tags=['jobs'])
app.include_router(serve_rest.router, prefix='/serve', tags=['serve'])
app.include_router(serve_controller_proxy.router)
app.include_router(users_rest.router, prefix='/users', tags=['users'])
app.include_router(workspaces_rest.router,
                   prefix='/workspaces',
                   tags=['workspaces'])
app.include_router(volumes_rest.router, prefix='/volumes', tags=['volumes'])
app.include_router(container_images_rest.router,
                   prefix='/images',
                   tags=['images'])
app.include_router(ssh_node_pools_rest.router,
                   prefix='/ssh_node_pools',
                   tags=['ssh_node_pools'])
app.include_router(recipes_rest.router, prefix='/recipes', tags=['recipes'])
app.include_router(events_rest.router, prefix='/events', tags=['events'])
app.include_router(file_mount_uploads.router)
# increase the resource limit for the server
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))


def _is_container_image_request_path(path: str) -> bool:
    return (path == '/images' or path.startswith('/images/') or
            path == '/api/v1/images' or path.startswith('/api/v1/images/'))


def _contains_container_image_input(value: Any) -> bool:
    """Returns whether rejected input may contain a managed-image selector."""
    if isinstance(value, str):
        return 'container_image' in value or 'image_id' in value
    if isinstance(value, dict):
        return any(key in ('container_image',
                           'image_id') or _contains_container_image_input(item)
                   for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_container_image_input(item) for item in value)
    return False


@app.exception_handler(fastapi_exceptions.RequestValidationError)
async def handle_request_validation_error(
    request: fastapi.Request,
    error: fastapi_exceptions.RequestValidationError,
) -> fastapi.responses.Response:
    """Prevents rejected container-image input from crossing the API wire."""
    if (not payloads.is_container_image_task_validation_error(error) and
            not _is_container_image_request_path(request.url.path) and
            not _contains_container_image_input(error.body)):
        return await fastapi_exception_handlers.request_validation_exception_handler(
            request, error)
    # FastAPI's default 422 body repeats both Pydantic's message and raw input.
    # Image references are an authentication boundary, so even rejected values
    # must not be reflected. Keep the standard detail-list shape without
    # including attacker-controlled locations, contexts, messages, or inputs.
    return fastapi.responses.JSONResponse(
        status_code=422,
        content={
            'detail': [{
                'type': 'value_error',
                'loc': ['request'],
                'msg': 'Invalid container image request.',
            }]
        })


@app.exception_handler(exceptions.ConcurrentWorkerExhaustedError)
def handle_concurrent_worker_exhausted_error(
        request: fastapi.Request, e: exceptions.ConcurrentWorkerExhaustedError):
    del request  # request is not used
    # Print detailed error message to server log
    logger.error('Concurrent worker exhausted: '
                 f'{common_utils.format_exception(e)}')
    with ux_utils.enable_traceback():
        logger.error(f'  Traceback: {traceback.format_exc()}')
    # Return human readable error message to client
    return fastapi.responses.JSONResponse(
        status_code=503,
        content={
            'detail':
                ('The server has exhausted its concurrent worker limit. '
                 'Please try again or scale the server if the load persists.')
        })


async def _read_html_template(template_name: str) -> str:
    template_path = pathlib.Path(__file__).parent / 'html' / template_name
    return await asyncio.to_thread(template_path.read_text, encoding='utf-8')


@app.get('/token')
async def token(request: fastapi.Request,
                local_port: int | None = None) -> fastapi.responses.Response:
    del local_port  # local_port is used by the served js, but ignored by server
    # Use base64 encoding to avoid having to escape anything in the HTML.
    base64_str = _generate_auth_token(request)
    user = _get_auth_user_header(request)

    try:
        html_content = await _read_html_template('token_page.html')
    except FileNotFoundError as e:
        raise fastapi.HTTPException(
            status_code=500, detail='Token page template not found.') from e

    user_info_string = html.escape(
        f'Logged in as {user.name}') if user is not None else ''
    html_content = html_content.replace(
        'SKYPILOT_API_SERVER_USER_TOKEN_PLACEHOLDER',
        base64_str).replace('USER_PLACEHOLDER', user_info_string)

    nonce = csp_utils.generate_nonce()
    request.state.csp_nonce = nonce
    html_content = csp_utils.inject_nonce_into_html(html_content, nonce)

    return fastapi.responses.HTMLResponse(
        content=html_content,
        headers={
            'Cache-Control': 'no-cache, no-transform',
            # X-Accel-Buffering: no is useful for preventing buffering issues
            # with some reverse proxies.
            'X-Accel-Buffering': 'no'
        })


@asyncio_utils.shield
async def _restore_cancelled_auth_session_poll(
        store: auth_sessions.AuthSessionStore,
        poll_task: asyncio.Task[str | None], code_verifier: str) -> None:
    """Restore a consumed token even if cancellation repeats."""
    auth_token = await poll_task
    if auth_token is not None:
        code_challenge = common_utils.compute_code_challenge(code_verifier)
        await asyncio.to_thread(store.restore_session, code_challenge,
                                auth_token)


async def _poll_auth_session(code_verifier: str) -> str | None:
    """Consume an auth session without losing it to caller cancellation."""
    store = auth_sessions.auth_session_store
    poll_task = asyncio.create_task(
        asyncio.to_thread(store.poll_session, code_verifier))
    try:
        return await asyncio.shield(poll_task)
    except asyncio.CancelledError:
        try:
            await _restore_cancelled_auth_session_poll(store, poll_task,
                                                       code_verifier)
        except Exception:  # pylint: disable=broad-except
            logger.exception('Failed to restore a cancelled auth session poll')
        raise


@app.get('/api/v1/auth/token')
async def poll_auth_token(
        code_verifier: str | None = None) -> fastapi.responses.Response:
    """Poll for auth token using code_verifier.

    Computes code_challenge from code_verifier to look up the session.

    Query params:
        code_verifier: The original code verifier (required)

    Returns:
        - 200 with token if session is authorized
        - 404 if session not found (user hasn't clicked Authorize yet)
    """
    if not code_verifier:
        raise fastapi.HTTPException(status_code=400,
                                    detail='code_verifier is required')

    # The auth session store performs a synchronous DB transaction. Keep CLI
    # polling from blocking unrelated API requests on this worker's event loop.
    auth_token = await _poll_auth_session(code_verifier)

    if auth_token is None:
        raise fastapi.HTTPException(status_code=404, detail='Session not found')

    return fastapi.responses.JSONResponse(content={'token': auth_token},
                                          headers={'Cache-Control': 'no-store'})


@app.post('/api/v1/auth/authorize')
async def authorize_auth_session(
        request: fastapi.Request) -> fastapi.responses.JSONResponse:
    """Authorize an auth session (called when user clicks Authorize button).

    This endpoint requires authentication (via auth proxy cookies).
    It generates the token and creates a session for the CLI to retrieve.

    Request body:
        code_challenge: The code challenge from the CLI

    Returns:
        - 200 if successfully authorized
    """
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        raise fastapi.HTTPException(status_code=400,
                                    detail='Invalid JSON body') from e

    code_challenge = body.get('code_challenge')
    if not code_challenge:
        raise fastapi.HTTPException(status_code=400,
                                    detail='code_challenge is required')
    # Validate format: base64url-encoded SHA256, 43 chars of A-Za-z0-9_-
    if not re.match(r'^[A-Za-z0-9_-]{43}$', code_challenge):
        raise fastapi.HTTPException(status_code=400,
                                    detail='Invalid code_challenge format')

    auth_token = _generate_auth_token(request)

    # Create the session with the token
    # Session creation is a synchronous DB transaction and can wait on another
    # worker, so run it outside the request event loop.
    await asyncio.to_thread(auth_sessions.auth_session_store.create_session,
                            code_challenge, auth_token)

    return fastapi.responses.JSONResponse(content={'status': 'authorized'},
                                          headers={'Cache-Control': 'no-store'})


@app.get('/auth/authorize')
async def authorize_page(
        request: fastapi.Request) -> fastapi.responses.Response:
    """Serve the authorization page where users click to authorize the CLI.

    This page requires authentication (via auth proxy). The code_challenge
    query param is read by JavaScript and sent to the POST endpoint.
    """
    user = request.state.auth_user
    if user is None:
        user = _get_auth_user_header(request)
    user_info = html.escape(
        f'Logged in as {user.name}') if user is not None else ''

    html_content = await _read_html_template('authorize_page.html')

    html_content = html_content.replace('USER_PLACEHOLDER', user_info)

    nonce = csp_utils.generate_nonce()
    request.state.csp_nonce = nonce
    html_content = csp_utils.inject_nonce_into_html(html_content, nonce)

    return fastapi.responses.HTMLResponse(
        content=html_content,
        headers={'Cache-Control': 'no-cache, no-transform'})


@app.post('/check')
async def check(request: fastapi.Request,
                check_body: payloads.CheckBody) -> None:
    """Checks enabled clouds."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CHECK,
        request_body=check_body,
        func=sky_check.check,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.get('/enabled_clouds')
async def enabled_clouds(request: fastapi.Request,
                         workspace: str | None = None,
                         expand: bool = False) -> None:
    """Gets enabled clouds on the server."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.ENABLED_CLOUDS,
        request_body=payloads.EnabledCloudsBody(workspace=workspace,
                                                expand=expand),
        func=core.enabled_clouds,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.get('/enabled_clouds/batch')
async def enabled_clouds_batch(request: fastapi.Request,
                               workspaces: str = '',
                               expand: bool = False) -> None:
    """Gets enabled clouds for multiple workspaces in a single request."""
    workspace_list = [w.strip() for w in workspaces.split(',') if w.strip()]
    # API-layer authorization: filter out workspaces the caller cannot access
    # before the request reaches the core function (defense-in-depth).
    auth_user = request.state.auth_user
    if auth_user is not None and workspace_list:
        workspace_list = list(
            permission.permission_service.get_accessible_workspace_names(
                auth_user.id, set(workspace_list)))
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.ENABLED_CLOUDS_BATCH,
        request_body=payloads.EnabledCloudsBatchBody(workspaces=workspace_list,
                                                     expand=expand),
        func=core.enabled_clouds_batch,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/realtime_kubernetes_gpu_availability')
async def realtime_kubernetes_gpu_availability(
    request: fastapi.Request,
    realtime_gpu_availability_body: payloads.RealtimeGpuAvailabilityRequestBody
) -> None:
    """Gets real-time Kubernetes GPU availability."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.
        REALTIME_KUBERNETES_GPU_AVAILABILITY,
        request_body=realtime_gpu_availability_body,
        func=core.realtime_kubernetes_gpu_availability,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/kubernetes_node_info')
async def kubernetes_node_info(
        request: fastapi.Request,
        kubernetes_node_info_body: payloads.KubernetesNodeInfoRequestBody
) -> None:
    """Gets Kubernetes nodes information and hints."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.KUBERNETES_NODE_INFO,
        request_body=kubernetes_node_info_body,
        func=kubernetes_utils.get_kubernetes_node_info,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/slurm_gpu_availability')
async def slurm_gpu_availability(
    request: fastapi.Request,
    slurm_gpu_availability_body: payloads.SlurmGpuAvailabilityRequestBody
) -> None:
    """Gets real-time Slurm GPU availability."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.REALTIME_SLURM_GPU_AVAILABILITY,
        request_body=slurm_gpu_availability_body,
        func=core.realtime_slurm_gpu_availability,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


# Keep the GET method for backwards compatibility
@app.api_route('/slurm_node_info', methods=['GET', 'POST'])
async def slurm_node_info(
        request: fastapi.Request,
        slurm_node_info_body: payloads.SlurmNodeInfoRequestBody) -> None:
    """Gets detailed information for each node in the Slurm cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SLURM_NODE_INFO,
        request_body=slurm_node_info_body,
        func=slurm_utils.slurm_node_info,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.get('/status_kubernetes')
async def status_kubernetes(request: fastapi.Request) -> None:
    """[Experimental] Get all SkyPilot resources (including from other '
    'users) in the current Kubernetes context."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.STATUS_KUBERNETES,
        request_body=payloads.RequestBody(),
        func=core.status_kubernetes,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/kubernetes_label_gpus')
async def kubernetes_label_gpus(
        request: fastapi.Request,
        kubernetes_label_gpus_body: payloads.KubernetesLabelGpusBody) -> None:
    """Labels GPU nodes in a Kubernetes cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.KUBERNETES_LABEL_GPUS,
        request_body=kubernetes_label_gpus_body,
        func=gpu_labeler.label_gpus_server,
        schedule_type=requests_lib.ScheduleType.LONG,  # Can take 10+ min
        auth_user=request.state.auth_user,
    )


@app.post('/list_accelerators')
async def list_accelerators(
        request: fastapi.Request,
        list_accelerator_counts_body: payloads.ListAcceleratorsBody) -> None:
    """Gets list of accelerators from cloud catalog."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.LIST_ACCELERATORS,
        request_body=list_accelerator_counts_body,
        func=catalog.list_accelerators,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/list_accelerator_counts')
async def list_accelerator_counts(
        request: fastapi.Request,
        list_accelerator_counts_body: payloads.ListAcceleratorCountsBody
) -> None:
    """Gets list of accelerator counts from cloud catalog."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.LIST_ACCELERATOR_COUNTS,
        request_body=list_accelerator_counts_body,
        func=catalog.list_accelerator_counts,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/validate')
async def validate(validate_body: payloads.ValidateBody) -> None:
    """Validates the user's DAG."""
    # TODO(SKY-1035): validate if existing cluster satisfies the requested
    # resources, e.g. sky exec --gpus V100:8 existing-cluster-with-no-gpus

    # TODO: Our current launch process is split into three calls:
    # validate, optimize, and launch. This requires us to apply the admin policy
    # in each step, which may be an expensive operation. We should consolidate
    # these into a single call or have a TTL cache for (task, admin_policy)
    # pairs.
    logger.debug(f'Validating tasks: {validate_body.dag}')

    context.initialize()
    ctx = context.get()
    assert ctx is not None
    # TODO(aylei): generalize this to all requests without a db record.
    ctx.override_envs(validate_body.env_vars)

    def validate_dag(dag: dag_utils.dag_lib.Dag):
        # TODO: Admin policy may contain arbitrary code, which may be expensive
        # to run and may block the server thread. However, moving it into the
        # executor adds a ~150ms penalty on the local API server because of
        # added RTTs. For now, we stick to doing the validation inline in the
        # server thread.
        with admin_policy_utils.apply_and_use_config_in_current_request(
                dag,
                request_name=request_names.AdminPolicyRequestName.VALIDATE,
                request_options=validate_body.get_request_options()) as dag:
            dag.resolve_and_validate_volumes()
            # Skip validating workdir and file_mounts, as those need to be
            # validated after the files are uploaded to the SkyPilot API server
            # with `upload_mounts_to_api_server`.
            dag.validate(skip_file_mounts=True, skip_workdir=True)

    try:
        dag = dag_utils.load_dag_from_yaml_str(validate_body.dag)
        # Apply admin policy and validate DAG is blocking, run it in a separate
        # thread executor to avoid blocking the uvicorn event loop.
        await asyncio.to_thread(validate_dag, dag)
    except Exception as e:  # pylint: disable=broad-except
        # Print the exception to the API server log.
        if env_options.Options.SHOW_DEBUG_INFO.get():
            logger.info('/validate exception:', exc_info=True)
        # Set the exception stacktrace for the serialized exception.
        requests_lib.set_exception_stacktrace(e)
        raise fastapi.HTTPException(
            status_code=400, detail=exceptions.serialize_exception(e)) from e


@app.post('/optimize')
async def optimize(optimize_body: payloads.OptimizeBody,
                   request: fastapi.Request) -> None:
    """Optimizes the user's DAG."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.OPTIMIZE,
        request_body=optimize_body,
        ignore_return_value=True,
        func=core.optimize,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/launch')
async def launch(launch_body: payloads.LaunchBody,
                 request: fastapi.Request) -> None:
    """Launches a cluster or task."""
    request_id = request.state.request_id
    logger.info(f'Launching request: {request_id}')
    launch_precondition = None
    if launch_body.is_launched_by_sky_serve_controller:
        launch_context = launch_body.extra_launch_context
        has_launch_fence = any(
            key in launch_context
            for key in serve_constants.REPLICA_LAUNCH_FENCE_KEYS)
        service_name = launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
        service_hash = launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
        service_version = launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY)
        controller_pid = launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY)
        controller_ip = launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY)
        if ((has_launch_fence or
             serve_utils.is_external_load_balancer_mode()) and
            (not isinstance(service_name, str) or not service_name or
             not isinstance(service_hash, str) or not service_hash or
             not (service_version is None or
                  type(service_version) is int and service_version > 0) or
             not (controller_pid is None or isinstance(controller_pid, int)) or
             not (controller_ip is None or isinstance(controller_ip, str)))):
            raise fastapi.HTTPException(
                status_code=409,
                detail='SkyServe replica launches require a complete durable '
                'service-owner fence.')
        if has_launch_fence:
            assert isinstance(service_name, str)
            assert isinstance(service_hash, str)
            launch_precondition = (
                preconditions.ServiceReplicaLaunchPrecondition(
                    request_id, service_name, service_hash, controller_pid,
                    controller_ip))
    await executor.schedule_request_async(
        request_id,
        request_name=request_names.RequestName.CLUSTER_LAUNCH,
        request_body=launch_body,
        func=execution.launch,
        schedule_type=requests_lib.ScheduleType.LONG,
        request_cluster_name=launch_body.cluster_name,
        precondition=launch_precondition,
        retryable=launch_body.retry_until_up,
        auth_user=request.state.auth_user,
    )


@app.post('/exec')
# pylint: disable=redefined-builtin
async def exec(request: fastapi.Request, exec_body: payloads.ExecBody) -> None:
    """Executes a task on an existing cluster."""
    cluster_name = exec_body.cluster_name
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_EXEC,
        request_body=exec_body,
        func=execution.exec,
        precondition=preconditions.ClusterStartCompletePrecondition(
            request_id=request.state.request_id,
            cluster_name=cluster_name,
        ),
        schedule_type=requests_lib.ScheduleType.LONG,
        request_cluster_name=cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/stop')
async def stop(request: fastapi.Request,
               stop_body: payloads.StopOrDownBody) -> None:
    """Stops a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_STOP,
        request_body=stop_body,
        func=core.stop,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=stop_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/status')
async def status(
    request: fastapi.Request,
    status_body: payloads.StatusBody = fastapi.Depends(
        role_filter.force_viewer_status_body),
) -> None:
    """Gets cluster statuses."""
    if state.get_block_requests():
        raise fastapi.HTTPException(
            status_code=503,
            detail='Server is shutting down, please try again later.')
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_STATUS,
        request_body=status_body,
        func=core.status,
        schedule_type=(requests_lib.ScheduleType.LONG if status_body.refresh
                       != common_lib.StatusRefreshMode.NONE else
                       requests_lib.ScheduleType.SHORT),
        auth_user=request.state.auth_user,
    )


@app.post('/endpoints')
async def endpoints(request: fastapi.Request,
                    endpoint_body: payloads.EndpointsBody) -> None:
    """Gets the endpoint for a given cluster and port number (endpoint)."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_ENDPOINTS,
        request_body=endpoint_body,
        func=core.endpoints,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=endpoint_body.cluster,
        auth_user=request.state.auth_user,
    )


@app.post('/down')
async def down(request: fastapi.Request,
               down_body: payloads.StopOrDownBody) -> None:
    """Tears down a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_DOWN,
        request_body=down_body,
        func=core.user_initiated_down,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=down_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/start')
async def start(request: fastapi.Request,
                start_body: payloads.StartBody) -> None:
    """Restarts a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_START,
        request_body=start_body,
        func=core.start,
        schedule_type=requests_lib.ScheduleType.LONG,
        request_cluster_name=start_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/autostop')
async def autostop(request: fastapi.Request,
                   autostop_body: payloads.AutostopBody) -> None:
    """Schedules an autostop/autodown for a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_AUTOSTOP,
        request_body=autostop_body,
        func=core.autostop,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=autostop_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/queue')
async def queue(request: fastapi.Request,
                queue_body: payloads.QueueBody) -> None:
    """Gets the job queue of a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_QUEUE,
        request_body=queue_body,
        func=core.queue,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=queue_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/job_status')
async def job_status(request: fastapi.Request,
                     job_status_body: payloads.JobStatusBody) -> None:
    """Gets the status of a job."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_JOB_STATUS,
        request_body=job_status_body,
        func=core.job_status,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=job_status_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/cancel')
async def cancel(request: fastapi.Request,
                 cancel_body: payloads.CancelBody) -> None:
    """Cancels jobs on a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_JOB_CANCEL,
        request_body=cancel_body,
        func=core.cancel,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=cancel_body.cluster_name,
        auth_user=request.state.auth_user,
    )


@app.post('/logs')
async def logs(
    request: fastapi.Request, cluster_job_body: payloads.ClusterJobBody,
    background_tasks: fastapi.BackgroundTasks
) -> fastapi.responses.StreamingResponse:
    """Tails the logs of a job."""
    stream_utils.ensure_request_log_storage_available()
    # TODO(zhwu): This should wait for the request on the cluster, e.g., async
    # launch, to finish, so that a user does not need to manually pull the
    # request status.
    kill_request_on_disconnect = False
    if executor.api_process_execution_enabled():
        executor.check_request_thread_executor_available()
        request_task = await executor.prepare_request_async(
            request_id=request.state.request_id,
            request_name=request_names.RequestName.CLUSTER_JOB_LOGS,
            request_body=cluster_job_body,
            func=core.tail_logs,
            schedule_type=requests_lib.ScheduleType.SHORT,
            request_cluster_name=cluster_job_body.cluster_name,
            auth_user=request.state.auth_user,
        )
        task = executor.execute_request_in_coroutine(request_task)
        background_tasks.add_task(task.cancel)
    else:
        await executor.schedule_request_async(
            request_id=request.state.request_id,
            request_name=request_names.RequestName.CLUSTER_JOB_LOGS,
            request_body=cluster_job_body,
            func=core.tail_logs,
            schedule_type=requests_lib.ScheduleType.SHORT,
            request_cluster_name=cluster_job_body.cluster_name,
            auth_user=request.state.auth_user,
        )
        request_task = await requests_lib.get_request_async(
            request.state.request_id)
        assert request_task is not None
        kill_request_on_disconnect = True
    # TODO(zhwu): This makes viewing logs in browser impossible. We should adopt
    # the same approach as /stream.
    return stream_utils.stream_response_for_long_request(
        request_id=request.state.request_id,
        logs_path=request_task.log_path,
        background_tasks=background_tasks,
        kill_request_on_disconnect=kill_request_on_disconnect,
    )


@app.post('/download_logs')
async def download_logs(
        request: fastapi.Request,
        cluster_jobs_body: payloads.ClusterJobsDownloadLogsBody) -> None:
    """Downloads the logs of a job."""
    user_hash = common.get_request_user_id(request, cluster_jobs_body)
    logs_dir_on_api_server = await asyncio.to_thread(
        common.prepare_download_tmp_dir, user_hash)
    # We should reuse the original request body, so that the env vars, such as
    # user hash, are kept the same.
    cluster_jobs_body.local_dir = str(logs_dir_on_api_server)
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_JOB_DOWNLOAD_LOGS,
        request_body=cluster_jobs_body,
        func=core.download_logs,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=cluster_jobs_body.cluster_name,
        auth_user=request.state.auth_user,
    )


def _is_path_within(path: pathlib.Path, allowed_root: pathlib.Path) -> bool:
    """Returns whether a resolved path is contained by a resolved root."""
    try:
        return os.path.commonpath([path, allowed_root]) == str(allowed_root)
    except ValueError:
        # Different drives on Windows cannot have a common path.
        return False


def _resolve_download_paths(
        folder_path_strings: list[str], logs_dir_on_api_server: pathlib.Path,
        download_tmp: pathlib.Path) -> tuple[list[pathlib.Path], pathlib.Path]:
    """Validates requested folders and returns their canonical paths."""
    resolved_logs_root = logs_dir_on_api_server.expanduser().resolve()
    allowed_roots = {
        resolved_logs_root,
        download_tmp.expanduser().resolve(),
    }
    folder_paths = []
    for folder_path_str in folder_path_strings:
        folder_path = pathlib.Path(folder_path_str).expanduser().resolve()
        if not any(
                _is_path_within(folder_path, allowed_root)
                for allowed_root in allowed_roots):
            raise fastapi.HTTPException(
                status_code=400,
                detail=
                f'Invalid folder path: {folder_path}; {logs_dir_on_api_server}')

        if not folder_path.exists():
            raise fastapi.HTTPException(
                status_code=404, detail=f'Folder not found: {folder_path}')
        # Keep the canonical path so a symlink cannot be swapped after the
        # containment check but before the archive is created.
        folder_paths.append(folder_path)
    return folder_paths, resolved_logs_root


@app.post('/download')
async def download(download_body: payloads.DownloadBody,
                   request: fastapi.Request) -> None:
    """Downloads a folder from the cluster to the local machine."""
    user_hash = common.get_request_user_id(request, download_body)
    logs_dir_on_api_server = common.api_server_user_logs_dir_prefix(user_hash)
    download_tmp = await asyncio.to_thread(common.prepare_download_tmp_dir,
                                           user_hash)
    folder_paths, resolved_logs_root = await asyncio.to_thread(
        _resolve_download_paths, download_body.folder_paths,
        logs_dir_on_api_server, download_tmp)

    # Create a temporary zip file
    log_id = str(uuid.uuid4().hex)
    zip_filename = f'folder_{log_id}.zip'
    zip_path = resolved_logs_root / zip_filename
    archive_abandoned = threading.Event()

    def _remove_abandoned_archive() -> None:
        with contextlib.suppress(OSError):
            zip_path.unlink(missing_ok=True)

    try:

        def _zip_files_and_folders(folder_paths, zip_path):
            try:
                folders = [
                    str(folder_path.expanduser().resolve())
                    for folder_path in folder_paths
                ]
                # Check for optional query parameter to control zip entry
                # structure.
                relative = request.query_params.get('relative', 'home')
                if relative == 'items':
                    # Dashboard-friendly: entries relative to selected folders
                    storage_utils.zip_files_and_folders(folders,
                                                        zip_path,
                                                        relative_to_items=True)
                else:
                    # CLI-friendly (default): entries with full paths for
                    # mapping.
                    storage_utils.zip_files_and_folders(folders, zip_path)
            finally:
                # Cancelling to_thread() cannot stop this worker. If the
                # request abandoned its archive, remove it after the writer
                # closes the file.
                if archive_abandoned.is_set():
                    _remove_abandoned_archive()

        await asyncio.to_thread(_zip_files_and_folders, folder_paths, zip_path)

        # Add home path to the response headers, so that the client can replace
        # the remote path in the zip file to the local path.
        headers = {
            'Content-Disposition': f'attachment; filename="{zip_filename}"',
            'X-Home-Path': str(pathlib.Path.home())
        }

        # Return the zip file as a download. starlette.background.BackgroundTask
        # (singular) runs after the response body is sent. The earlier
        # `BackgroundTasks().add_task(...)` form was a bug — `.add_task`
        # returns None, so the unlink never ran and prepared zips
        # accumulated on disk per download.
        return fastapi.responses.FileResponse(
            path=zip_path,
            filename=zip_filename,
            media_type='application/zip',
            headers=headers,
            background=starlette.background.BackgroundTask(zip_path.unlink,
                                                           missing_ok=True))
    except asyncio.CancelledError:
        archive_abandoned.set()
        # Also clean here in case the worker finished just before cancellation
        # was delivered to the awaiting task.
        _remove_abandoned_archive()
        raise
    except Exception as e:
        archive_abandoned.set()
        _remove_abandoned_archive()
        raise fastapi.HTTPException(status_code=500,
                                    detail=f'Error creating zip file: {str(e)}')


# TODO(aylei): run it asynchronously after global_user_state support async op
@app.post('/provision_logs')
def provision_logs(provision_logs_body: payloads.ProvisionLogsBody,
                   follow: bool = True,
                   tail: int = 0) -> fastapi.responses.StreamingResponse:
    """Streams the provision.log for the latest launch request of a cluster."""
    log_path = None
    cluster_name = provision_logs_body.cluster_name
    worker = provision_logs_body.worker
    # stream head node logs
    if worker is None:
        # Prefer clusters table first, then cluster_history as fallback.
        log_path_str = global_user_state.get_cluster_provision_log_path(
            cluster_name)
        if not log_path_str:
            log_path_str = (
                global_user_state.get_cluster_history_provision_log_path(
                    cluster_name))
        if not log_path_str:
            raise fastapi.HTTPException(
                status_code=404,
                detail=('Provision log path is not recorded for this cluster. '
                        'Please relaunch to generate provisioning logs.'))
        log_path = pathlib.Path(log_path_str).expanduser().resolve()
        if not log_path.exists():
            raise fastapi.HTTPException(
                status_code=404,
                detail=(f'Provision log path does not exist: {str(log_path)}. '
                        'The API server has likely restarted since this '
                        'cluster was launched, and provision logs are stored '
                        'on the API server\'s local disk, so the log did not '
                        'survive the restart. Relaunch the cluster to '
                        'generate fresh provisioning logs.'))

    # stream worker node logs
    else:
        handle = global_user_state.get_handle_from_cluster_name(cluster_name)
        if handle is None:
            raise fastapi.HTTPException(
                status_code=404,
                detail=('Cluster handle is not recorded for this cluster. '
                        'Please relaunch to generate provisioning logs.'))
        # instance_ids includes head node
        instance_ids = handle.instance_ids
        if instance_ids is None:
            raise fastapi.HTTPException(
                status_code=400,
                detail='Instance IDs are not recorded for this cluster. '
                'Please relaunch to generate provisioning logs.')
        if worker > len(instance_ids) - 1:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f'Worker {worker} is out of range. '
                f'The cluster has {len(instance_ids)} nodes.')
        log_path = metadata_utils.get_instance_log_dir(
            handle.get_cluster_name_on_cloud(), instance_ids[worker])

    # Tail semantics: 0 means print all lines. Convert 0 -> None for streamer.
    effective_tail = None if tail is None or tail <= 0 else tail

    return fastapi.responses.StreamingResponse(
        content=stream_utils.log_streamer(None,
                                          log_path,
                                          tail=effective_tail,
                                          follow=follow,
                                          cluster_name=cluster_name),
        media_type='text/plain',
        headers={
            'Cache-Control': 'no-cache, no-transform',
            'X-Accel-Buffering': 'no',
            'Transfer-Encoding': 'chunked',
        },
    )


@app.post('/hook_logs')
async def hook_logs(
    request: fastapi.Request, hook_logs_body: payloads.HookLogsBody,
    background_tasks: fastapi.BackgroundTasks
) -> fastapi.responses.StreamingResponse:
    """Tails lifecycle-hook logs of a cluster.

    If ``event`` is None, auto-selects whichever hook event has fired.
    """
    kill_request_on_disconnect = False
    if executor.api_process_execution_enabled():
        executor.check_request_thread_executor_available()
        request_task = await executor.prepare_request_async(
            request_id=request.state.request_id,
            request_name=request_names.RequestName.CLUSTER_HOOK_LOGS,
            request_body=hook_logs_body,
            func=core.tail_hook_logs,
            schedule_type=requests_lib.ScheduleType.SHORT,
            request_cluster_name=hook_logs_body.cluster_name,
            auth_user=request.state.auth_user,
        )
        task = executor.execute_request_in_coroutine(request_task)
        background_tasks.add_task(task.cancel)
    else:
        await executor.schedule_request_async(
            request_id=request.state.request_id,
            request_name=request_names.RequestName.CLUSTER_HOOK_LOGS,
            request_body=hook_logs_body,
            func=core.tail_hook_logs,
            schedule_type=requests_lib.ScheduleType.SHORT,
            request_cluster_name=hook_logs_body.cluster_name,
            auth_user=request.state.auth_user,
        )
        request_task = await requests_lib.get_request_async(
            request.state.request_id)
        assert request_task is not None
        kill_request_on_disconnect = True
    return stream_utils.stream_response_for_long_request(
        request_id=request.state.request_id,
        logs_path=request_task.log_path,
        background_tasks=background_tasks,
        kill_request_on_disconnect=kill_request_on_disconnect,
    )


@app.post('/cost_report')
async def cost_report(request: fastapi.Request,
                      cost_report_body: payloads.CostReportBody) -> None:
    """Gets the cost report of a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_COST_REPORT,
        request_body=cost_report_body,
        func=core.cost_report,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.get('/estimated_spend')
def estimated_spend(
    request: fastapi.Request,
    days: int = estimated_spend_lib.DEFAULT_LOOKBACK_DAYS,
    group_by: estimated_spend_lib.GroupBy = estimated_spend_lib.GroupBy.JOB,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, Any]:
    """Returns the materialized compute-cost estimate to admins only."""
    auth_user = request.state.auth_user
    if auth_user is not None:
        roles = permission.permission_service.get_user_roles(auth_user.id)
        if rbac.RoleName.ADMIN.value not in roles:
            raise fastapi.HTTPException(
                status_code=403, detail='Only admins can view estimated spend.')
    try:
        return estimated_spend_lib.get_estimated_spend(
            days=days,
            group_by=group_by,
            start_date=start_date,
            end_date=end_date,
        )
    except estimated_spend_lib.InvalidDateRangeError as e:
        raise fastapi.HTTPException(status_code=422, detail=str(e)) from e


@app.get('/estimated_spend/drilldown')
def estimated_spend_drilldown(
    request: fastapi.Request,
    level: estimated_spend_lib.SpendDrilldownLevel,
    days: int = estimated_spend_lib.DEFAULT_LOOKBACK_DAYS,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    owner_user_hash: str | None = None,
    owner_unknown: bool = False,
    workload_type: str | None = None,
    workload_id: str | None = None,
    workload_task_id: int | None = None,
    offset: int = 0,
    limit: int = estimated_spend_lib.DRILLDOWN_DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Returns one materialized spend-attribution page to admins only."""
    auth_user = request.state.auth_user
    if auth_user is not None:
        roles = permission.permission_service.get_user_roles(auth_user.id)
        if rbac.RoleName.ADMIN.value not in roles:
            raise fastapi.HTTPException(
                status_code=403, detail='Only admins can view estimated spend.')
    try:
        return estimated_spend_lib.get_estimated_spend_drilldown(
            level=level,
            days=days,
            start_date=start_date,
            end_date=end_date,
            owner_user_hash=owner_user_hash,
            owner_unknown=owner_unknown,
            workload_type=workload_type,
            workload_id=workload_id,
            workload_task_id=workload_task_id,
            offset=offset,
            limit=limit,
        )
    except (estimated_spend_lib.InvalidDateRangeError,
            estimated_spend_lib.InvalidDrilldownScopeError) as e:
        raise fastapi.HTTPException(status_code=422, detail=str(e)) from e


def _operator_notification_user_id(request: fastapi.Request) -> str:
    """Return the current admin identity or reject the notification API."""
    auth_user = request.state.auth_user
    if auth_user is None:
        # Authentication-disabled local API servers retain their existing
        # single-user behavior and treat the local user as the operator.
        return common_utils.get_user_hash()
    roles = permission.permission_service.get_user_roles(auth_user.id)
    if rbac.RoleName.ADMIN.value not in roles:
        raise fastapi.HTTPException(
            status_code=403,
            detail='Only admins can view operator notifications.')
    return auth_user.id


@app.get('/notifications')
def operator_notifications(
        request: fastapi.Request,
        days: int = fastapi.Query(default=7, ge=1, le=30),
) -> dict[str, Any]:
    """Return recent coalesced notifications and the caller's unread state."""
    user_id = _operator_notification_user_id(request)
    since = int(time.time()) - days * 24 * 60 * 60
    return global_user_state.get_operator_notifications(user_id, since)


@app.post('/notifications/read')
def mark_operator_notifications_read(
    request: fastapi.Request,
    body: payloads.OperatorNotificationReadBody,
) -> dict[str, int]:
    """Advance the caller's notification cursor monotonically."""
    user_id = _operator_notification_user_id(request)
    if body.through_sequence < 0:
        raise fastapi.HTTPException(
            status_code=422, detail='through_sequence must be non-negative')
    cursor = global_user_state.mark_operator_notifications_read(
        user_id, body.through_sequence)
    return {'last_seen_sequence': cursor}


@app.post('/cluster_events')
async def cluster_events(
        request: fastapi.Request,
        cluster_events_body: payloads.ClusterEventsBody) -> None:
    """Gets events for a cluster."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CLUSTER_EVENTS,
        request_body=cluster_events_body,
        func=core.get_cluster_events,
        schedule_type=requests_lib.ScheduleType.SHORT,
        request_cluster_name=cluster_events_body.cluster_name or '',
        auth_user=request.state.auth_user,
    )


@app.get('/storage/ls')
async def storage_ls(request: fastapi.Request) -> None:
    """Gets the storages."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.STORAGE_LS,
        request_body=payloads.RequestBody(),
        func=core.storage_ls,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/storage/delete')
async def storage_delete(request: fastapi.Request,
                         storage_body: payloads.StorageBody) -> None:
    """Deletes a storage."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.STORAGE_DELETE,
        request_body=storage_body,
        func=core.storage_delete,
        schedule_type=requests_lib.ScheduleType.LONG,
        auth_user=request.state.auth_user,
    )


@app.post('/local_up')
async def local_up(request: fastapi.Request,
                   local_up_body: payloads.LocalUpBody) -> None:
    """Launches a Kubernetes cluster on API server."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.LOCAL_UP,
        request_body=local_up_body,
        func=core.local_up,
        schedule_type=requests_lib.ScheduleType.LONG,
        auth_user=request.state.auth_user,
    )


@app.post('/local_down')
async def local_down(request: fastapi.Request,
                     local_down_body: payloads.LocalDownBody) -> None:
    """Tears down the Kubernetes cluster started by local_up."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.LOCAL_DOWN,
        request_body=local_down_body,
        func=core.local_down,
        schedule_type=requests_lib.ScheduleType.LONG,
        auth_user=request.state.auth_user,
    )


async def get_expanded_request_id(request_id: str) -> str:
    """Gets the expanded request ID for a given request ID prefix."""
    request_tasks = await requests_lib.get_requests_async_with_prefix(
        request_id, fields=['request_id'])
    if request_tasks is None:
        raise fastapi.HTTPException(status_code=404,
                                    detail=f'Request {request_id!r} not found')
    if len(request_tasks) > 1:
        raise fastapi.HTTPException(status_code=400,
                                    detail=('Multiple requests found for '
                                            f'request ID prefix: {request_id}'))
    return request_tasks[0].request_id


# === API server related APIs ===
@app.get('/api/get')
async def api_get(request_id: str) -> payloads.RequestPayload:
    """Gets a request with a given request ID prefix."""
    # Validate request_id prefix matches a single request.
    request_id = await get_expanded_request_id(request_id)

    # Exponential backoff: start fast (10ms) for short requests like
    # status/queue, then back off to 100ms for long requests like
    # launch/exec.
    poll_interval = 0.01
    while True:
        req_status = await requests_lib.get_request_status_async(request_id)
        if req_status is None:
            print(f'No task with request ID {request_id}', flush=True)
            raise fastapi.HTTPException(
                status_code=404, detail=f'Request {request_id!r} not found')
        if (req_status.status == requests_lib.RequestStatus.RUNNING and
                daemons.is_daemon_request_id(request_id)):
            # Daemon requests run forever, break without waiting for complete.
            break
        if req_status.status > requests_lib.RequestStatus.RUNNING:
            break
        await asyncio.sleep(poll_interval)
        # Back off: 10ms -> 20ms -> 40ms -> 80ms -> 100ms (cap)
        poll_interval = min(poll_interval * 2, 0.1)
    request_task = await requests_lib.get_request_async(request_id)
    if request_task is None:
        # Request retention can delete an old terminal row after the status
        # poll above and before this full-row fetch.
        raise fastapi.HTTPException(status_code=404,
                                    detail=f'Request {request_id!r} not found')
    # TODO(aylei): refine this, /api/get will not be retried and this is
    # meaningless to retry. It is the original request that should be retried.
    if request_task.should_retry:
        raise fastapi.HTTPException(
            status_code=503, detail=f'Request {request_id!r} should be retried')
    request_error = request_task.get_error()
    if request_error is not None:
        raise fastapi.HTTPException(status_code=500,
                                    detail=request_task.encode().model_dump())
    return request_task.encode()


def _resolve_stream_log_path(log_path: str) -> pathlib.Path:
    """Resolve and validate a user-supplied log path."""
    if log_path == constants.API_SERVER_LOGS:
        resolved_log_path = pathlib.Path(constants.API_SERVER_LOGS).expanduser()
        if not resolved_log_path.exists():
            raise fastapi.HTTPException(
                status_code=404,
                detail='Server log file does not exist. The API server may '
                'have been started with `--foreground` - check the '
                'stdout of API server process, such as: '
                '`kubectl logs -n api-server-namespace '
                'api-server-pod-name`')
        return resolved_log_path

    # This should be a log path under ~/sky_logs.
    resolved_logs_directory = pathlib.Path(
        constants.SKY_LOGS_DIRECTORY).expanduser().resolve()
    resolved_log_path = resolved_logs_directory.joinpath(log_path).resolve()
    # Make sure the log path is under ~/sky_logs. We calculate the
    # common path to check if the log path is under ~/sky_logs.
    # This prevents path traversal using '..'
    if os.path.commonpath([resolved_log_path, resolved_logs_directory
                          ]) != str(resolved_logs_directory):
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Unauthorized log path: '
                                    f'{log_path!r}')
    if not resolved_log_path.exists():
        raise fastapi.HTTPException(
            status_code=404, detail=f'Log path {log_path!r} does not exist')
    return resolved_log_path


@app.get('/api/stream')
async def stream(
    request: fastapi.Request,
    request_id: str | None = None,
    log_path: str | None = None,
    tail: int | None = None,
    follow: bool = True,
    # Choices: 'auto', 'plain', 'html', 'console'
    # 'auto': automatically choose between HTML and plain text
    #         based on the request source
    # 'plain': plain text for HTML clients
    # 'html': HTML for browsers
    # 'console': console for CLI/API clients
    # pylint: disable=redefined-builtin
    format: Literal['auto', 'plain', 'html', 'console'] = 'auto',
    # When set, return the stream as an attachment (browser download)
    # with this filename. Forces plain-text formatting so the saved
    # file is the raw log content. Use this to download large running
    # job logs via `<a download href=/api/stream?...>`: bytes start
    # flowing the moment the underlying request emits its first chunk,
    # so the user sees the OS save dialog immediately instead of
    # waiting for sync_down to complete.
    download: str | None = None,  # pylint: disable=redefined-outer-name
    # When 'gz', gzip-stream the bytes inline and adjust the saved
    # filename to end in .gz. Text logs compress ~10-30x, which makes
    # multi-GB downloads dramatically faster and smaller; macOS Finder
    # and most Linux file managers auto-extract on open.
    compress: Literal['gz'] | None = None,
) -> fastapi.responses.Response:
    """Streams the logs of a request.

    When format is 'auto' and the request is coming from a browser, the response
    is a HTML page with JavaScript to handle streaming, which will request the
    API server again with format='plain' to get the actual log content.

    Args:
        request_id: Request ID to stream logs for.
        log_path: Log path to stream logs for.
        tail: Number of lines to stream from the end of the log file.
        follow: Whether to follow the log file.
        format: Response format - 'auto' (HTML for browsers, plain for HTML
            clients, console for CLI/API clients), 'plain' (force plain text),
            'html' (force HTML), or 'console' (force console)
    """
    # We need to save the user-supplied request ID for the response header.
    user_supplied_request_id = request_id
    if request_id is not None and log_path is not None:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Only one of request_id and log_path can be provided')

    if request_id is not None:
        request_id = await get_expanded_request_id(request_id)

    if request_id is None and log_path is None:
        request_id = await requests_lib.get_latest_request_id_async()
        if request_id is None:
            raise fastapi.HTTPException(status_code=404,
                                        detail='No request found')

    # download mode forces a plain-text streaming response with an
    # attachment header — the browser saves the bytes to disk as they
    # arrive instead of rendering them.
    if download is not None:
        format = 'plain'
        use_html = False
    elif format == 'auto':
        # Check if request is coming from a browser
        user_agent = request.headers.get('user-agent', '').lower()
        use_html = any(browser in user_agent
                       for browser in ['mozilla', 'chrome', 'safari', 'edge'])
    else:
        use_html = format == 'html'

    if use_html:
        # Return HTML page with JavaScript to handle streaming
        stream_url = request.url.include_query_params(format='plain')
        html_content = await _read_html_template('log.html')
        html_content = html_content.replace(
            '{stream_url}',  # noqa: RUF027
            str(stream_url))

        nonce = csp_utils.generate_nonce()
        request.state.csp_nonce = nonce
        html_content = csp_utils.inject_nonce_into_html(html_content, nonce)

        return fastapi.responses.HTMLResponse(
            html_content,
            headers={
                'Cache-Control': 'no-cache, no-transform',
                'X-Accel-Buffering': 'no'
            })

    polling_interval = stream_utils.DEFAULT_POLL_INTERVAL
    # Original plain text streaming logic
    if request_id is not None:
        request_task = await requests_lib.get_request_async(
            request_id, fields=['request_id', 'schedule_type'])
        if request_task is None:
            print(f'No task with request ID {request_id}')
            raise fastapi.HTTPException(
                status_code=404, detail=f'Request {request_id!r} not found')
        # req.log_path is derived from request_id,
        # so it's ok to just grab the request_id in the above query.
        log_path_to_stream = request_task.log_path
        if request_task.schedule_type == requests_lib.ScheduleType.LONG:
            polling_interval = stream_utils.LONG_REQUEST_POLL_INTERVAL
        del request_task
    else:
        assert log_path is not None, (request_id, log_path)
        log_path_to_stream = await asyncio.to_thread(_resolve_stream_log_path,
                                                     log_path)

    headers = {
        'Cache-Control': 'no-cache, no-transform',
        'X-Accel-Buffering': 'no',
        'Transfer-Encoding': 'chunked'
    }
    if request_id is not None:
        headers[server_constants.STREAM_REQUEST_HEADER] = (
            user_supplied_request_id
            if user_supplied_request_id else request_id)
    if download is not None:
        # Sanitize the filename to prevent header injection (CR/LF) and
        # path traversal (slashes, ..). Restrict to a conservative
        # ASCII set so we don't have to worry about UTF-8 truncation
        # landing mid-codepoint.
        safe_filename = re.sub(r'[^A-Za-z0-9._-]+', '_', download)[:200]
        if not safe_filename:
            safe_filename = 'download'
        if compress == 'gz' and not safe_filename.endswith('.gz'):
            safe_filename = f'{safe_filename}.gz'
        headers['Content-Disposition'] = (
            f'attachment; filename="{safe_filename}"')

    if request_id is not None:
        content = log_provider.get_log_provider().log_stream(
            request_id=request_id,
            log_path=log_path_to_stream,
            plain_logs=format == 'plain',
            tail=tail,
            follow=follow,
            polling_interval=polling_interval)
    else:
        content = stream_utils.log_streamer(request_id=None,
                                            log_path=log_path_to_stream,
                                            plain_logs=format == 'plain',
                                            tail=tail,
                                            follow=follow,
                                            polling_interval=polling_interval)

    media_type = 'text/plain'
    if compress == 'gz':
        # Gzip-stream the chunks. We do this as PAYLOAD (not transport)
        # encoding because the browser would decompress the latter
        # before saving — defeating the bandwidth/disk savings. The
        # downloaded file is a real .log.gz that double-clicks open
        # on macOS / extracts trivially with `gunzip` on Linux.
        media_type = 'application/gzip'
        # zlib.MAX_WBITS | 16 = gzip wrapper.
        compressor = zlib.compressobj(6, zlib.DEFLATED, 16 + zlib.MAX_WBITS)

        async def gzipped():
            # Track whether we ever observed a non-empty source chunk so
            # the empty-stream signal (used by the SDK to fall back to
            # the rsync path for terminal jobs) survives gzip framing.
            # The gzip header alone is ~10 bytes; we suppress it
            # entirely for an empty source by skipping the trailing
            # flush() in that case.
            saw_payload = False
            try:
                async for chunk in content:
                    if isinstance(chunk, str):
                        chunk_bytes = chunk.encode('utf-8')
                    else:
                        chunk_bytes = chunk
                    if chunk_bytes:
                        saw_payload = True
                        compressed = compressor.compress(chunk_bytes)
                        if compressed:
                            yield compressed
            except (asyncio.CancelledError, GeneratorExit):  # pylint: disable=try-except-raise
                # Client disconnect: PEP 525 forbids yielding while a
                # GeneratorExit is propagating, so we explicitly do
                # not run the flush() yield below.
                raise
            # Natural EOF only — emit the gzip trailer if we actually
            # produced anything; otherwise the response stays empty so
            # the SDK's bytes_written==0 fallback fires.
            if saw_payload:
                tail_bytes = compressor.flush()
                if tail_bytes:
                    yield tail_bytes

        out_content: Any = gzipped()
    else:
        out_content = content

    return fastapi.responses.StreamingResponse(
        content=out_content,
        media_type=media_type,
        headers=headers,
    )


@app.post('/api/cancel')
async def api_cancel(request: fastapi.Request,
                     request_cancel_body: payloads.RequestCancelBody) -> None:
    """Cancels requests."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.API_CANCEL,
        request_body=request_cancel_body,
        func=requests_lib.kill_requests_with_prefix,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.get('/api/status')
async def api_status(
    request_ids: list[str] | None = fastapi.Query(
        None, description='Request ID prefixes to get status for.'),
    all_status: bool = fastapi.Query(
        False, description='Get finished requests as well.'),
    limit: int | None = fastapi.Query(
        None, description='Number of requests to show.'),
    fields: list[str] | None = fastapi.Query(
        None, description='Fields to get. If None, get all fields.'),
    cluster_name: str | None = fastapi.Query(
        None, description='Filter requests by cluster name.'),
) -> list[payloads.RequestPayload]:
    """Gets the list of requests."""
    if request_ids is None:
        statuses = None
        if not all_status:
            statuses = requests_lib.RequestStatus.active_statuses()
        request_tasks = await requests_lib.get_request_tasks_async(
            req_filter=requests_lib.RequestTaskFilter(
                status=statuses,
                cluster_names=[cluster_name] if cluster_name else None,
                exclude_request_names=[
                    server_constants.REQUEST_NAME_PREFIX + d.value
                    for d in daemons.HIDDEN_REQUEST_NAMES
                ],
                limit=limit,
                fields=fields,
                sort=True,
            ))
        # encode_requests does a sync get_all_users() DB read; offload it so
        # the event loop is not blocked.
        return await asyncio.to_thread(requests_lib.encode_requests,
                                       request_tasks)
    else:
        matched_request_tasks = []
        for request_id in request_ids:
            request_tasks = await requests_lib.get_requests_async_with_prefix(
                request_id)
            if request_tasks is None:
                continue
            matched_request_tasks.extend(request_tasks)
        # encode_requests resolves user names with a single batched
        # get_all_users() lookup for all matched rows; offload the sync DB
        # read so the event loop is not blocked.
        return await asyncio.to_thread(requests_lib.encode_requests,
                                       matched_request_tasks)


@app.get('/dashboard_config')
async def dashboard_config() -> dict[str, Any]:
    """Returns admin-configured dashboard settings consumed by the UI.

    Currently exposes the optional `external_links` allowlist that the dashboard
    matches against streamed logs to render labeled external links on cluster
    and job detail pages.
    """
    external_links = skypilot_config.get_nested(('dashboard', 'external_links'),
                                                [])
    sanitized: list[dict[str, str]] = []
    if isinstance(external_links, list):
        for entry in external_links:
            if not isinstance(entry, dict):
                continue
            label = entry.get('label')
            regex = entry.get('regex')
            if isinstance(label, str) and isinstance(regex, str):
                sanitized.append({'label': label, 'regex': regex})
    return {'external_links': sanitized}


@app.get('/api/plugins')
async def list_plugins() -> dict[str, list[dict[str, Any]]]:
    """Return metadata about loaded backend plugins."""
    plugin_infos = []
    for plugin_info in plugins.get_plugins():
        if plugin_info.hidden_from_display:
            continue
        info = {
            'js_extension_path': plugin_info.js_extension_path,
            'requires_early_init': plugin_info.requires_early_init,
        }
        for attr in ('name', 'version', 'commit'):
            value = getattr(plugin_info, attr, None)
            if value is not None:
                info[attr] = value
        plugin_infos.append(info)
    return {'plugins': plugin_infos}


@app.get('/api/health/ready')
async def readiness() -> dict[str, str]:
    """Check PostgreSQL-backed role readiness for Kubernetes endpoints."""
    if os.environ.get('SKYPILOT_API_REQUEST_BACKEND') == 'postgres':
        try:
            # Runtime import keeps the SQLite/local API import path light.
            # pylint: disable=import-outside-toplevel
            from sky.server.requests import postgres as request_postgres
            if not request_postgres.current_instance_is_ready():
                raise RuntimeError('role heartbeat is not ready')
        except Exception as e:
            raise fastapi.HTTPException(
                status_code=503,
                detail=f'API role is not ready: '
                f'{common_utils.format_exception(e)}') from e
    return {'status': 'ready'}


@app.get(
    '/api/health',
    # response_model_exclude_unset omits unset fields
    # in the response JSON.
    response_model_exclude_unset=True)
async def health(request: fastapi.Request) -> responses.APIHealthResponse:
    """Checks the health of the API server.

    Returns:
        responses.APIHealthResponse: The health response.
    """
    user = request.state.auth_user
    is_anonymous = getattr(request.state, 'anonymous_user', False)
    server_status = common.ApiServerStatus.HEALTHY
    if is_anonymous:
        # API server authentication is enabled, but the request is not
        # authenticated. We still have to serve the request because the
        # /api/health endpoint has two different usage:
        # 1. For health check from `api start` and external ochestration
        #    tools (k8s), which does not require authentication and user info.
        # 2. Return server info to client and hint client to login if required.
        # Separating these two usage to different APIs will break backward
        # compatibility for existing ochestration solutions (e.g. helm chart).
        # So we serve these two usages in a backward compatible manner below.
        client_version = versions.get_remote_api_version()
        # - For Client with API version >= 14, we return 200 response with
        #   status=NEEDS_AUTH, new client will handle the login process.
        # - For health check from `sky api start`, the client code always uses
        #   the same API version with the server, thus there is no compatibility
        #   issue.
        server_status = common.ApiServerStatus.NEEDS_AUTH
        if client_version is None:
            # - For health check from ochestration tools (e.g. k8s), we also
            #   return 200 with status=NEEDS_AUTH, which passes HTTP probe
            #   check.
            # - There is no harm when an malicious client calls /api/health
            #   without authentication since no sensitive information is
            #   returned.
            return responses.APIHealthResponse(
                status=common.ApiServerStatus.HEALTHY,)
        # TODO(aylei): remove this after min_compatible_api_version >= 14.
        if client_version < 14:
            # For Client with API version < 14, the NEEDS_AUTH status is not
            # honored. Return 401 to trigger the login process.
            raise fastapi.HTTPException(status_code=401,
                                        detail='Authentication required')

    logger.debug(f'Health endpoint: request.state.auth_user = {user}')

    # Get latest version from cache (returns None for dev versions
    # or if not available)
    latest_version = version_check.get_latest_version_for_current()
    release_metadata: dict[str, str] = {}
    if not is_anonymous:
        if sky.__commit_timestamp__ is not None:
            release_metadata['commit_timestamp'] = sky.__commit_timestamp__
        release_metadata['deployment_timestamp'] = _SERVER_STARTED_AT

    return responses.APIHealthResponse(
        status=server_status,
        # Kept for backward compatibility, clients before 0.11.0 will read this
        # field to check compatibility and hint the user to upgrade the CLI.
        # TODO(aylei): remove this field after 0.13.0
        api_version=str(server_constants.API_VERSION),
        version=sky.__version__,
        version_on_disk=common.get_skypilot_version_on_disk(),
        commit=sky.__commit__,
        # Build number that auto-increments with every commit.
        build=sky.__build__,
        # Whether basic auth on api server is enabled
        basic_auth_enabled=os.environ.get(constants.ENV_VAR_ENABLE_BASIC_AUTH,
                                          'false').lower() == 'true',
        user=user if user is not None else None,
        # Whether service account token is enabled
        service_account_token_enabled=(os.environ.get(
            constants.ENV_VAR_ENABLE_SERVICE_ACCOUNTS,
            'false').lower() == 'true'),
        # Whether basic auth on ingress is enabled
        ingress_basic_auth_enabled=os.environ.get(
            constants.SKYPILOT_INGRESS_BASIC_AUTH_ENABLED,
            'false').lower() == 'true',
        # Whether external proxy auth is enabled (from server.yaml config)
        external_proxy_auth_enabled=server_config.load_external_proxy_config().
        enabled,
        **release_metadata,
        # Latest version info (if available and newer than current)
        latest_version=latest_version,
        # Whether telemetry/usage collection is enabled
        telemetry_enabled=not env_options.Options.DISABLE_LOGGING.get(),
    )


# These aliases preserve the historical sky.server.server import surface while
# provider-specific cluster SSH transport lives behind its own router.
# pylint: disable=protected-access
SSHMessageType = websocket_utils.SSHMessageType
_get_cluster_and_validate = ssh_proxy._get_cluster_and_validate
kubernetes_pod_ssh_proxy = ssh_proxy.kubernetes_pod_ssh_proxy
slurm_job_ssh_proxy = ssh_proxy.slurm_job_ssh_proxy

# Preserve module and pickle identities for historical imports.
for _ssh_proxy_symbol in (
        _get_cluster_and_validate,
        kubernetes_pod_ssh_proxy,
        slurm_job_ssh_proxy,
):
    _ssh_proxy_symbol.__module__ = __name__
# pylint: enable=protected-access

app.include_router(ssh_proxy.router)


@app.websocket('/ssh-interactive-auth')
async def ssh_interactive_auth(websocket: fastapi.WebSocket,
                               session_id: str) -> None:
    """Proxies PTY for SSH interactive authentication via websocket.

    This endpoint receives a PTY file descriptor from a worker process
    and bridges it bidirectionally with a websocket connection, allowing
    the client to handle interactive SSH authentication (e.g., 2FA).

    Detects auth completion by monitoring terminal echo state and data flow.
    """
    await websocket.accept()
    logger.info(f'WebSocket connection accepted for SSH auth session: '
                f'{session_id}')

    loop = asyncio.get_running_loop()

    # Connect to worker process to receive PTY file descriptor
    fd_socket_path = interactive_utils.get_pty_socket_path(session_id)
    fd_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    master_fd = -1
    try:
        # Connect to worker's FD-passing socket
        await loop.sock_connect(fd_sock, fd_socket_path)
        master_fd = await loop.run_in_executor(None, interactive_utils.recv_fd,
                                               fd_sock)
        logger.debug(f'Received PTY master fd {master_fd} for session '
                     f'{session_id}')

        # PTYs are not portable asyncio pipes under uvloop. Keep their blocking
        # I/O in worker threads, but poll with a stop signal so every operation
        # is bounded and can be joined before the descriptor is closed.
        stop_pty_io = threading.Event()
        pty_io_futures = set()

        def read_from_pty():
            with selectors.DefaultSelector() as selector:
                selector.register(master_fd, selectors.EVENT_READ)
                while not stop_pty_io.is_set():
                    if selector.select(timeout=0.1):
                        return os.read(master_fd, 4096)
            return None

        def write_to_pty(data: bytes) -> None:
            pending = memoryview(data)
            with selectors.DefaultSelector() as selector:
                selector.register(master_fd, selectors.EVENT_WRITE)
                while pending and not stop_pty_io.is_set():
                    if not selector.select(timeout=0.1):
                        continue
                    written = os.write(master_fd, pending[:4096])
                    if written == 0:
                        raise OSError('PTY write returned zero bytes')
                    pending = pending[written:]

        async def run_pty_io(func, *args):
            future = loop.run_in_executor(None, func, *args)
            pty_io_futures.add(future)
            cancelled = False
            try:
                # Preserve the underlying operation when its forwarding task
                # is cancelled so the handler can join it before closing the
                # shared PTY descriptor.
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                cancelled = True
                raise
            finally:
                if not cancelled:
                    pty_io_futures.discard(future)

        # Bridge PTY ↔ websocket bidirectionally
        async def websocket_to_pty():
            """Forward websocket messages to PTY."""
            try:
                async for message in websocket.iter_bytes():
                    await run_pty_io(write_to_pty, message)
            except fastapi.WebSocketDisconnect:
                logger.debug(f'WebSocket disconnected for session {session_id}')
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Error in websocket_to_pty: {e}')

        async def pty_to_websocket():
            """Forward PTY output to websocket and detect auth completion.

            Detects auth completion by monitoring terminal echo state.
            Echo is disabled during password prompts and enabled after
            successful authentication. Auth is considered complete when
            echo has been enabled for a sustained period (1s).
            """
            try:
                while True:
                    data = b''
                    try:
                        data = await run_pty_io(read_from_pty)
                    except OSError as e:
                        logger.error(f'PTY read error (likely closed): {e}')
                        break

                    if data is None:
                        break
                    if not data:
                        break

                    await websocket.send_bytes(data)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Error in pty_to_websocket: {e}')
            finally:
                try:
                    await websocket.close()
                except Exception:  # pylint: disable=broad-except
                    pass

        proxy_tasks = (asyncio.create_task(websocket_to_pty()),
                       asyncio.create_task(pty_to_websocket()))
        try:
            done, _ = await asyncio.wait(proxy_tasks,
                                         return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            # Signal worker I/O before the first cleanup await so even repeated
            # parent cancellation cannot leave an executor thread blocked.
            stop_pty_io.set()
            for task in proxy_tasks:
                task.cancel()
            await asyncio.gather(*proxy_tasks, return_exceptions=True)
            if pty_io_futures:
                await asyncio.gather(*tuple(pty_io_futures),
                                     return_exceptions=True)

    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Error in SSH interactive auth websocket: {e}')
        raise
    finally:
        # Clean up
        if master_fd >= 0:
            try:
                os.close(master_fd)
            except OSError:
                pass
        fd_sock.close()
        logger.debug(f'SSH interactive auth session {session_id} completed')


@app.get('/all_contexts')
async def all_contexts(request: fastapi.Request) -> None:
    """Gets all Kubernetes and SSH node pool contexts."""

    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.ALL_CONTEXTS,
        request_body=payloads.RequestBody(),
        func=core.get_all_contexts,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@app.post('/debug/dump_create')
async def create_debug_dump(
        request: fastapi.Request,
        create_debug_dump_body: payloads.CreateDebugDumpBody) -> None:
    """Starts a debug dump."""

    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.CREATE_DEBUG_DUMP,
        request_body=create_debug_dump_body,
        func=core.create_debug_dump,
        schedule_type=requests_lib.ScheduleType.LONG,
        auth_user=request.state.auth_user,
    )


def _resolve_debug_dump_path(dump_filename: str) -> pathlib.Path:
    """Resolve and validate a requested debug dump path."""
    dump_dir = pathlib.Path(debug_utils.DEBUG_DUMP_DIR).expanduser()
    dump_path = dump_dir / dump_filename

    # Security: check path traversal before existence to avoid
    # leaking whether arbitrary files exist on the filesystem.
    try:
        dump_path.resolve().relative_to(dump_dir.resolve())
    except ValueError as path_err:
        raise fastapi.HTTPException(status_code=403,
                                    detail='Invalid path') from path_err

    if not dump_path.exists():
        raise fastapi.HTTPException(status_code=404,
                                    detail='Debug dump not found')
    return dump_path


@app.get('/debug/dump_download/{dump_filename}')
async def download_debug_dump(
        dump_filename: str) -> fastapi.responses.FileResponse:
    """Download a debug dump file.

    The dump file is automatically deleted after the download completes.
    """
    dump_path = await asyncio.to_thread(_resolve_debug_dump_path, dump_filename)

    # Delete the dump file after download completes
    return fastapi.responses.FileResponse(
        path=dump_path,
        filename=dump_filename,
        media_type='application/zip',
        background=starlette.background.BackgroundTask(dump_path.unlink,
                                                       missing_ok=True),
    )


# === Internal APIs ===
@app.get('/api/completion/cluster_name')
async def complete_cluster_name(incomplete: str,) -> list[str]:
    return await asyncio.to_thread(
        global_user_state.get_cluster_names_start_with, incomplete)


@app.get('/api/completion/storage_name')
async def complete_storage_name(incomplete: str,) -> list[str]:
    return await asyncio.to_thread(
        global_user_state.get_storage_names_start_with, incomplete)


@app.get('/api/completion/volume_name')
async def complete_volume_name(incomplete: str,) -> list[str]:
    return await asyncio.to_thread(
        global_user_state.get_volume_names_start_with, incomplete)


@app.get('/api/completion/api_request')
async def complete_api_request(incomplete: str,) -> list[str]:
    return await requests_lib.get_api_request_ids_start_with(incomplete)


app.include_router(dashboard_app.router)


def _init_or_restore_server_user_hash():
    """Compatibility facade for the shared role bootstrap helper."""
    # pylint: disable=import-outside-toplevel
    from sky.server import runtime as runtime_lib
    runtime_lib.init_or_restore_server_user_hash()


if __name__ == '__main__':
    # Imported only for executable entrypoints, preserving this module's
    # historical FastAPI and pickle facade for ordinary imports.
    # pylint: disable=import-outside-toplevel
    from sky.server import runtime as runtime_entrypoint
    runtime_entrypoint.main()
