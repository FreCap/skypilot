"""REST API for the managed container image catalog."""

import fastapi

from sky import exceptions
from sky.container_images import core
from sky.container_images import models
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.workspaces import core as workspaces_core

router = fastapi.APIRouter()


def _resolve_workspace(request: fastapi.Request, requested: str | None) -> str:
    try:
        if requested is not None:
            requested = models.validate_workspace_name(
                requested, 'Container image workspace')
        resolution = workspaces_core.resolve_workspace_for_user(
            request.state.auth_user, requested)
        return models.validate_workspace_name(
            resolution.workspace, 'Resolved container image workspace')
    except exceptions.PermissionDeniedError:
        raise fastapi.HTTPException(
            status_code=403,
            detail='Container image workspace access denied.') from None
    except (exceptions.WorkspaceAmbiguousError, ValueError):
        raise fastapi.HTTPException(
            status_code=422,
            detail='Invalid container image workspace.') from None


@router.get('')
async def image_status(request: fastapi.Request,
                       image: str | None = None,
                       workspace: str | None = None) -> None:
    """Lists logical images and verified preparation status."""
    resolved_workspace = _resolve_workspace(request, workspace)
    try:
        body = payloads.ImageStatusBody(image=image,
                                        workspace=resolved_workspace)
    except ValueError as error:
        # This model is constructed from query parameters inside the endpoint,
        # outside FastAPI's RequestValidationError path. Never reflect the raw
        # selector through the debug exception response.
        raise fastapi.HTTPException(
            status_code=422,
            detail='Invalid container image request.') from error
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.IMAGE_STATUS,
        request_body=body,
        func=core.status,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


async def _schedule_image_publish(
        request: fastapi.Request, body: payloads.ImagePublishBody,
        request_name: request_names.RequestName) -> None:
    body.workspace = _resolve_workspace(request, body.workspace)
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_name,
        request_body=body,
        func=core.publish,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/publish')
async def image_publish(request: fastapi.Request,
                        body: payloads.ImagePublishBody) -> None:
    """Publishes a digest-pinned source in the workspace catalog."""
    await _schedule_image_publish(request, body,
                                  request_names.RequestName.IMAGE_PUBLISH)


@router.post('/register', include_in_schema=False)
async def image_register(request: fastapi.Request,
                         body: payloads.ImagePublishBody) -> None:
    """Compatibility route preserving the original dispatcher name."""
    await _schedule_image_publish(request, body,
                                  request_names.RequestName.IMAGE_REGISTER)


@router.post('/prepare')
async def image_prepare(request: fastapi.Request,
                        body: payloads.ImagePrepareBody) -> None:
    """Creates durable preparation intents for explicit targets."""
    body.workspace = _resolve_workspace(request, body.workspace)
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.IMAGE_PREPARE,
        request_body=body,
        func=core.prepare,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/retry')
async def image_retry(request: fastapi.Request,
                      body: payloads.ImageRetryBody) -> None:
    """Retries one failed or missing preparation target."""
    body.workspace = _resolve_workspace(request, body.workspace)
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.IMAGE_RETRY,
        request_body=body,
        func=core.retry,
        schedule_type=requests_lib.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )
