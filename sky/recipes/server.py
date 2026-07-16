"""REST API for Recipe Hub.

This module provides the FastAPI router for recipe management,
including CRUD operations, pinning, and deployment.
"""
import fastapi

from sky.recipes import core
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as api_requests

router = fastapi.APIRouter()


@router.get('')
async def list_recipes(request: fastapi.Request) -> None:
    """List recipes (pinned + user's own)."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_LIST,
        request_body=payloads.RecipeListBody(),
        func=core.list_recipes,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/list')
async def list_recipes_with_filters(
    request: fastapi.Request,
    list_body: payloads.RecipeListBody,
) -> None:
    """List recipes with filters."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_LIST,
        request_body=list_body,
        func=core.list_recipes,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/get')
async def get_recipe(
    request: fastapi.Request,
    get_body: payloads.RecipeGetBody,
) -> None:
    """Get a single recipe by name."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_GET,
        request_body=get_body,
        func=core.get_recipe,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/create')
async def create_recipe(
    request: fastapi.Request,
    create_body: payloads.RecipeCreateBody,
) -> None:
    """Create a new recipe."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_CREATE,
        request_body=create_body,
        func=core.create_recipe,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/update')
async def update_recipe(
    request: fastapi.Request,
    update_body: payloads.RecipeUpdateBody,
) -> None:
    """Update an existing recipe."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_UPDATE,
        request_body=update_body,
        func=core.update_recipe,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/delete')
async def delete_recipe(
    request: fastapi.Request,
    delete_body: payloads.RecipeDeleteBody,
) -> None:
    """Delete a recipe."""
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_DELETE,
        request_body=delete_body,
        func=core.delete_recipe,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/pin')
async def pin_recipe(
    request: fastapi.Request,
    pin_body: payloads.RecipePinBody,
) -> None:
    """Toggle pin status of a recipe (admin only)."""
    # Note: Admin check should be performed in the core function or
    # via middleware. For now, the API is available to all authenticated users.
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.RECIPE_PIN,
        request_body=pin_body,
        func=core.toggle_pin,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )
