"""Tests for the Recipes API routes."""

from collections.abc import Awaitable
from collections.abc import Callable
from unittest import mock

import pytest

from sky import models
from sky.recipes import server
from sky.server.requests import payloads


def _request() -> mock.MagicMock:
    request = mock.MagicMock()
    request.state.request_id = 'request-id'
    request.state.auth_user = models.User(id='user-id', name='User Name')
    return request


@pytest.mark.asyncio
async def test_list_recipes_uses_authenticated_user() -> None:
    request = _request()

    with mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        await server.list_recipes(request)

    schedule.assert_awaited_once()
    kwargs = schedule.await_args.kwargs
    assert isinstance(kwargs['request_body'], payloads.RecipeListBody)
    assert kwargs['auth_user'] is request.state.auth_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('endpoint', 'body'),
    [
        (server.list_recipes_with_filters, payloads.RecipeListBody()),
        (server.get_recipe, payloads.RecipeGetBody(recipe_name='recipe')),
        (server.create_recipe,
         payloads.RecipeCreateBody(
             name='recipe', content='resources: {}', recipe_type='cluster')),
        (server.update_recipe,
         payloads.RecipeUpdateBody(recipe_name='recipe',
                                   content='resources: {}')),
        (server.delete_recipe, payloads.RecipeDeleteBody(recipe_name='recipe')),
        (server.pin_recipe,
         payloads.RecipePinBody(recipe_name='recipe', pinned=True)),
    ],
)
async def test_recipe_routes_use_authenticated_user(
    endpoint: Callable[..., Awaitable[None]],
    body: payloads.RequestBody,
) -> None:
    request = _request()

    with mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        await endpoint(request, body)

    schedule.assert_awaited_once()
    kwargs = schedule.await_args.kwargs
    assert kwargs['request_body'] is body
    assert kwargs['auth_user'] is request.state.auth_user
