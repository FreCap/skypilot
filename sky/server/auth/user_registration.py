"""User registration shared by API server authentication middlewares."""

import asyncio

from sky import global_user_state
from sky import models
from sky.users import permission


def _add_or_update_user_with_default_role(user: models.User) -> None:
    """Persist a user and finish any required default-role assignment."""
    newly_added = global_user_state.add_or_update_user(user)
    if newly_added:
        permission.permission_service.add_user_if_not_exists(user.id)


async def add_or_update_user_with_default_role(user: models.User) -> None:
    """Register a user without allowing cancellation to split its writes."""
    await asyncio.to_thread(_add_or_update_user_with_default_role, user)
