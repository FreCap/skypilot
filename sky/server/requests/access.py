"""Authorization scope for persisted API requests."""

from __future__ import annotations

from collections.abc import Collection
import dataclasses

from sky import models
from sky.users import rbac


@dataclasses.dataclass(frozen=True)
class RequestAccessScope:
    """The persisted-request rows and operations available to one caller.

    ``owner_user_id=None`` is reserved for trusted principal-free requests
    (authentication-disabled local servers and trusted loopback callers) and
    authenticated admins.  A controller-generation header is a fencing signal
    and deliberately does not widen this authorization scope.
    """

    owner_user_id: str | None
    can_cancel: bool

    @property
    def can_access_all_users(self) -> bool:
        return self.owner_user_id is None

    @property
    def can_stream_arbitrary_log_path(self) -> bool:
        return self.can_access_all_users


def resolve_request_access(
    auth_user: models.User | None,
    roles: Collection[str],
) -> RequestAccessScope:
    """Resolve request access from the authenticated principal and roles."""
    if auth_user is None:
        # Principal-free local/loopback requests have no user identity with
        # which to partition their historical single-user request store.
        return RequestAccessScope(owner_user_id=None, can_cancel=True)

    role_set = frozenset(roles)
    if rbac.RoleName.ADMIN.value in role_set:
        return RequestAccessScope(owner_user_id=None, can_cancel=True)

    is_viewer = rbac.RoleName.VIEWER.value in role_set
    return RequestAccessScope(owner_user_id=auth_user.id,
                              can_cancel=not is_viewer)
