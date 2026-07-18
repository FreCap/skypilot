"""Per-user workspace resolution policy."""

from dataclasses import dataclass

from sky import exceptions
from sky import models
from sky.skylet import constants
from sky.workspaces import constants as workspace_constants


@dataclass
class WorkspaceResolution:
    """Outcome of resolving a user's effective workspace.

    workspace: the resolved workspace name to use.
    source: where it came from, one of ``WORKSPACE_SOURCE_*``.
    note: optional explanation when a stored preference is no longer usable.
    """
    workspace: str
    source: str
    note: str | None = None


# Keep existing serialized identities and imports stable through the core
# facade even though the implementation now lives in this module.
WorkspaceResolution.__module__ = 'sky.workspaces.core'


def resolve_automatic_workspace(user: models.User,
                                accessible: list[str]) -> WorkspaceResolution:
    """Resolve a workspace when the caller did not explicitly request one.

    The caller owns workspace loading and permission checks. This function
    owns only the deterministic precedence policy.
    """
    # Read preferred from the User dataclass: it is populated by
    # global_user_state.add_or_update_user(return_user=True), which the
    # executor already calls upstream for every request. Re-querying the
    # users table per request would be redundant on the hot path.
    preferred = user.preferred_workspace
    if preferred is not None and preferred in accessible:
        return WorkspaceResolution(
            preferred, workspace_constants.WORKSPACE_SOURCE_PREFERRED)

    drift_note: str | None = None
    if preferred is not None and preferred not in accessible:
        # The preference was set in the past but the user no longer has
        # access (RBAC drift). Surface this in the source note so users
        # understand why their preference wasn't honored.
        drift_note = f'preferred {preferred!r} not accessible'

    if constants.SKYPILOT_DEFAULT_WORKSPACE in accessible:
        # Default-fallback: don't break legacy multi-workspace users and
        # admins who used to land on 'default' implicitly.
        return WorkspaceResolution(
            constants.SKYPILOT_DEFAULT_WORKSPACE,
            workspace_constants.WORKSPACE_SOURCE_DEFAULT_FALLBACK, drift_note)

    if len(accessible) == 1:
        return WorkspaceResolution(
            accessible[0],
            workspace_constants.WORKSPACE_SOURCE_SINGLE_MEMBERSHIP, drift_note)

    if not accessible:
        raise exceptions.NoWorkspaceAccessError(
            f'User {user.name} ({user.id}) has no accessible workspaces.')

    raise exceptions.WorkspaceAmbiguousError(accessible, note=drift_note)
