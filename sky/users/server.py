"""REST API for workspace management."""

from collections.abc import Generator
import contextlib
import hashlib
import os
from typing import Any

import fastapi
import filelock
import sqlalchemy

from sky import exceptions
from sky import global_user_state
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.serve import serve_state
from sky.server import common as server_common
from sky.server.requests import payloads
from sky.skylet import constants
from sky.users import permission
from sky.users import rbac
from sky.users import resolver as user_resolver
from sky.users import service_accounts
from sky.utils import common
from sky.utils import common_utils
from sky.utils import context
from sky.utils import resource_checker
from sky.workspaces import constants as workspace_constants
from sky.workspaces import core as workspaces_core

logger = sky_logging.init_logger(__name__)

# Filelocks for the user management.
USER_LOCK_PATH = os.path.expanduser('~/.sky/.{user_id}.lock')
USER_LOCK_TIMEOUT_SECONDS = 20

# Built-in user identities that the server seeds at startup. They must not
# be deletable or have their role changed via the user-management API.
_INTERNAL_USER_IDS = (
    common.SERVER_ID,
    constants.SKYPILOT_SYSTEM_USER_ID,
    constants.SKYPILOT_SERVE_CONTROLLER_SYSTEM_USER_ID,
    constants.SKYPILOT_SYSTEM_VIEWER_USER_ID,
)

router = fastapi.APIRouter()


def get_user_type(user: models.User) -> str:
    """Get user type for a user for backward compatibility.

    Args:
        user: The user to get the type for.

    Returns:
        The user type string.
    """
    if user.is_service_account():
        return models.UserType.SA.value
    if user.id in _INTERNAL_USER_IDS:
        return models.UserType.SYSTEM.value
    if user.password is not None:
        return models.UserType.BASIC.value
    if user.name and '@' in user.name:
        return models.UserType.SSO.value
    return models.UserType.LEGACY.value


# All handlers in user handler are sync to get fastAPI run it in a
# ThreadPoolExecutor to avoid blocking the async event loop.
# TODO(aylei): make these async once we have the global_user_state async
# support.
@router.get('')
def users() -> list[dict[str, Any]]:
    """Gets all users."""
    all_users = []
    user_list = global_user_state.get_all_users()

    users_to_role = {}
    for role in rbac.get_supported_roles():
        user_ids = permission.permission_service.get_users_for_role(role)
        for user_id in user_ids:
            users_to_role[user_id] = role

    for user in user_list:
        # Filter out service accounts - they have IDs starting with "sa-"
        if user.is_service_account():
            continue

        user_type = user.user_type or get_user_type(user)
        all_users.append({
            'id': user.id,
            'name': user.name,
            'created_at': user.created_at,
            'role': users_to_role.get(user.id, ''),
            'user_type': user_type,
        })
    return all_users


@router.get('/role')
def get_current_user_role(request: fastapi.Request):
    """Get current user's role."""
    # TODO(hailong): is there a reliable way to get the user
    # hash for the request without 'X-Auth-Request-Email' header?
    auth_user = request.state.auth_user
    if auth_user is None:
        return {'id': '', 'name': '', 'role': rbac.RoleName.ADMIN.value}
    user_roles = permission.permission_service.get_user_roles(auth_user.id)
    return {
        'id': auth_user.id,
        'name': auth_user.name,
        'role': user_roles[0] if user_roles else ''
    }


@router.post('/me/workspace')
def set_user_preferred_workspace(
    request: fastapi.Request,
    body: payloads.UserPreferredWorkspaceBody,
) -> dict[str, Any]:
    """Sets (or clears with `preferred: null`) the user's preferred workspace.

    Echoes the new preferred value on success. Callers that need the
    resolved workspace + accessible list should follow up with
    ``GET /users/me/workspace``.

    RBAC: rejects setting a workspace the user does not have access to.
    """
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Not authenticated.')
    try:
        workspaces_core.set_user_preferred_workspace(auth_user, body.preferred)
    except exceptions.PermissionDeniedError as e:
        raise fastapi.HTTPException(status_code=403, detail=str(e)) from e
    except ValueError as e:
        # Workspace does not exist.
        raise fastapi.HTTPException(status_code=404, detail=str(e)) from e
    return {'preferred': body.preferred}


@router.get('/me/workspace')
@context.contextual
def get_user_workspace(
    request: fastapi.Request,
    requested: str | None = None,
) -> dict[str, Any]:
    """Returns workspace state for the calling user.

    One stop for everything ``sky workspace info`` / dashboard pages /
    ``_show_enabled_infra`` need:

    * ``workspace``: the workspace the launch path would pick for this
      user RIGHT NOW. Mirrors the launch-path precedence — an explicit
      ``active_workspace`` (set client-side in ``.sky.yaml`` and passed
      here via ``?requested=``, or set in the server's own loaded
      config) wins; otherwise the resolver runs preferred /
      default-fallback / single-membership.
    * ``source``: one of ``WORKSPACE_SOURCE_*``. Tells the UI / CLI why
      the resolver landed where it did (``explicit`` when the active
      override won, otherwise ``preferred`` / ``default-fallback`` /
      ``single-membership``). Drives the optional ``note``.
    * ``note``: free-form message — drift on success
      (``preferred 'team-x' not accessible``), or the error message
      when the resolver couldn't pick a workspace
      (``WorkspaceAmbiguousError``, ``NoWorkspaceAccessError``, or a
      ``PermissionDeniedError`` against an explicit ``requested``). In
      those error cases ``workspace`` is ``None`` and ``source`` is
      ``None``; the caller should render ``note`` as guidance instead
      of treating the request as a server fault.
    * ``preferred``: the persisted preferred workspace (``None`` if the
      user has not set one).
    * ``accessible``: sorted list of every workspace the user can launch
      into. Same set ``/workspaces`` returns the config for, but just
      the names.

    Args:
        request: FastAPI request — the auth middleware must have
            populated ``request.state.auth_user``.
        requested: explicit active workspace from the caller. Mirrors
            the resolver's precedence-1 slot. The client SDK reads its
            local ``active_workspace`` (if any) and stamps it here, so
            the answer reflects what would actually land at launch.

    The handler is synchronous (no executor.schedule_request_async) —
    no request body, the resolver is pure-Python, and dashboard pages
    poll this frequently enough that latency matters.
    """
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Not authenticated.')
    # Sync FastAPI handler — see comment in user_update for why we have
    # to set the per-request user context here ourselves. Without this,
    # `workspaces_core.get_accessible_workspace_names()` (which calls
    # `common_utils.get_current_user()`) would fall back to the API-
    # server process's own identity and return the wrong user's
    # accessible set.
    common_utils.set_current_user(auth_user)
    # Same reason: refresh process-cached workspace config +
    # request-scoped lru cache so admin `workspace create/update`
    # ops that ran on a worker process are visible here.
    server_common.refresh_workspace_state_for_sync_handler()
    # The auth middleware does not populate `preferred_workspace` on
    # `auth_user` (only id/name/type travel via the request context); the
    # resolver reads it off the User dataclass directly. Re-fetch the user
    # row so this handler matches what worker-side `add_or_update_user(
    # return_user=True)` would supply.
    fresh_user = global_user_state.get_user(auth_user.id)
    user_for_resolve = fresh_user if fresh_user is not None else auth_user
    # Mirror launch-path precedence: an explicit `active_workspace` — set
    # by the client and shipped here as `?requested=`, or
    # set in the server's own loaded config — beats the resolver's 2-6
    # path. For queued requests the executor reads the merged thread-
    # local (client-overlay + server base), but this synchronous GET has
    # no request body, so the SDK stamps `?requested=` explicitly. We
    # still check the server-side config as a fallback so an admin who
    # set `active_workspace` globally gets honored.
    if requested is None and skypilot_config.is_active_workspace_set():
        requested = skypilot_config.get_active_workspace()
    accessible = sorted(workspaces_core.get_accessible_workspace_names())
    response: dict[str, Any] = {
        'workspace': None,
        'source': None,
        'note': None,
        'preferred': user_for_resolve.preferred_workspace,
        'accessible': accessible,
    }
    try:
        resolution = workspaces_core.resolve_workspace_for_user(
            user_for_resolve, requested=requested)
    except exceptions.WorkspaceAmbiguousError as e:
        # Per-user state, not a server fault — return 200 with a state-
        # coded `source` and a SHORT `note`. The CLI / dashboard show
        # the long recovery guidance separately (see
        # `WorkspaceAmbiguousError.recovery_hint`) so the structured
        # payload (`workspace` / `source` / `note` / `preferred` /
        # `accessible`) stays clean and grep-able.
        response['source'] = workspace_constants.WORKSPACE_SOURCE_AMBIGUOUS
        # `e.note` only carries drift context ("preferred 'X' not
        # accessible"); fall back to a generic one-liner otherwise.
        response['note'] = (e.note
                            if e.note else 'multiple workspaces accessible; '
                            'no preferred or active workspace set')
        return response
    except exceptions.NoWorkspaceAccessError as e:
        # One-line message from the raise site ("User <name> (<id>) has
        # no accessible workspaces.") — short enough to fit in the tree
        # row and more informative than a generic stand-in.
        response['source'] = workspace_constants.WORKSPACE_SOURCE_NO_ACCESS
        response['note'] = str(e)
        return response
    except exceptions.PermissionDeniedError as e:
        # Per-workspace deny — raised when an explicit `requested`
        # workspace exists but the user can't access it. We keep the
        # exception message here because it names the specific
        # workspace and the reason (RBAC / not-in-allowed-users),
        # which the payload alone wouldn't convey.
        response['source'] = (
            workspace_constants.WORKSPACE_SOURCE_PERMISSION_DENIED)
        response['note'] = str(e)
        return response
    response['workspace'] = resolution.workspace
    response['source'] = resolution.source
    response['note'] = resolution.note
    return response


@router.post('/create')
def user_create(user_create_body: payloads.UserCreateBody) -> None:
    username = user_create_body.username
    password = user_create_body.password
    role = user_create_body.role

    if not username or not password:
        raise fastapi.HTTPException(status_code=400,
                                    detail='Username and password are required')
    if role and role not in rbac.get_supported_roles():
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Invalid role: {role}')

    if not role:
        role = rbac.get_default_role()

    # Create user
    password_hash = server_common.crypt_ctx.hash(password)
    # MD5 here only derives a stable user identifier from the (non-secret)
    # username; it is not used for any security purpose (passwords use bcrypt
    # via crypt_ctx).
    user_hash = hashlib.md5(
        username.encode(),
        usedforsecurity=False).hexdigest()[:common_utils.USER_HASH_LENGTH]
    with _user_lock(user_hash):
        # Check if user already exists
        if global_user_state.get_user_by_name(username):
            raise fastapi.HTTPException(
                status_code=400, detail=f'User {username!r} already exists')
        global_user_state.add_or_update_user(
            models.User(id=user_hash,
                        name=username,
                        password=password_hash,
                        user_type=models.UserType.BASIC.value))
        permission.permission_service.update_role(user_hash, role)


@router.post('/update')
@context.contextual
def user_update(request: fastapi.Request,
                user_update_body: payloads.UserUpdateBody) -> None:
    """Updates the user role."""
    user_id = user_update_body.user_id
    role = user_update_body.role
    password = user_update_body.password
    supported_roles = rbac.get_supported_roles()
    if role and role not in supported_roles:
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Invalid role: {role}')
    target_user_roles = permission.permission_service.get_user_roles(user_id)
    need_update_role = role and (not target_user_roles or
                                 (role != target_user_roles[0]))
    current_user = request.state.auth_user
    if current_user is not None:
        # This is a sync FastAPI handler, so it doesn't go through the
        # executor's reload_for_new_request pipeline that normally
        # populates the per-request user context. Without this, downstream
        # calls (e.g. resource_checker -> queue_v2 ->
        # get_accessible_workspace_names) would fall back to the local
        # machine user and silently filter out anything in private
        # workspaces the local user can't see.
        common_utils.set_current_user(current_user)
        current_user_roles = permission.permission_service.get_user_roles(
            current_user.id)
        if not current_user_roles:
            raise fastapi.HTTPException(status_code=403, detail='Invalid user')
        if current_user_roles[0] != rbac.RoleName.ADMIN.value:
            if need_update_role:
                raise fastapi.HTTPException(
                    status_code=403, detail='Only admin can update user role')
            if password and user_id != current_user.id:
                raise fastapi.HTTPException(
                    status_code=403,
                    detail='Only admin can update password for other users')
    user_info = global_user_state.get_user(user_id)
    if user_info is None:
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'User {user_id} does not exist')
    # Disallow updating the internal users.
    if need_update_role and user_info.id in _INTERNAL_USER_IDS:
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Cannot update role for internal '
                                    f'API server user {user_info.name}')
    if password and user_info.id in _INTERNAL_USER_IDS:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Cannot update password for internal '
            f'API server user {user_info.name}')

    # When demoting from admin to a non-admin role, ensure the user has no
    # active resources in private workspaces they will lose access to.
    is_demotion = (need_update_role and target_user_roles and
                   target_user_roles[0] == rbac.RoleName.ADMIN.value and
                   role != rbac.RoleName.ADMIN.value)
    if is_demotion:
        try:
            resource_checker.check_user_role_demotion(user_info)
        except ValueError as e:
            raise fastapi.HTTPException(status_code=400, detail=str(e))

    with _user_lock(user_info.id):
        if password:
            password_hash = server_common.crypt_ctx.hash(password)
            global_user_state.add_or_update_user(
                models.User(id=user_info.id,
                            name=user_info.name,
                            password=password_hash))
        if role and need_update_role:
            # Update user role in casbin policy
            permission.permission_service.update_role(user_info.id, role)


@router.post('/batch_update')
@context.contextual
def user_batch_update(request: fastapi.Request,
                      body: payloads.UserBatchUpdateBody) -> dict[str, Any]:
    """Updates the role for a batch of users.

    Returns a per-user result with ``succeeded`` and ``failed`` lists so the
    caller can show partial failures (e.g. one user is blocked
    while others are not).
    """
    role = body.role
    user_ids = body.user_ids
    supported_roles = rbac.get_supported_roles()
    if not role or role not in supported_roles:
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Invalid role: {role}')
    if not user_ids:
        raise fastapi.HTTPException(status_code=400,
                                    detail='user_ids must not be empty')

    # Only admin can run a batch role update.
    current_user = request.state.auth_user
    if current_user is not None:
        # Sync FastAPI handler -- see comment in user_update for why we
        # have to set the per-request user context here ourselves.
        common_utils.set_current_user(current_user)
        current_user_roles = permission.permission_service.get_user_roles(
            current_user.id)
        if not current_user_roles:
            raise fastapi.HTTPException(status_code=403, detail='Invalid user')
        if current_user_roles[0] != rbac.RoleName.ADMIN.value:
            raise fastapi.HTTPException(
                status_code=403, detail='Only admin can update user roles')

    # Pre-fetch the per-user role state ONCE for the whole batch so the
    # per-user loop is O(1) dict lookups instead of N * casbin
    # get_user_roles.
    users_to_role: dict[str, str] = {}
    for supported_role in supported_roles:
        for uid in permission.permission_service.get_users_for_role(
                supported_role):
            users_to_role[uid] = supported_role

    batch_workspaces_allowed_users: dict[str, set[str]] | None = None
    if role == rbac.RoleName.ADMIN.value:
        # Promotion -> nobody needs the demotion check, so we only need
        # user info for the batch's user_ids (one targeted IN query,
        # avoiding a full-table scan that gets wasted on this path).
        all_users_map = global_user_state.get_users(set(user_ids))
        batch_workspaces = None
        batch_resources = None
    else:
        # Demotion -> we need the full user list anyway to detect
        # username-uniqueness across the whole system when resolving each
        # private workspace's ``allowed_users``. Build one shared
        # UserResolver and derive the per-user map from the same fetch
        # so we still do exactly one DB round-trip.
        resolver = user_resolver.UserResolver()
        all_users_map = resolver.id_to_user
        batch_workspaces = resource_checker.load_fresh_workspaces()
        batch_resources = resource_checker.ResourceSnapshot.fetch_all()
        # Pre-resolve each private workspace's allowed_users -> user_id
        # set ONCE for the batch. Without this, check_user_role_demotion
        # iterates private workspaces and calls get_workspace_users for
        # each (every call re-fetches get_all_users() from the DB),
        # giving N * P * get_all_users() round-trips in a batch.
        batch_workspaces_allowed_users = (
            resolver.resolve_workspaces_allowed_users(batch_workspaces))

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []

    for user_id in user_ids:
        try:
            user_info = all_users_map.get(user_id)
            if user_info is None:
                failed.append({
                    'user_id': user_id,
                    'error': f'User {user_id} does not exist'
                })
                continue
            if user_info.id in _INTERNAL_USER_IDS:
                failed.append({
                    'user_id': user_id,
                    'error': (f'Cannot update role for internal API server '
                              f'user {user_info.name}')
                })
                continue
            current_role = users_to_role.get(user_id)
            target_user_roles = [current_role] if current_role else []
            need_update_role = (not target_user_roles or
                                role != target_user_roles[0])
            if not need_update_role:
                # Already in the desired role; record as success no-op.
                succeeded.append(user_id)
                continue
            # When demoting from admin to a non-admin role (user / viewer),
            # ensure the user has no active resources in private workspaces
            # they will lose implicit access to. Reuse the per-batch
            # pre-fetched workspaces + active resources to keep this O(C+J)
            # for the whole batch instead of O(N * (C+J)).
            if (target_user_roles and
                    target_user_roles[0] == rbac.RoleName.ADMIN.value and
                    role != rbac.RoleName.ADMIN.value):
                resource_checker.check_user_role_demotion(
                    user_info,
                    workspaces=batch_workspaces,
                    resources=batch_resources,
                    workspaces_allowed_users=batch_workspaces_allowed_users)
            with _user_lock(user_info.id):
                permission.permission_service.update_role(user_info.id, role)
            succeeded.append(user_id)
        except ValueError as e:
            failed.append({'user_id': user_id, 'error': str(e)})
        except Exception as e:  # pylint: disable=broad-except
            logger.exception(f'Failed to update role for user {user_id}')
            failed.append({'user_id': user_id, 'error': str(e)})

    return {'succeeded': succeeded, 'failed': failed}


def _delete_user(user_id: str) -> None:
    """Delete a user."""
    user_info = global_user_state.get_user(user_id)
    if user_info is None:
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'User {user_id} does not exist')
    # Disallow deleting the internal users.
    if user_info.id in _INTERNAL_USER_IDS:
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Cannot delete internal '
                                    f'API server user {user_info.name}')

    if serve_state.service_owner_attestation_transition_active():
        raise fastapi.HTTPException(
            status_code=409,
            detail=('User deletion is temporarily unavailable while '
                    'SkyServe service-owner attestation is being completed.'))

    owned_services = serve_state.get_service_names_owned_by_user_id(user_id)
    if owned_services:
        preview = ', '.join(owned_services[:5])
        suffix = '' if len(owned_services) <= 5 else ', ...'
        raise fastapi.HTTPException(
            status_code=400,
            detail=(f'User {user_id} owns active SkyServe service(s): '
                    f'{preview}{suffix}. Tear them down before deleting the '
                    'user.'))

    # Check for active clusters and managed jobs owned by the user
    try:
        resource_checker.check_no_active_resources_for_users([(user_id,
                                                               'delete')])
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))

    with _user_lock(user_id):
        try:
            global_user_state.delete_user(user_id)
        except sqlalchemy.exc.IntegrityError as error:
            # The Serve055 foreign key closes the race with a service
            # creation or one-shot owner attestation after the read above.
            raise fastapi.HTTPException(
                status_code=400,
                detail=(f'User {user_id} acquired a durable resource owner '
                        'reference and cannot be deleted.')) from error
        permission.permission_service.delete_user(user_id)


@router.post('/delete')
@context.contextual
def user_delete(request: fastapi.Request,
                user_delete_body: payloads.UserDeleteBody) -> None:
    current_user = request.state.auth_user
    if current_user is not None:
        # Sync FastAPI handler -- see comment in user_update for why we
        # have to set the per-request user context here ourselves.
        common_utils.set_current_user(current_user)
    user_id = user_delete_body.user_id
    _delete_user(user_id)


@router.post('/import')
def user_import(user_import_body: payloads.UserImportBody) -> dict[str, Any]:
    """Import users from CSV content."""
    csv_content = user_import_body.csv_content

    if not csv_content:
        raise fastapi.HTTPException(status_code=400,
                                    detail='CSV content is required')

    # Parse CSV content
    lines = csv_content.strip().split('\n')
    if len(lines) < 2:
        raise fastapi.HTTPException(
            status_code=400,
            detail='CSV must have at least a header row and one data row')

    # Parse headers
    headers = [h.strip().lower() for h in lines[0].split(',')]
    required_headers = ['username', 'password', 'role']

    # Check if all required headers are present
    missing_headers = [
        header for header in required_headers if header not in headers
    ]
    if missing_headers:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Missing required columns: {", ".join(missing_headers)}')

    # Parse user data
    users_to_create = []
    parse_errors = []

    for i, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue  # Skip empty lines

        values = [v.strip() for v in line.split(',')]
        if len(values) != len(headers):
            parse_errors.append(f'Line {i}: Invalid number of columns')
            continue

        user_data = dict(zip(headers, values))

        # Validate required fields
        if not user_data.get('username') or not user_data.get('password'):
            parse_errors.append(f'Line {i}: Username and password are required')
            continue

        # Validate role
        role = user_data.get('role', '').lower()
        if role and role not in rbac.get_supported_roles():
            role = rbac.get_default_role()  # Default to default role if invalid
        elif not role:
            role = rbac.get_default_role()

        users_to_create.append({
            'username': user_data['username'],
            'password': user_data['password'],
            'role': role
        })

    if not users_to_create and parse_errors:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'No valid users found. Errors: {"; ".join(parse_errors)}')

    # Create users
    success_count = 0
    error_count = 0
    creation_errors = []

    for user_data in users_to_create:
        try:
            username = user_data['username']
            password = user_data['password']
            role = user_data['role']

            # Check if user already exists
            if global_user_state.get_user_by_name(username):
                error_count += 1
                creation_errors.append(f'{username}: User already exists')
                continue

            # Check if password is already hashed
            if server_common.crypt_ctx.identify(password) is not None:
                # Password is already hashed, use it directly
                password_hash = password
            else:
                # Password is plain text, hash it
                password_hash = server_common.crypt_ctx.hash(password)

            # MD5 only derives a stable user identifier from the (non-secret)
            # username; not a security use (passwords use bcrypt via crypt_ctx).
            user_hash = hashlib.md5(username.encode(),
                                    usedforsecurity=False).hexdigest()
            user_hash = user_hash[:common_utils.USER_HASH_LENGTH]

            with _user_lock(user_hash):
                global_user_state.add_or_update_user(
                    models.User(
                        id=user_hash,
                        name=username,
                        password=password_hash,
                        user_type=models.UserType.BASIC.value,
                    ))
                permission.permission_service.update_role(user_hash, role)

            success_count += 1

        except Exception as e:  # pylint: disable=broad-except
            error_count += 1
            creation_errors.append(f'{user_data["username"]}: {str(e)}')

    return {
        'success_count': success_count,
        'error_count': error_count,
        'total_processed': len(users_to_create),
        'parse_errors': parse_errors,
        'creation_errors': creation_errors
    }


@router.get('/export')
def user_export() -> dict[str, Any]:
    """Export all users as CSV content."""
    try:
        # Get all users
        user_list = global_user_state.get_all_users()

        # Create CSV content
        csv_lines = ['username,password,role']  # Header

        exported_users = []
        for user in user_list:
            # Filter out service accounts - they have IDs starting with "sa-"
            if user.is_service_account():
                continue

            # Get user role
            user_roles = permission.permission_service.get_user_roles(user.id)
            role = user_roles[0] if user_roles else rbac.get_default_role()
            # Avoid exporting `None` values
            line = ''
            if user.name:
                line += user.name
            line += ','
            if user.password:
                line += user.password
            line += ','
            if role:
                line += role
            csv_lines.append(line)
            exported_users.append(user)

        csv_content = '\n'.join(csv_lines)

        return {'csv_content': csv_content, 'user_count': len(exported_users)}

    except Exception as e:
        raise fastapi.HTTPException(status_code=500,
                                    detail=f'Failed to export users: {str(e)}')


@contextlib.contextmanager
def _user_lock(user_id: str) -> Generator[None, None, None]:
    """Context manager for user lock."""
    try:
        with filelock.FileLock(USER_LOCK_PATH.format(user_id=user_id),
                               USER_LOCK_TIMEOUT_SECONDS):
            yield
    except filelock.Timeout as e:
        raise RuntimeError(f'Failed to update user due to a timeout '
                           f'when trying to acquire the lock at '
                           f'{USER_LOCK_PATH.format(user_id=user_id)}. '
                           'Please try again or manually remove the lock '
                           f'file if you believe it is stale.') from e


@router.post('/service-account-tokens/delete')
def delete_service_account_token(
        request: fastapi.Request,
        token_body: payloads.ServiceAccountTokenDeleteBody) -> dict[str, str]:
    """Delete a service account token.

    Admins can delete any token, users can only delete their own.
    """
    return service_accounts.delete_service_account_token(
        request, token_body, delete_user=_delete_user)


# Keep the established users router and import path as the public facade.
router.include_router(service_accounts.router)
get_service_account_tokens = service_accounts.get_service_account_tokens
create_service_account_token = service_accounts.create_service_account_token
get_service_account_role = service_accounts.get_service_account_role
update_service_account_role = service_accounts.update_service_account_role
rotate_service_account_token = service_accounts.rotate_service_account_token
_generate_service_account_user_id = (
    service_accounts._generate_service_account_user_id)  # pylint: disable=protected-access
