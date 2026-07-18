"""REST API implementation for service account token management."""

from collections.abc import Callable
import re
import secrets
import time
from typing import Any

import fastapi

from sky import global_user_state
from sky import models
from sky import sky_logging
from sky.server.requests import payloads
from sky.skylet import constants
from sky.users import permission
from sky.users import token_service

logger = sky_logging.init_logger('sky.users.server')

router = fastapi.APIRouter()


# SkyPilot currently does not distinguish between service accounts and service
# account tokens, i.e. service accounts have a 1-1 mapping to service account
# tokens.
@router.get('/service-account-tokens')
def get_service_account_tokens(
        request: fastapi.Request) -> list[dict[str, Any]]:
    """Get service account tokens. All users can see all tokens."""
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Authentication required')

    # All authenticated users can see all tokens
    tokens = global_user_state.get_all_service_account_tokens()

    result = []
    for token in tokens:
        token_info = {
            'token_id': token['token_id'],
            'token_name': token['token_name'],
            'created_at': token['created_at'],
            'last_used_at': token['last_used_at'],
            'expires_at': token['expires_at'],
            'creator_user_hash': token['creator_user_hash'],
            'service_account_user_id': token['service_account_user_id'],
        }

        # Add creator display name
        creator_user = global_user_state.get_user(token['creator_user_hash'])
        token_info[
            'creator_name'] = creator_user.name if creator_user else 'Unknown'

        # Add service account name
        sa_user = global_user_state.get_user(token['service_account_user_id'])
        token_info['service_account_name'] = (sa_user.name if sa_user else
                                              token['token_name'])

        # Add service account roles
        roles = permission.permission_service.get_user_roles(
            token['service_account_user_id'])
        token_info['service_account_roles'] = roles

        result.append(token_info)

    return result


def _generate_service_account_user_id() -> str:
    """Generate a unique user ID for a service account."""
    random_suffix = secrets.token_hex(8)  # 16 character hex string
    service_account_id = (f'sa-{random_suffix}')
    return service_account_id


@router.post('/service-account-tokens')
def create_service_account_token(
        request: fastapi.Request,
        token_body: payloads.ServiceAccountTokenCreateBody) -> dict[str, Any]:
    """Create a new service account token."""
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Authentication required')

    token_name = token_body.token_name.strip()

    # Check if token follows a valid format
    if not re.match(constants.CLUSTER_NAME_VALID_REGEX, token_name):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Token name must contain only letters, numbers, and '
            'underscores. Please use a different name.')

    # Validate expiration (allow 0 as special value for "never expire")
    if (token_body.expires_in_days is not None and
            token_body.expires_in_days < 0):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Expiration days must be positive or 0 for never expire')

    try:
        # Generate a unique service account user ID
        service_account_user_id = _generate_service_account_user_id()

        # Create a user entry for the service account
        service_account_user = models.User(id=service_account_user_id,
                                           name=token_name,
                                           user_type=models.UserType.SA.value)
        is_new_user = global_user_state.add_or_update_user(
            service_account_user, allow_duplicate_name=False)

        if not is_new_user:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f'Service account with name {token_name!r} '
                f'already exists ({service_account_user_id}). '
                'Please use a different name.')

        # Add service account to permission system with default role
        # Import here to avoid circular imports
        # pylint: disable=import-outside-toplevel
        from sky.users.permission import permission_service
        permission_service.add_user_if_not_exists(service_account_user_id)

        # Handle expiration: 0 means "never expire"
        expires_in_days = token_body.expires_in_days
        if expires_in_days == 0:
            expires_in_days = None

        # Create JWT-based token with service account user ID
        token_data = token_service.token_service.create_token(
            creator_user_id=auth_user.id,
            service_account_user_id=service_account_user_id,
            token_name=token_name,
            expires_in_days=expires_in_days)

        # Store token metadata in database
        global_user_state.add_service_account_token(
            token_id=token_data['token_id'],
            token_name=token_name,
            token_hash=token_data['token_hash'],
            creator_user_hash=auth_user.id,
            service_account_user_id=service_account_user_id,
            expires_at=token_data['expires_at'])

        # Return the JWT token only once (never stored in plain text)
        return {
            'token_id': token_data['token_id'],
            'token_name': token_name,
            'token': token_data['token'],  # Full JWT token with sky_ prefix
            'expires_at': token_data['expires_at'],
            'service_account_user_id': service_account_user_id,
            'creator_user_id': auth_user.id,
            'message': 'Please save this token - it will not be shown again!'
        }

    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to create service account token: {e}')
        raise fastapi.HTTPException(
            status_code=500,
            detail=f'Failed to create service account token: {e}')


def delete_service_account_token(
    request: fastapi.Request,
    token_body: payloads.ServiceAccountTokenDeleteBody,
    delete_user: Callable[[str], None],
) -> dict[str, str]:
    """Delete a service account token and its guarded user identity."""
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Authentication required')

    # Get token info first
    token_info = global_user_state.get_service_account_token(
        token_body.token_id)
    if token_info is None:
        raise fastapi.HTTPException(status_code=404, detail='Token not found')

    # Check permissions using Casbin policy system
    if not permission.permission_service.check_service_account_token_permission(
            auth_user.id, token_info['creator_user_hash'], 'delete'):
        raise fastapi.HTTPException(
            status_code=403,
            detail='You can only delete your own tokens. Only admins can '
            'delete tokens owned by other users.')

    # Delete the service account user first to prove there are no active
    # resources owned by the service account.
    delete_user(token_info['service_account_user_id'])

    deleted = global_user_state.delete_service_account_token(
        token_body.token_id)
    if not deleted:
        raise fastapi.HTTPException(status_code=404, detail='Token not found')

    return {'message': 'Token deleted successfully'}


@router.post('/service-account-tokens/get-role')
def get_service_account_role(
        request: fastapi.Request,
        role_body: payloads.ServiceAccountTokenRoleBody) -> dict[str, Any]:
    """Get the role of a service account."""
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Authentication required')

    token_info = global_user_state.get_service_account_token(role_body.token_id)
    if token_info is None:
        raise fastapi.HTTPException(status_code=404, detail='Token not found')

    if not permission.permission_service.check_service_account_token_permission(
            auth_user.id, token_info['creator_user_hash'], 'view'):
        raise fastapi.HTTPException(
            status_code=403,
            detail='You can only view roles for your own service accounts. '
            'Only admins can view roles for service accounts owned by other '
            'users.')

    service_account_user_id = token_info['service_account_user_id']
    roles = permission.permission_service.get_user_roles(
        service_account_user_id)

    return {
        'token_id': role_body.token_id,
        'service_account_user_id': service_account_user_id,
        'roles': roles
    }


@router.post('/service-account-tokens/update-role')
def update_service_account_role(
        request: fastapi.Request,
        role_body: payloads.ServiceAccountTokenUpdateRoleBody
) -> dict[str, str]:
    """Update the role of a service account."""
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Authentication required')

    token_info = global_user_state.get_service_account_token(role_body.token_id)
    if token_info is None:
        raise fastapi.HTTPException(status_code=404, detail='Token not found')

    if not permission.permission_service.check_service_account_token_permission(
            auth_user.id, token_info['creator_user_hash'], 'update'):
        raise fastapi.HTTPException(
            status_code=403,
            detail='You can only update roles for your own service accounts. '
            'Only admins can update roles for service accounts owned by other '
            'users.')

    try:
        service_account_user_id = token_info['service_account_user_id']
        permission.permission_service.update_role(service_account_user_id,
                                                  role_body.role)

        return {
            'message': f'Service account role updated to {role_body.role}',
            'token_id': role_body.token_id,
            'service_account_user_id': service_account_user_id,
            'new_role': role_body.role
        }
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to update service account role: {e}')
        raise fastapi.HTTPException(
            status_code=500, detail='Failed to update service account role')


@router.post('/service-account-tokens/rotate')
def rotate_service_account_token(
        request: fastapi.Request,
        token_body: payloads.ServiceAccountTokenRotateBody) -> dict[str, Any]:
    """Rotate a service account token.

    Generates a new token value for an existing service account while keeping
    the same service account identity and roles.
    """
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Authentication required')

    token_info = global_user_state.get_service_account_token(
        token_body.token_id)
    if token_info is None:
        raise fastapi.HTTPException(status_code=404, detail='Token not found')

    if not permission.permission_service.check_service_account_token_permission(
            auth_user.id, token_info['creator_user_hash'], 'delete'):
        raise fastapi.HTTPException(
            status_code=403,
            detail='You can only rotate your own tokens. Only admins can '
            'rotate tokens owned by other users.')

    if (token_body.expires_in_days is not None and
            token_body.expires_in_days < 0):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Expiration days must be positive or 0 for never expire')

    try:
        expires_in_days = token_body.expires_in_days
        if expires_in_days == 0:
            expires_in_days = None
        elif expires_in_days is None:
            if token_info['expires_at']:
                current_time = time.time()
                remaining_seconds = token_info['expires_at'] - current_time
                if remaining_seconds > 0:
                    expires_in_days = max(1,
                                          int(remaining_seconds / (24 * 3600)))
                else:
                    expires_in_days = 30

        token_data = token_service.token_service.create_token(
            creator_user_id=token_info['creator_user_hash'],
            service_account_user_id=token_info['service_account_user_id'],
            token_name=token_info['token_name'],
            expires_in_days=expires_in_days)

        global_user_state.rotate_service_account_token(
            token_id=token_body.token_id,
            new_token_hash=token_data['token_hash'],
            new_expires_at=token_data['expires_at'])

        return {
            'token_id': token_body.token_id,
            'token_name': token_info['token_name'],
            'token': token_data['token'],  # Full JWT token with sky_ prefix
            'expires_at': token_data['expires_at'],
            'service_account_user_id': token_info['service_account_user_id'],
            'message': ('Token rotated successfully! Please save this new '
                        'token - it will not be shown again!')
        }

    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to rotate service account token: {e}')
        raise fastapi.HTTPException(
            status_code=500, detail='Failed to rotate service account token')
