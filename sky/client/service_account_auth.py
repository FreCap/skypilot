"""Service account token authentication for SkyPilot client."""

import os
import uuid

from sky import skypilot_config
from sky.server import constants as server_constants
from sky.skylet import constants


def _get_service_account_token() -> str | None:
    """Get service account token from environment variable or config file.

    Priority order:
    1. SKYPILOT_SERVICE_ACCOUNT_TOKEN environment variable
    2. ~/.sky/config.yaml service_account_token field

    Returns:
        The service account token if found, None otherwise.
    """
    # Check environment variable first
    token = os.environ.get(constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR)
    if token:
        if not token.startswith('sky_'):
            raise ValueError('Invalid service account token format. '
                             'Token must start with "sky_"')
        return token

    # Check config file
    token = skypilot_config.get_nested(('api_server', 'service_account_token'),
                                       default_value=None)
    if token and not token.startswith('sky_'):
        raise ValueError('Invalid service account token format in config. '
                         'Token must start with "sky_"')
    return token


def get_service_account_headers() -> dict:
    """Get authentication and server-owned controller-origin headers.

    Returns:
        Authentication headers plus controller fencing metadata when this
        client runs inside an elected controller process.
    """
    headers = {}
    token = _get_service_account_token()
    if token:
        headers['Authorization'] = f'Bearer {token}'

    instance_id = os.environ.get(
        server_constants.CONTROLLER_INSTANCE_ID_ENV_VAR)
    generation = os.environ.get(server_constants.CONTROLLER_GENERATION_ENV_VAR)
    if instance_id is None and generation is None:
        return headers
    if instance_id is None or generation is None:
        raise RuntimeError('Controller SDK requests require both instance and '
                           'generation identity.')
    try:
        uuid.UUID(instance_id)
        parsed_generation = int(generation)
    except (TypeError, ValueError) as e:
        raise RuntimeError('Controller SDK request identity is invalid.') from e
    if parsed_generation <= 0:
        raise RuntimeError(
            'Controller SDK request generation must be positive.')
    headers[server_constants.CONTROLLER_INSTANCE_ID_HEADER] = instance_id
    headers[server_constants.CONTROLLER_GENERATION_HEADER] = str(
        parsed_generation)
    return headers
