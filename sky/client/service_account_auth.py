"""Service account token authentication for SkyPilot client."""

import os
import uuid

from sky import skypilot_config
from sky.server import constants as server_constants
from sky.server import versions
from sky.skylet import constants
from sky.utils import controller_capability
from sky.utils import controller_constants


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

    # Managed-job ControllerManager processes inherit the canonical runtime
    # owner, while other controller work uses the generic outer-generation
    # environment.  They must never disagree: the nested request's outer pair
    # is one immutable fencing identity.
    generic_instance_id = os.environ.get(
        server_constants.CONTROLLER_INSTANCE_ID_ENV_VAR)
    generic_generation = os.environ.get(
        server_constants.CONTROLLER_GENERATION_ENV_VAR)
    managed_instance_id = os.environ.get(
        server_constants.MANAGED_JOB_CONTROLLER_INSTANCE_ID_ENV_VAR)
    managed_generation = os.environ.get(
        server_constants.MANAGED_JOB_CONTROLLER_GENERATION_ENV_VAR)
    context_origin = versions.get_managed_job_origin()
    if (generic_instance_id is not None and managed_instance_id is not None and
            generic_instance_id != managed_instance_id):
        raise RuntimeError('Controller SDK request owner identities disagree.')
    if (generic_generation is not None and managed_generation is not None and
            generic_generation != managed_generation):
        raise RuntimeError('Controller SDK request owner generations disagree.')
    context_instance_id: str | None = None
    context_generation: str | None = None
    if context_origin is not None:
        try:
            context_instance_id = context_origin[1]
            context_generation = str(context_origin[2])
        except (IndexError, TypeError) as e:
            raise RuntimeError(
                'Managed-job SDK request context is invalid.') from e
        for label, environment_value, context_value in (
            ('identity', generic_instance_id, context_instance_id),
            ('generation', generic_generation, context_generation),
            ('identity', managed_instance_id, context_instance_id),
            ('generation', managed_generation, context_generation),
        ):
            if (environment_value is not None and
                    environment_value != context_value):
                raise RuntimeError('Controller SDK request context and '
                                   f'environment {label} disagree.')
    instance_id = (context_instance_id or generic_instance_id or
                   managed_instance_id)
    generation = context_generation or generic_generation or managed_generation
    environment_capability = os.environ.get(
        server_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR)
    if environment_capability is not None:
        raise RuntimeError(
            'Controller SDK request capability authority must not be inherited '
            'in the process environment.')
    capability = controller_capability.get_process_local()
    managed_job_id_environment = os.environ.get(
        server_constants.MANAGED_JOB_ID_ENV_VAR)
    managed_slot_id_environment = os.environ.get(
        server_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR)
    managed_slot_attempt_environment = os.environ.get(
        server_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR)
    managed_owner_mode_environment = os.environ.get(
        controller_constants.MANAGED_JOB_CONTROLLER_OWNER_MODE_ENV_VAR)
    managed_owner_pid_environment = os.environ.get(
        controller_constants.MANAGED_JOB_CONTROLLER_OWNER_PID_ENV_VAR)
    managed_owner_start_environment = os.environ.get(
        controller_constants.MANAGED_JOB_CONTROLLER_OWNER_START_TICKS_ENV_VAR)
    managed_ready_fd_environment = os.environ.get(
        controller_constants.MANAGED_JOB_CONTROLLER_READY_FD_ENV_VAR)
    managed_capability_fd_environment = os.environ.get(
        controller_constants.MANAGED_JOB_CONTROLLER_CAPABILITY_FD_ENV_VAR)
    managed_attempt_present = (context_origin is not None or any(
        value is not None
        for value in (managed_job_id_environment, managed_slot_id_environment,
                      managed_slot_attempt_environment)))
    managed_authority_present = (context_origin is not None or any(
        value is not None for value in (
            managed_instance_id, managed_generation, managed_job_id_environment,
            managed_slot_id_environment, managed_slot_attempt_environment,
            managed_owner_mode_environment, managed_owner_pid_environment,
            managed_owner_start_environment, managed_ready_fd_environment,
            managed_capability_fd_environment)))
    if capability is None:
        if managed_authority_present:
            raise RuntimeError(
                'Managed-job SDK requests require controller capability '
                'authority.')
        # A controller-class request handler needs the generic pair for local
        # PostgreSQL write fencing, but that pair alone does not authorize it
        # to originate controller SDK work. Treat the complete pair as neutral
        # authentication context and emit no internal origin headers.
        if instance_id is None and generation is None:
            return headers
        if generic_instance_id is not None and generic_generation is not None:
            return headers
    # PostgreSQL all-mode publishes its managed-runtime owner for refresh and
    # slot fencing, but normal work in that combined process must not inherit
    # controller-origin SDK authority.  Only an explicit managed attempt (or
    # the generic pair installed at a trusted daemon/controller boundary) may
    # turn the process-local capability into origin headers.
    if (capability is not None and not managed_attempt_present and
            generic_instance_id is None and generic_generation is None and
            managed_instance_id is not None and managed_generation is not None):
        return headers
    if instance_id is None or generation is None or capability is None:
        raise RuntimeError(
            'Controller SDK requests require instance, generation, and '
            'capability authority.')
    try:
        canonical_instance_id = str(uuid.UUID(instance_id))
        parsed_generation = int(generation)
        controller_capability.digest(capability)
    except (AttributeError, TypeError, ValueError) as e:
        raise RuntimeError('Controller SDK request identity is invalid.') from e
    if (canonical_instance_id != instance_id or parsed_generation <= 0 or
            str(parsed_generation) != generation):
        raise RuntimeError('Controller SDK request identity is not canonical.')
    headers[server_constants.CONTROLLER_INSTANCE_ID_HEADER] = instance_id
    headers[server_constants.CONTROLLER_GENERATION_HEADER] = str(
        parsed_generation)
    headers[server_constants.CONTROLLER_ORIGIN_CAPABILITY_HEADER] = capability

    managed_job_id: str | None
    slot_id: str | None
    slot_attempt: str | None
    if context_origin is not None:
        try:
            managed_job_id = str(context_origin[0])
            slot_id = str(context_origin[3])
            slot_attempt = context_origin[4]
        except (IndexError, TypeError) as e:
            raise RuntimeError(
                'Managed-job SDK request context is invalid.') from e
    else:
        managed_job_id = managed_job_id_environment
        slot_id = managed_slot_id_environment
        slot_attempt = managed_slot_attempt_environment
    managed_fields = (managed_job_id, slot_id, slot_attempt)
    if all(value is None for value in managed_fields):
        return headers
    if any(value is None for value in managed_fields):
        raise RuntimeError(
            'Managed-job SDK request identity requires job, slot, and attempt.')
    assert managed_job_id is not None
    assert slot_id is not None
    assert slot_attempt is not None
    try:
        parsed_job_id = int(managed_job_id)
        parsed_slot_id = int(slot_id)
        canonical_attempt = str(uuid.UUID(slot_attempt))
    except (AttributeError, TypeError, ValueError) as e:
        raise RuntimeError(
            'Managed-job SDK request identity is invalid.') from e
    if (parsed_job_id <= 0 or parsed_slot_id < 0 or
            str(parsed_job_id) != managed_job_id or
            str(parsed_slot_id) != slot_id or
            canonical_attempt != slot_attempt):
        raise RuntimeError('Managed-job SDK request identity is not canonical.')
    headers[server_constants.MANAGED_JOB_ID_HEADER] = str(parsed_job_id)
    headers[server_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_HEADER] = str(
        parsed_slot_id)
    headers[server_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_HEADER] = (
        canonical_attempt)
    return headers
