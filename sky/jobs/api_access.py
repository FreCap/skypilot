"""Service-account token lifecycle helpers for managed jobs."""

from sky import global_user_state
from sky.adaptors.common import LazyImport
from sky.jobs import constants as managed_job_constants

token_service_lib = LazyImport('sky.users.token_service')


def create_job_api_token(creator_user_id: str,
                         token_name_suffix: str) -> tuple[str, str]:
    """Create a short-lived service-account token as the job's user."""
    token_name = (f'{managed_job_constants.MANAGED_JOB_TOKEN_NAME_PREFIX}'
                  f'{token_name_suffix}')
    token_data = token_service_lib.token_service.create_token(
        creator_user_id=creator_user_id,
        service_account_user_id=creator_user_id,
        token_name=token_name,
        expires_in_days=managed_job_constants.MANAGED_JOB_TOKEN_TTL_DAYS)

    global_user_state.add_service_account_token(
        token_id=token_data['token_id'],
        token_name=token_name,
        token_hash=token_data['token_hash'],
        creator_user_hash=creator_user_id,
        service_account_user_id=creator_user_id,
        expires_at=token_data['expires_at'])

    return token_data['token'], token_data['token_id']
