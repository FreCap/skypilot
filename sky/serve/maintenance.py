"""Explicit operational maintenance fences for SkyServe."""

import os

from sky.serve import constants


def is_controller_hold_active() -> bool:
    """Whether non-pool Serve control-plane mutations are held.

    The deployment must either omit the variable or provide an exact boolean
    spelling. Rejecting malformed values is intentional: a typo must stop
    controller recovery and provider-boundary launches instead of silently
    disabling the maintenance fence.
    """
    value = os.environ.get(constants.SERVE_CONTROLLER_HOLD_ENV_VAR)
    if value is None or value == 'false':
        return False
    if value == 'true':
        return True
    raise RuntimeError(
        f'{constants.SERVE_CONTROLLER_HOLD_ENV_VAR} must be exactly '
        "'true' or 'false'.")
