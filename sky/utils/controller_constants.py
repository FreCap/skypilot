"""Cycle-free default resource contracts for SkyPilot controllers.

These values are consumed while the public ``sky.jobs`` and ``sky.serve``
packages are still initializing.  Keeping their shared metadata below either
package prevents controller type construction from forcing the other package's
public initializer through a circular import.
"""

from typing import Any

JOBS_CONTROLLER_RESOURCES: dict[str, str | int] = {
    'cpus': '4+',
    'memory': '4x',
    'disk_size': 50,
}
JOBS_CONTROLLER_AUTOSTOP: dict[str, Any] = {
    'idle_minutes': 10,
    'down': False,
}

SERVE_CONTROLLER_RESOURCES: dict[str, str | int] = {
    'cpus': '4+',
    'memory': '8+',
    'disk_size': 200,
}
SERVE_CONTROLLER_AUTOSTOP: dict[str, Any] = {
    'idle_minutes': 10,
    'down': False,
}

# Server-owned managed-job controller identity.  Keep these strings in this
# cycle-free module: both the controller runtime and the API client header
# builder need them while the public ``sky.jobs`` package may still be
# initializing.
MANAGED_JOB_CONTROLLER_OWNER_MODE_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_MODE')
MANAGED_JOB_CONTROLLER_INSTANCE_ID_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_INSTANCE_ID')
MANAGED_JOB_CONTROLLER_GENERATION_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_GENERATION')
MANAGED_JOB_CONTROLLER_OWNER_PID_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_PID')
MANAGED_JOB_CONTROLLER_OWNER_START_TICKS_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_START_TICKS')
MANAGED_JOB_ID_ENV_VAR = 'SKYPILOT_SERVER_MANAGED_JOB_ID'
MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ID')
MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT')
MANAGED_JOB_CONTROLLER_READY_FD_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_READY_FD')
MANAGED_JOB_CONTROLLER_CAPABILITY_FD_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD')

# An unguessable controller-origin bearer capability accompanies the durable
# owner/slot identity. PostgreSQL runtimes validate its hash in the leadership
# row; the single-host remote jobs controller validates the same hash through
# a private, process-birth-bound authority file.
CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR = (
    'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY')
CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR = (
    'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH')
