"""Cycle-free public enum contracts shared with SkyServe request payloads."""

import enum

EXTERNAL_LB_ENABLED_ENV_VAR = 'SKYPILOT_SERVE_EXTERNAL_LB_ENABLED'


class ServiceComponent(enum.Enum):
    """A user-addressable component of a SkyServe service."""

    CONTROLLER = 'controller'
    LOAD_BALANCER = 'load_balancer'
    REPLICA = 'replica'


class UpdateMode(enum.Enum):
    """Update mode for updating a service."""

    ROLLING = 'rolling'
    BLUE_GREEN = 'blue_green'
