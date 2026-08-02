"""Private fail-closed handlers for durable SkyServe resource actions.

The dedicated authority-worker cohort must have a stable, closed handler
inventory before it may claim any request.  Provider mutation remains disabled
until the checked-in provider profile and live cohort preflight clear the
promotion gates in ``docs/designs/durable-serve-replica-actions.md``.  These
handlers therefore deliberately fail before reading provider credentials or
crossing the generic resource-action intent watermark.

They are registered explicitly by :mod:`sky.server.requests.registry`; the
ordinary built-in module scanner never grants their private names to a general
executor.
"""

from typing import Any, NoReturn


class ResourceActionProviderProfileDisabledError(RuntimeError):
    """A private action reached execution before its provider profile opened."""


def _provider_profile_disabled(**unused_payload: Any) -> NoReturn:
    del unused_payload
    raise ResourceActionProviderProfileDisabledError(
        'SkyServe resource-action provider execution is disabled until the '
        'canonical promotion gates and live cohort preflight pass.')


def serve_shadow_candidate_launch(**payload: Any) -> NoReturn:
    """Reject a launch shadow request before provider I/O."""
    _provider_profile_disabled(**payload)


def serve_shadow_candidate_down(**payload: Any) -> NoReturn:
    """Reject a down shadow request before provider I/O."""
    _provider_profile_disabled(**payload)


def serve_resource_action_launch(**payload: Any) -> NoReturn:
    """Reject an authoritative launch request before provider I/O."""
    _provider_profile_disabled(**payload)


def serve_resource_action_down(**payload: Any) -> NoReturn:
    """Reject an authoritative down request before provider I/O."""
    _provider_profile_disabled(**payload)
