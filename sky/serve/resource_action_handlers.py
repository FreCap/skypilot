"""Quarantined handlers retained for persisted resource-action requests.

The dedicated authority-worker activation path has been retired.  These four
private names remain registered so persisted requests can still be decoded,
but every ordinary queue excludes them and the functions fail before reading
provider credentials or crossing the generic resource-action intent
watermark.
"""

from typing import Any, NoReturn


class ResourceActionProviderProfileDisabledError(RuntimeError):
    """A retired private resource action reached execution."""


def _provider_profile_disabled(**unused_payload: Any) -> NoReturn:
    del unused_payload
    raise ResourceActionProviderProfileDisabledError(
        'SkyServe resource-action authority execution has been retired.')


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
