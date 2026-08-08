"""Stateful dark evaluator for the Serve039 authority-preflight trust fence.

This module deliberately owns no database transaction, request submission,
claimant, renderer, provider client, or Kubernetes client.  Its validator
callback is the sole bridge to the PostgreSQL trust transaction.  A trusted
request still receives only the typed unavailable result; complete capsules
remain unreachable until the later P2b evaluator lands.
"""

from __future__ import annotations

from collections.abc import Callable
import uuid

from sky.serve import resource_action_preflight_v2 as preflight_v2
from sky.server.requests import resource_actions as kernel_actions

ProviderAuthorityPreflightTrustValidatorV2 = Callable[
    [preflight_v2.ProviderAuthorityPreflightRequestV2, uuid.UUID], bool]


def _canonical_uuid(value: uuid.UUID | str, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be a UUID or canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as e:
        raise ValueError(f'{name} must be a UUID.') from e
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


class InitialProviderPreflightEvaluatorV2:
    """Return typed NR only after the complete stateful trust fence passes."""

    def __init__(
        self,
        validate_trust: ProviderAuthorityPreflightTrustValidatorV2,
        worker_instance_id: uuid.UUID | str,
    ) -> None:
        if not callable(validate_trust):
            raise TypeError('validate_trust must be callable.')
        self._validate_trust = validate_trust
        self._worker_instance_id = _canonical_uuid(worker_instance_id,
                                                   name='worker_instance_id')

    def __call__(
        self, request: preflight_v2.ProviderAuthorityPreflightRequestV2
    ) -> preflight_v2.ProviderAuthorityPreflightResponseV2 | None:
        if type(request) is not (
                preflight_v2.ProviderAuthorityPreflightRequestV2):
            raise TypeError('preflight request V2 has an invalid type.')
        trusted = self._validate_trust(request, self._worker_instance_id)
        if type(trusted) is not bool:
            raise TypeError('V2 preflight trust validator must return a '
                            'Boolean.')
        if not trusted:
            return None
        if request.action_kind is kernel_actions.ActionKind.LAUNCH:
            return (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2.
                    unavailable(request))
        return (preflight_v2.ProviderDownAuthorityPreflightResponseV2.
                unavailable(request))


__all__ = [
    'InitialProviderPreflightEvaluatorV2',
    'ProviderAuthorityPreflightTrustValidatorV2',
]
