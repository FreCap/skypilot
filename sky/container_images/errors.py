"""Typed, value-free errors crossing the managed image runtime boundary."""

from __future__ import annotations

from collections.abc import Iterable

_SAFE_MESSAGES = {
    'ARTIFACT_NOT_READY': 'ARTIFACT_NOT_READY: publish and verify the image.',
    'CATALOG_AUTHORITY_UNAVAILABLE': (
        'CATALOG_AUTHORITY_UNAVAILABLE: retry after the image catalog recovers.'
    ),
    'IMAGE_DEMAND_TARGET_MISMATCH':
        ('IMAGE_DEMAND_TARGET_MISMATCH: retry the owning deployment generation.'
        ),
    'IMAGE_LIMIT_EXCEEDED': (
        'IMAGE_LIMIT_EXCEEDED: reduce retained images or raise the profile limit.'
    ),
    'IMAGE_LOCALITY_UNSUPPORTED':
        ('IMAGE_LOCALITY_UNSUPPORTED: select a qualified image target.'),
    'IMAGE_NOT_PUBLISHED':
        ('IMAGE_NOT_PUBLISHED: run sky image publish first.'),
    'IMAGE_PREPARATION_FAILED':
        ('IMAGE_PREPARATION_FAILED: inspect the image operation and retry it.'),
    'IMAGE_RESOLUTION_FAILED':
        ('IMAGE_RESOLUTION_FAILED: inspect image worker health and retry.'),
    'IMAGE_WARMING': ('IMAGE_WARMING: registry preparation is still running.'),
    'PROFILE_NOT_ACTIVE':
        ('PROFILE_NOT_ACTIVE: activate a qualified registry profile.'),
    'QUALIFICATION_FAILED':
        ('QUALIFICATION_FAILED: requalify the registry profile.'),
    'QUALIFICATION_STALE':
        ('QUALIFICATION_STALE: requalify the registry profile.'),
    'QUALIFIED_HOST_IMAGE_REQUIRED':
        ('QUALIFIED_HOST_IMAGE_REQUIRED: use the profile-qualified host image.'
        ),
    'QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED':
        ('QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED: use the qualified node pool.'
        ),
    'QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED':
        ('QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED: use the qualified runtime role.'
        ),
    'REGISTRY_CAPACITY_EXHAUSTED':
        ('REGISTRY_CAPACITY_EXHAUSTED: add a shard or raise verified capacity.'
        ),
}

_MAX_NESTED_ERRORS = 64


class ContainerImageError(ValueError):
    """A bounded code-valued error safe to persist and return to a client."""

    def __init__(self, code: str):
        if code not in _SAFE_MESSAGES:
            raise ValueError('Unsupported managed image error code.')
        self.code = code
        super().__init__(_SAFE_MESSAGES[code])


def from_exception(error: BaseException) -> ContainerImageError:
    """Converts an image-boundary failure without carrying source values."""
    if isinstance(error, ContainerImageError):
        return error
    code = str(error).partition(':')[0]
    if code not in _SAFE_MESSAGES:
        code = 'IMAGE_RESOLUTION_FAILED'
    return ContainerImageError(code)


def _nested_errors(error: BaseException) -> Iterable[BaseException]:
    for nested in (error.__cause__, error.__context__):
        if isinstance(nested, BaseException):
            yield nested
    for attribute in ('failover_history', 'reasons', 'errors'):
        values = getattr(error, attribute, None)
        if isinstance(values, (list, tuple)):
            for nested in values:
                if isinstance(nested, BaseException):
                    yield nested


def find_safe_error(error: BaseException) -> ContainerImageError | None:
    """Finds a typed image error through a bounded provisioning wrapper graph."""
    pending = [error]
    seen: set[int] = set()
    while pending and len(seen) < _MAX_NESTED_ERRORS:
        candidate = pending.pop()
        identity = id(candidate)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(candidate, ContainerImageError):
            return candidate
        pending.extend(_nested_errors(candidate))
    return None
